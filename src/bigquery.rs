use gcp_bigquery_client::error::BQError;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::query_response::ResultSet;
use gcp_bigquery_client::model::table_data_insert_all_request::TableDataInsertAllRequest;
use std::{
    env,
    io::{
        Error,
        ErrorKind
    },
    thread,
    time::Duration,
};
use tokio::runtime::Runtime;
use tokio::time::timeout;

use crate::transaction::Transaction;
use crate::block::Block;

const TRANSACTIONS_TABLE_ID: &str = "transactions";
const BLOCKS_TABLE_ID: &str = "blocks";

pub struct BigQuery {
    client: gcp_bigquery_client::Client,
    runtime: Runtime,
    project_id: String,
    dataset_id: String,
    block_pending: Option<Block>,
    transactions_pending: Vec<Transaction>,
}

impl BigQuery {
    async fn get_client() -> gcp_bigquery_client::Client {
        let gcp_key = env::var("GOOGLE_APPLICATION_CREDENTIALS")
            .expect("Environment variable GOOGLE_APPLICATION_CREDENTIALS is required");
        loop {
            let client_res = timeout(
                    Duration::from_secs(60),
                    gcp_bigquery_client::Client::from_service_account_key_file(&gcp_key)
                )
                .await;
            match client_res {
                Ok(client) => {
                    return client;
                }
                Err(_) => {
                    println!("Timed out waiting for the BigQuery client. Retry.");
                }
            }
        }
    }

    pub fn new(project_id: &str, dataset_id: &str) -> BigQuery {
        let runtime = Runtime::new().unwrap();
        let client = runtime.block_on(Self::get_client());

        BigQuery {
            client: client,
            runtime: runtime,
            project_id: project_id.to_string(),
            dataset_id: dataset_id.to_string(),
            block_pending: None,
            transactions_pending: Vec::new(),
        }
    }

    async fn query_latest_slot(&self) -> ResultSet {
        loop {
            let query = QueryRequest::new(
                format!("SELECT MAX(slot) AS slot FROM `{}.{}.{}`",
                    self.project_id, self.dataset_id, BLOCKS_TABLE_ID
                )
            );
            let res = timeout(
                Duration::from_secs(60),
                self.client
                    .job()
                    .query(&self.project_id, query)
            )
            .await;
            match res {
                Ok(set) => {
                    return set.expect("Query failed");
                }
                Err(_) => {
                    println!("Timed out waiting for the latest slot. Retry.");
                }
            }
        }
    }

    pub fn get_latest_slot(&self) -> Result<u64, BQError> {
        let mut rows = self.runtime.block_on(self.query_latest_slot());
        if rows.next_row() {
            if let Some(slot) = rows.get_i64_by_name("slot")? {
                if slot >= 0 {
                    return Ok(slot as u64);
                }
            }
        }
        return Err(
            BQError::from(
                Error::new(
                    ErrorKind::InvalidData,
                    "Could not find latest slot",
                )
            )
        );
    }

    pub fn add_block(&mut self, block: Block) {
        self.block_pending = Some(block);
    }

    pub fn add_transaction(&mut self, transaction: Transaction) {
        self.transactions_pending.push(transaction);
    }

    fn create_transaction_request(&self)
        -> Result<TableDataInsertAllRequest, BQError> {
        let mut transactions = TableDataInsertAllRequest::new();
        for transaction in &self.transactions_pending {
            transactions.add_row(None, transaction)?;
        }
        Ok(transactions)
    }

    async fn insert_transactions(&self) {
        let retry_period = Duration::from_secs(1);
        const MAX_ATTEMPTS: u32 = 10;
        for attempt in 0..MAX_ATTEMPTS {
            if attempt > 0 {
                println!("Retry {}/{} after {} second(s).",
                    attempt,
                    MAX_ATTEMPTS,
                    retry_period.as_secs());
                thread::sleep(retry_period);
            }
            let transactions: TableDataInsertAllRequest;
            match self.create_transaction_request() {
                Ok(t) => {
                    transactions = t;
                }
                Err(err) => {
                    eprintln!("{:?}", err);
                    eprintln!("Failed to add transaction row.");
                    continue;
                }
            }
            let res = timeout(
                Duration::from_secs(60),
                self.client
                    .tabledata()
                    .insert_all(
                        &self.project_id,
                        &self.dataset_id,
                        TRANSACTIONS_TABLE_ID,
                        transactions
                    )
            )
            .await;
            match res {
                Err(_) => {
                    eprintln!("Timed out waiting to insert transactions.");
                    continue;
                }
                Ok(r) => {
                    match r {
                        Err(err) => {
                            eprintln!("{:?}", err);
                            eprintln!("Failed to insert transactions.");
                            continue;
                        }
                        Ok(res) => {
                            if let Some(errors) = res.insert_errors {
                                eprintln!("{:?}", errors);
                                eprintln!("One or more transactions failed to insert.");
                                continue;
                            }
                            return;
                        }
                    }
                }
            }
        }
    }

    async fn insert_block(&self) {
        let retry_period = Duration::from_secs(1);
        const MAX_ATTEMPTS: u32 = 10;
        for attempt in 0..MAX_ATTEMPTS {
            if attempt > 0 {
                println!("Retry {}/{} after {} second(s).",
                    attempt,
                    MAX_ATTEMPTS,
                    retry_period.as_secs());
                thread::sleep(retry_period);
            }
            let mut block_request = TableDataInsertAllRequest::new();
            let block_pending = self.block_pending
                .as_ref()
                .expect("Failed to find block to insert");
            if let Err(err) = block_request.add_row(None, block_pending) {
                eprintln!("{:?}", err);
                eprintln!("Failed to add block row.");
                continue;
            }

            let res = timeout(
                Duration::from_secs(60),
                self.client
                    .tabledata()
                    .insert_all(
                        &self.project_id,
                        &self.dataset_id,
                        BLOCKS_TABLE_ID,
                        block_request
                    )
            )
            .await;

            match res {
                Err(_) => {
                    eprintln!("Timed out waiting to insert block");
                    continue
                }
                Ok(r) => {
                    match r {
                        Err(err) => {
                            eprintln!("{:?}", err);
                            eprintln!("Failed to insert block.");
                            continue;
                        }
                        Ok(res) => {
                            if let Some(errors) = res.insert_errors {
                                eprintln!("{:?}", errors);
                                eprintln!("Block failed to insert.");
                                continue;
                            }
                            return;
                        }
                    }
                }
            }
        }
    }

    pub fn commit(mut self) {
        if self.transactions_pending.len() == 0 {
            return;
        }

        self.runtime.block_on(self.insert_transactions());

        println!("Transactions recorded: {}", self.transactions_pending.len());

        self.transactions_pending = Vec::new();

        self.runtime.block_on(self.insert_block());

        self.block_pending = None;
    }
}
