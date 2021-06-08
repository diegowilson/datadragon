use gcp_bigquery_client::error::BQError;
use gcp_bigquery_client::model::table::Table;
use gcp_bigquery_client::model::table_field_schema::TableFieldSchema;
use gcp_bigquery_client::model::table_schema::TableSchema;
use gcp_bigquery_client::model::time_partitioning::TimePartitioning;
use std::env;

const PROJECT_ID: &str = "datadragonio-stage";
const DATASET_ID: &str = "solana_test";
const BLOCK_TABLE_ID: &str = "blocks";
const TRANSACTION_TABLE_ID: &str = "transactions";

#[tokio::main]
async fn main() -> Result<(), BQError> {
    let gcp_key = env::var("GOOGLE_APPLICATION_CREDENTIALS")
            .expect("Environment variable GOOGLE_APPLICATION_CREDENTIALS is required");
    // Init BigQuery client
    let client = gcp_bigquery_client::Client::from_service_account_key_file(&gcp_key).await;

    let dataset = client.dataset().get(PROJECT_ID, DATASET_ID).await?;

    let mut pre_balance_schema = TableFieldSchema::record(
        "pre_token_balances",
        vec![
            TableFieldSchema::string("mint"),
            TableFieldSchema::numeric("amount"),
        ]
    );
    pre_balance_schema.mode = Some("REPEATED".to_string());

    let mut post_balance_schema = TableFieldSchema::record(
        "post_token_balances",
        vec![
            TableFieldSchema::string("mint"),
            TableFieldSchema::numeric("amount"),
        ]
    );
    post_balance_schema.mode = Some("REPEATED".to_string());

    let mut account_schema = TableFieldSchema::record(
        "accounts",
        vec![
            TableFieldSchema::string("address"),
            TableFieldSchema::integer("pre_sol_balance"),
            TableFieldSchema::integer("post_sol_balance"),
            pre_balance_schema,
            post_balance_schema,
        ]
    );
    account_schema.mode = Some("REPEATED".to_string());

    let mut instruction_accounts_schema = TableFieldSchema::string("accounts");
    instruction_accounts_schema.mode = Some("REPEATED".to_string());

    let mut instruction_schema = TableFieldSchema::record(
        "instructions",
        vec![
            TableFieldSchema::string("program_id"),
            instruction_accounts_schema,
            TableFieldSchema::bytes("data"),
        ]
    );
    instruction_schema.mode = Some("REPEATED".to_string());

    let mut log_schema = TableFieldSchema::string("log_messages");
    log_schema.mode = Some("REPEATED".to_string());

    // Create a new table
    let transaction_table = dataset
        .create_table(
            &client,
            Table::from_dataset(
                &dataset,
                TRANSACTION_TABLE_ID,
                TableSchema::new(vec![
                    TableFieldSchema::timestamp("block_timestamp"),
                    TableFieldSchema::integer("slot"),
                    TableFieldSchema::string("transaction_id"),
                    TableFieldSchema::bool("is_successful"),
                    TableFieldSchema::string("error"),
                    TableFieldSchema::integer("fee"),
                    account_schema,
                    instruction_schema,
                    log_schema,
                ]),
            )
            .friendly_name("Transactions")
            .description("Solana ledger transactions")
            .label("owner", "me")
            .label("env", "prod")
            .time_partitioning(
                TimePartitioning::per_day()
                    .field("block_timestamp"),
            ),
        )
        .await?;
    println!("Table created -> {:?}", transaction_table);

    let mut reward_schema = TableFieldSchema::record(
        "rewards",
        vec![
            TableFieldSchema::string("pubkey"),
            TableFieldSchema::integer("lamports"),
            TableFieldSchema::integer("post_balance"),
            TableFieldSchema::string("reward_type"),
        ]
    );
    reward_schema.mode = Some("REPEATED".to_string());

    // Create a new table
    let block_table = dataset
        .create_table(
            &client,
            Table::from_dataset(
                &dataset,
                BLOCK_TABLE_ID,
                TableSchema::new(vec![
                    TableFieldSchema::timestamp("block_timestamp"),
                    TableFieldSchema::integer("slot"),
                    TableFieldSchema::integer("parent_slot"),
                    TableFieldSchema::string("blockhash"),
                    TableFieldSchema::string("previous_blockhash"),
                    reward_schema,
                ]),
            )
            .friendly_name("Blocks")
            .description("Solana ledger blocks")
            .label("owner", "me")
            .label("env", "prod")
            .time_partitioning(
                TimePartitioning::per_day()
                    .field("block_timestamp"),
            ),
        )
        .await?;

    println!("Table created -> {:?}", block_table);

    Ok(())
}
