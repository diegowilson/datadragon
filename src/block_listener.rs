use std::{
    thread,
    time,
};

use chrono::{
    DateTime,
    Utc,
};

use num_cpus;

use solana_rpc::SolanaRpc;

use solana_client::{
    client_error::Result as ClientResult,
};

use solana_sdk::{
    clock::Slot,
    transaction::Transaction as SolanaTransaction,
};

use solana_transaction_status::{
    EncodedConfirmedBlock,
    UiTransactionEncoding,
    UiTransactionStatusMeta,
};

use crate::{
    bigquery::BigQuery,
    block::Block,
    counter::Counter,
    solana_rpc,
    transaction::Transaction,
};

const SLOTS_BEHIND_LATEST: u64 = 200;
const MAX_SLOT_RANGE: u64 = 100;

pub struct Listener {
    solana_client: SolanaRpc,
    project_id: String,
    dataset_id: String,
    processed_slot: Slot,
    end_slot: Option<Slot>,
    max_processor_count: usize,
    processor_counter: Counter,
}

impl Listener {
    fn get_block(&self, slot: Slot) -> EncodedConfirmedBlock {
        let mut period = time::Duration::from_millis(100);
        loop {
            let block_result = self.solana_client.get_block_with_encoding(
                slot, UiTransactionEncoding::Base64);
            if let Ok(block) = block_result {
                return block;
            }
            println!("Attempt to get block failed. Retry after {} ms.",
                period.as_millis());
            thread::sleep(period);
            //Use exponential backoff
            period *= 2;
        }
    }

    fn get_unprocessed_slots(&mut self) -> Vec<Slot> {
        let latest_slot = self.solana_client.get_latest_slot();


        if self.processed_slot + SLOTS_BEHIND_LATEST >= latest_slot {
            let empty_slots: Vec<Slot> = vec![];
            return empty_slots;
        }

        let mut target_slot = latest_slot - SLOTS_BEHIND_LATEST;
        if target_slot - self.processed_slot > MAX_SLOT_RANGE {
            target_slot = self.processed_slot + MAX_SLOT_RANGE;
        }
        if let Some(end_slot) = self.end_slot {
            if target_slot > end_slot {
                target_slot = end_slot;
            }
        }

        println!("Latest slot: {}. Target slot: {}. Processed slot: {}. Trailing latest {}. Trailing target: {}.",
            latest_slot,
            target_slot,
            self.processed_slot,
            latest_slot - self.processed_slot,
            target_slot - self.processed_slot,
            );

        let mut period = time::Duration::from_millis(100);
        loop {
            let slots_result = self.solana_client.get_blocks(
                self.processed_slot + 1, Some(target_slot));
            match slots_result {
                Ok(slots) => {
                    return slots;
                }
                Err(error) => {
                    println!("Attempt to fetch list of pending slots failed with error: {:?}", error);
                    println!("Retry after {} ms.", period.as_millis());
                }
            }
            thread::sleep(period);
            //Use exponential backoff
            period *= 2;
        }
    }


    fn process_slots(&mut self) -> bool {
        if let Some(end_slot) = self.end_slot {
            if self.processed_slot >= end_slot {
                println!("Stop after processing the selected end slot {}", end_slot);
                return false;
            }
        }

        let all_unprocessed_slots = self.get_unprocessed_slots();

        const NO_UNPROCESSED_SLOTS_WAIT: std::time::Duration = time::Duration::from_millis(1000);
        if all_unprocessed_slots.is_empty() {
            thread::sleep(NO_UNPROCESSED_SLOTS_WAIT);
            return true;
        }

        for slot in all_unprocessed_slots.into_iter() {
            let block = self.get_block(slot);
            self.processor_counter.wait_if_above(self.max_processor_count - 1);
            self.processor_counter.increase();
            let project_id = self.project_id.clone();
            let dataset_id = self.dataset_id.clone();
            let processor_counter = self.processor_counter.clone();
            thread::spawn(move || {
                let processor = Processor::new(&project_id, &dataset_id);
                processor.process_block(slot, block)
                    .expect("Failed to process block");
                processor_counter.decrease();
            });
            self.processed_slot = slot;
        }

        return true;
    }

    pub fn listen(&mut self) {
        while self.process_slots() {}
        self.processor_counter.wait_if_above(0);
    }

    pub fn new(
        project_id: &str,
        dataset_id: &str,
        start_slot: Option<Slot>,
        end_slot: Option<Slot>,
    ) -> Listener {
        let mut solana_client = SolanaRpc::new();
        let processed_slot: Slot;
        if let Some(start) = start_slot {
            processed_slot = start - 1;
            println!("Start from selected slot {}", start);
        }
        else {
            let bq_client = BigQuery::new(project_id, dataset_id);
            if let Ok(slot) = bq_client.get_latest_slot() {
                processed_slot = slot;
                println!("Resume from latest processed slot {}", processed_slot);
            } else {
                processed_slot = solana_client.get_latest_slot()
                    - SLOTS_BEHIND_LATEST;
                println!("Could not find any previously processed slots.");
                println!("Start at the latest live slot {}", processed_slot);
            }
        }
        let max_processor_count = num_cpus::get() * 2;
        Listener {
            solana_client: solana_client,
            project_id: project_id.to_string(),
            dataset_id: dataset_id.to_string(),
            processed_slot: processed_slot,
            end_slot: end_slot,
            max_processor_count: max_processor_count,
            processor_counter: Counter::new(),
        }
    }
}

struct Processor {
    bq_client: BigQuery,
}

impl Processor {
    pub fn new(project_id: &str, dataset_id: &str) -> Processor {
        Processor {
            bq_client: BigQuery::new(project_id, dataset_id),
        }
    }

    fn process_transaction(
        &mut self,
        block_timestamp: &Option<DateTime<Utc>>,
        slot: Slot,
        meta: &UiTransactionStatusMeta,
        solana_transaction: &SolanaTransaction) {

        let transaction = Transaction::new(
            block_timestamp,
            slot,
            meta,
            solana_transaction,
        );

        self.bq_client.add_transaction(transaction);
    }

    fn process_block(mut self, slot: Slot, encoded_block: EncodedConfirmedBlock) -> ClientResult<String> {
        let block = Block::new(slot, &encoded_block);
        let timestamp = block.get_timestamp();
        self.bq_client.add_block(block);

        for rpc_transaction in encoded_block.transactions {
            match rpc_transaction.meta {
                None => {
                    panic!("Transaction has no meta");
                }
                Some(meta) => {
                    if let Some(transaction) = rpc_transaction.transaction.decode() {
                        if transaction.verify().is_ok() {
                            self.process_transaction(&timestamp, slot, &meta, &transaction);
                        } else {
                            panic!("Transaction signature verification failed");
                        }
                    } else {
                        panic!("Transaction decode failed");
                    }
                }
            }
        }
        self.bq_client.commit();
        Ok("".to_string())
    }
}
