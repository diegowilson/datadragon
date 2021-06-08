use std::{thread, time};

use solana_client::rpc_client::RpcClient;
use solana_client::client_error::Result as ClientResult;
use solana_sdk::{
    clock::Slot,
    commitment_config::CommitmentConfig,
};
use solana_transaction_status::{
    EncodedConfirmedBlock,
    UiTransactionEncoding,
};

pub struct SolanaRpc {
    rpc_client: RpcClient,
    node_url: String,
}

const SOLANA_NODE_URL: &str = "https://api.mainnet-beta.solana.com";
const SERUM_NODE_URL: &str = "https://solana-api.projectserum.com";
//Time to wait before we try another RPC node
const NODE_TIMEOUT: time::Duration = time::Duration::from_millis(10000);

impl SolanaRpc {
    pub fn new() -> SolanaRpc {
        SolanaRpc {
            rpc_client: RpcClient::new(SOLANA_NODE_URL.to_string()),
            node_url: SOLANA_NODE_URL.to_string(),
        }
    }

    pub fn get_block_with_encoding(&self, slot: Slot, encoding: UiTransactionEncoding)
        -> ClientResult<EncodedConfirmedBlock> {
        return self.rpc_client.get_block_with_encoding(slot, encoding);
    }

    pub fn get_blocks(&self, start_slot: Slot, end_slot: Option<Slot>)
        -> ClientResult<Vec<Slot>> {
        return self.rpc_client.get_blocks(start_slot, end_slot);
    }

    pub fn get_latest_slot(&mut self) -> Slot {
        let mut period = time::Duration::from_millis(100);
        loop {
            let slot_result = self.rpc_client.get_slot_with_commitment(CommitmentConfig::finalized());
            match slot_result {
                Ok(slot) => {
                    return slot;
                }
                Err(error) => {
                    println!("Attempt to find the latest finalized slot failed with error: {:?}",
                        error);
                    println!("Retrying after {} ms.", period.as_millis());
                }
            }
            if period > NODE_TIMEOUT {
                if self.node_url == SOLANA_NODE_URL.to_string() {
                    self.rpc_client = RpcClient::new(SERUM_NODE_URL.to_string());
                    self.node_url = SERUM_NODE_URL.to_string();
                } else {
                    self.rpc_client = RpcClient::new(SOLANA_NODE_URL.to_string());
                    self.node_url = SOLANA_NODE_URL.to_string();
                }
                println!("Try out RPC node {}.", self.node_url);
                period = time::Duration::from_millis(100);
                continue;
            }
            thread::sleep(period);
            //Use exponential backoff
            period *= 2;
        }
    }
}
