use chrono::{
    DateTime,
    NaiveDateTime,
    Utc,
};
use serde::Serialize;
use solana_sdk::clock::Slot;
use solana_transaction_status::EncodedConfirmedBlock;

#[derive(Serialize)]
pub struct Block {
    block_timestamp: Option<DateTime<Utc>>,
    slot: u64,
    parent_slot: u64,
    blockhash: String,
    previous_blockhash: String,
    rewards: Vec<Reward>,
}

#[derive(Serialize)]
struct Reward {
    pubkey: String,
    lamports: i64,
    post_balance: u64,
    reward_type: Option<String>,
}

impl Block {
    pub fn new(
        slot: Slot,
        encoded_block: &EncodedConfirmedBlock,
    ) -> Block {
        let block_timestamp: Option<DateTime<Utc>>;
        match encoded_block.block_time {
            None => {
                block_timestamp = None;
            }
            Some(bt) => {
                let naive_datetime = NaiveDateTime::from_timestamp(bt, 0);
                block_timestamp = Some(DateTime::from_utc(naive_datetime, Utc));
            }
        }

        let mut block = Block {
            block_timestamp: block_timestamp,
            slot: slot,
            parent_slot: encoded_block.parent_slot,
            blockhash: encoded_block.blockhash.clone(),
            previous_blockhash: encoded_block.previous_blockhash.clone(),
            rewards: Vec::new(),
        };

        for reward in &encoded_block.rewards {
            let reward_type: Option<String>;
            match reward.reward_type {
                None => {
                    reward_type = None;
                }
                Some(rt) => {
                    reward_type = Some(rt.to_string());
                }
            }
            block.rewards.push(Reward {
                pubkey: reward.pubkey.clone(),
                lamports: reward.lamports,
                post_balance: reward.post_balance,
                reward_type: reward_type,
            });
        }
        return block;
    }

    pub fn get_timestamp(&self) -> Option<DateTime<Utc>> {
        self.block_timestamp
    }
}
