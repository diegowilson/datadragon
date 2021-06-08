use base64;
use chrono::{DateTime, Utc};
use serde::Serialize;
use solana_sdk::{
    clock::Slot,
    transaction::Transaction as SolanaTransaction,
};
use solana_transaction_status::UiTransactionStatusMeta;

#[derive(Serialize)]
pub struct Transaction {
    block_timestamp: Option<DateTime<Utc>>,
    slot: u64,
    transaction_id: String,
    is_successful: bool,
    error: String,
    fee: u64,
    accounts: Vec<Account>,
    instructions: Vec<Instruction>,
    log_messages: Vec<String>,
}

#[derive(Serialize)]
struct Account {
    address: String,
    pre_sol_balance: u64,
    post_sol_balance: u64,
    pre_token_balances: Vec<TokenBalance>,
    post_token_balances: Vec<TokenBalance>,
}

#[derive(Serialize)]
struct TokenBalance {
    mint: String,
    amount: String,
}

#[derive(Serialize)]
struct Instruction {
    program_id: String,
    accounts: Vec<String>,
    //Base64 encoded data buffer
    data: String,
}

impl Transaction {
    // trim amount to max decimal precision
    // allows in a BigQuery Decimal type
    fn trim_decimals(amount: &String) -> &str {
        const MAX_BQ_DECIMALS: usize = 9;
        let parts: Vec<&str> = amount.split('.').collect();

        //any decimals at all?
        if parts.len() < 2 {
            return &amount[..];
        }

        //less than the max decimals?
        let decimals = parts[1].len();
        if decimals <= MAX_BQ_DECIMALS {
            return &amount[..];
        }

        //return the amount with the
        //the full integer part, the period, and the max decimals
        let max_length = parts[0].len() + 1 + MAX_BQ_DECIMALS;
        return &amount[..max_length];
    }

    pub fn new(
        block_timestamp: &Option<DateTime<Utc>>,
        slot: Slot,
        meta: &UiTransactionStatusMeta,
        solana_transaction: &SolanaTransaction,
    ) -> Transaction {
        let mut transaction = Transaction {
            block_timestamp: *block_timestamp,
            slot: slot,
            transaction_id: solana_transaction.signatures[0].to_string(),
            is_successful: false,
            error: String::from(""),
            fee: meta.fee,
            accounts: Vec::new(),
            instructions: Vec::new(),
            log_messages: Vec::new(),
        };
        if let Ok(_) = meta.status {
           transaction.is_successful = true;
        }
        if let Some(e) = &meta.err {
            transaction.error = e.to_string();
        }

        for (index, address) in solana_transaction.message.account_keys.iter().enumerate() {
            let account = Account {
                address: address.to_string(),
                pre_sol_balance: meta.pre_balances[index],
                post_sol_balance: meta.post_balances[index],
                pre_token_balances: Vec::new(),
                post_token_balances: Vec::new(),
            };
            transaction.accounts.push(account);
        }

        for instruction in &solana_transaction.message.instructions {
            let mut accounts: Vec<String> = Vec::new();
            for account_index in &instruction.accounts {
                accounts.push(transaction.accounts[*account_index as usize].address.clone());
            }
            transaction.instructions.push(Instruction {
                program_id: transaction.accounts[instruction.program_id_index as usize].address.clone(),
                accounts: accounts,
                data: base64::encode(&instruction.data[..]),
            });
        }

        if let Some(balances) = &meta.pre_token_balances {
            for balance in balances {
                let token_balance = TokenBalance {
                    mint: balance.mint.clone(),
                    amount: Self::trim_decimals(&balance.ui_token_amount.ui_amount_string).to_owned(),
                };
                transaction.accounts[balance.account_index as usize]
                    .pre_token_balances.push(token_balance);
            }
        }
        if let Some(balances) = &meta.post_token_balances {
            for balance in balances {
                let token_balance = TokenBalance {
                    mint: balance.mint.clone(),
                    amount: Self::trim_decimals(&balance.ui_token_amount.ui_amount_string).to_owned(),
                };
                transaction.accounts[balance.account_index as usize]
                    .post_token_balances.push(token_balance);
            }
        }
        match &meta.log_messages {
            None => {
                transaction.log_messages = Vec::new();
            }
            Some(messages) => {
                transaction.log_messages = messages.to_vec();
            }
        }

        return transaction;
    }
}
