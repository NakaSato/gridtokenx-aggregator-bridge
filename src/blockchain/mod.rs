use anyhow::{Result, anyhow};
use solana_client::rpc_client::RpcClient;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::transaction::Transaction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::system_program;
use std::str::FromStr;
use tracing::info;

pub struct BlockchainClient {
    rpc_client: RpcClient,
    authority: Keypair,
    oracle_program_id: Pubkey,
}

impl BlockchainClient {
    pub fn new(rpc_url: &str) -> Result<Self> {
        let rpc_client = RpcClient::new(rpc_url.to_string());
        
        let wallet_path = std::env::var("AUTHORITY_WALLET_PATH")
            .unwrap_or_else(|_| "gridtokenx-api/dev-wallet.json".to_string());
        
        // In a real environment, we'd load this from a secret manager
        let authority = if std::path::Path::new(&wallet_path).exists() {
            solana_sdk::signature::read_keypair_file(&wallet_path)
                .map_err(|e| anyhow!("Failed to read authority keypair: {}", e))?
        } else {
            Keypair::new()
        };

        let oracle_program_id_str = std::env::var("SOLANA_ORACLE_PROGRAM_ID")
            .unwrap_or_else(|_| "YEyAcHFbsV6e4E3G3H1ZBoQJ7YiMVaZf3vrdcVxjkAT".to_string());
        let oracle_program_id = Pubkey::from_str(&oracle_program_id_str)?;

        info!("🔑 Authority Loaded: {}", authority.pubkey());
        info!("🔗 Oracle Program: {}", oracle_program_id);

        Ok(Self {
            rpc_client,
            authority,
            oracle_program_id,
        })
    }

    pub async fn submit_meter_reading(
        &self,
        meter_id: String,
        produced: u64,
        consumed: u64,
        timestamp: i64,
    ) -> Result<String> {
        // Solana PDA seeds are limited to 32 bytes. UUID strings with hyphens are 36 bytes.
        // We remove hyphens to get a 32-character hex string.
        let seed_id = meter_id.replace("-", "");
        
        let (oracle_data_pda, _) = Pubkey::find_program_address(&[b"oracle_data"], &self.oracle_program_id);
        let (meter_state_pda, _) = Pubkey::find_program_address(&[b"meter", seed_id.as_bytes()], &self.oracle_program_id);

        // submit_meter_reading discriminator: [181, 247, 196, 139, 78, 88, 192, 206]
        // This is the Anchor discriminator for the "submit_meter_reading" instruction.
        let mut data = Vec::new();
        data.extend_from_slice(&[181, 247, 196, 139, 78, 88, 192, 206]);
        
        // Pack arguments (Borsh)
        // meter_id (String) -> 4 bytes len + bytes
        let meter_id_bytes = seed_id.as_bytes();
        data.extend_from_slice(&(meter_id_bytes.len() as u32).to_le_bytes());
        data.extend_from_slice(meter_id_bytes);
        
        data.extend_from_slice(&produced.to_le_bytes());
        data.extend_from_slice(&consumed.to_le_bytes());
        data.extend_from_slice(&timestamp.to_le_bytes());

        let accounts = vec![
            AccountMeta::new_readonly(oracle_data_pda, false),
            AccountMeta::new(meter_state_pda, false),
            AccountMeta::new(self.authority.pubkey(), true),
            AccountMeta::new_readonly(system_program::id(), false),
        ];

        let instruction = Instruction {
            program_id: self.oracle_program_id,
            accounts,
            data,
        };

        let recent_blockhash = self.rpc_client.get_latest_blockhash()?;
        let transaction = Transaction::new_signed_with_payer(
            &[instruction],
            Some(&self.authority.pubkey()),
            &[&self.authority],
            recent_blockhash,
        );

        let signature = self.rpc_client.send_and_confirm_transaction(&transaction)?;
        
        Ok(signature.to_string())
    }
}
