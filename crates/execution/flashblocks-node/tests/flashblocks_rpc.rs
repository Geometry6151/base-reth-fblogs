//! Integration tests covering the Flashblocks RPC surface area.

use std::{str::FromStr, time::Duration};

use DoubleCounter::DoubleCounterInstance;
use alloy_consensus::{Transaction, constants::EMPTY_WITHDRAWALS};
use alloy_eips::{BlockNumberOrTag, Decodable2718, Encodable2718, eip7685::EMPTY_REQUESTS_HASH};
use alloy_network::{ReceiptResponse, TransactionResponse};
use alloy_primitives::{Address, B256, Bytes, TxHash, U256, address, b256, bytes, keccak256};
use alloy_provider::Provider;
use alloy_rpc_client::RpcClient;
use alloy_rpc_types::{
    BlockOverrides,
    simulate::{SimBlock, SimulatePayload},
    state::{AccountOverride, StateOverride},
};
use alloy_rpc_types_engine::PayloadId;
use alloy_rpc_types_eth::{TransactionInput, error::EthRpcErrorCode};
use base_common_consensus::TxDeposit;
use base_common_flashblocks::{
    ExecutionPayloadBaseV1, ExecutionPayloadFlashblockDeltaV1, Flashblock, Metadata,
};
use base_common_network::Base;
use base_common_rpc_types::BaseTransactionRequest;
use base_flashblocks::{
    FastFlashblockLogsDelta, FlashblockDryRunResult, FlashblockLogsBatch, FlashblockSnapshotId,
    FlashblocksAPI, FlashblocksMode,
};
use base_flashblocks_node::test_harness::FlashblocksHarness;
use base_node_runner::test_utils::L1_BLOCK_INFO_DEPOSIT_TX;
use base_test_utils::{Account, DoubleCounter};
use eyre::Result;
use futures::{SinkExt, StreamExt, future::join_all};
use reth_revm::context::TransactionType;
use reth_rpc_eth_api::RpcReceipt;
use serde_json::json;
use serial_test::serial;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

// LogEmitterB: Emits LOG1 with TEST_LOG_TOPIC_0 when called
// Runtime bytecode:
//   PUSH32 0x01       ; data to log (32 bytes of value 1)
//   PUSH1 0x00        ; memory offset to store data
//   MSTORE            ; store data at memory[0:32]
//   PUSH32 topic0     ; TEST_LOG_TOPIC_0
//   PUSH1 0x20        ; log data size (32 bytes)
//   PUSH1 0x00        ; log data offset
//   LOG1              ; emit log with 1 topic
//   STOP              ; end execution
const LOG_EMITTER_B_RUNTIME: &str = concat!(
    "7f",
    "0000000000000000000000000000000000000000000000000000000000000001", // PUSH32 data
    "6000",                                                             // PUSH1 0
    "52",                                                               // MSTORE
    "7f",
    "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", // PUSH32 topic0
    "6020",                                                             // PUSH1 32
    "6000",                                                             // PUSH1 0
    "a1",                                                               // LOG1
    "00",                                                               // STOP
);

// LogEmitterA: Emits LOG2 with topic0 and topic1, then CALLs LogEmitterB
// Runtime bytecode (LOG_EMITTER_B_ADDR will be patched in):
//   PUSH32 data       ; 1 ETH in wei as log data
//   PUSH1 0x00        ; memory offset
//   MSTORE            ; store at memory[0:32]
//   PUSH32 topic1     ; TEST_LOG_TOPIC_1
//   PUSH32 topic0     ; TEST_LOG_TOPIC_0
//   PUSH1 0x20        ; size
//   PUSH1 0x00        ; offset
//   LOG2              ; emit log with 2 topics
//   ; Now CALL LogEmitterB
//   PUSH1 0x00        ; retSize
//   PUSH1 0x00        ; retOffset
//   PUSH1 0x00        ; argsSize
//   PUSH1 0x00        ; argsOffset
//   PUSH1 0x00        ; value
//   PUSH20 addr       ; LogEmitterB address (patched)
//   PUSH2 0xffff      ; gas
//   CALL              ; call LogEmitterB
//   STOP
fn log_emitter_a_runtime(log_emitter_b_addr: Address) -> String {
    // Convert address to hex without 0x prefix
    let addr_hex = format!("{log_emitter_b_addr:040x}");
    format!(
        concat!(
            "7f",
            "0000000000000000000000000000000000000000000000000de0b6b3a7640000", // PUSH32 data (1 ETH)
            "6000",                                                             // PUSH1 0
            "52",                                                               // MSTORE
            "7f",
            "000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266", // PUSH32 topic1
            "7f",
            "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", // PUSH32 topic0
            "6020",                                                             // PUSH1 32
            "6000",                                                             // PUSH1 0
            "a2",                                                               // LOG2
            "6000",                                                             // PUSH1 0 (retSize)
            "6000", // PUSH1 0 (retOffset)
            "6000", // PUSH1 0 (argsSize)
            "6000", // PUSH1 0 (argsOffset)
            "6000", // PUSH1 0 (value)
            "73",
            "{addr}", // PUSH20 LogEmitterB address
            "61ffff", // PUSH2 0xffff (gas)
            "f1",     // CALL
            "00",     // STOP
        ),
        addr = addr_hex
    )
}

// Wrap runtime code in init code that returns it
// Init code: CODECOPY runtime to memory[0:size], RETURN it
fn wrap_in_init_code(runtime_hex: &str) -> Bytes {
    // Parse hex string (handle both with and without 0x prefix)
    let hex_str = runtime_hex.strip_prefix("0x").unwrap_or(runtime_hex);
    let mut runtime_bytes = Vec::new();
    for i in (0..hex_str.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex_str[i..i + 2], 16).expect("valid hex");
        runtime_bytes.push(byte);
    }
    let runtime_size = runtime_bytes.len();

    // Init code:
    //   PUSH1 runtime_size
    //   PUSH1 init_size (12 bytes)
    //   PUSH1 0
    //   CODECOPY
    //   PUSH1 runtime_size
    //   PUSH1 0
    //   RETURN
    // Total init: 12 bytes
    let init_size = 12u8;
    let mut init_code = vec![
        0x60,
        runtime_size as u8, // PUSH1 runtime_size
        0x60,
        init_size, // PUSH1 init_size
        0x60,
        0x00, // PUSH1 0
        0x39, // CODECOPY
        0x60,
        runtime_size as u8, // PUSH1 runtime_size
        0x60,
        0x00, // PUSH1 0
        0xf3, // RETURN
    ];
    init_code.extend(runtime_bytes);
    Bytes::from(init_code)
}

fn block_env_reader_runtime(opcode: u8) -> Bytes {
    Bytes::from(vec![opcode, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3])
}

fn block_number_revert_if_eq_runtime(expected_block_number: u8) -> Bytes {
    Bytes::from(vec![
        0x43, // NUMBER
        0x60,
        expected_block_number, // PUSH1 expected_block_number
        0x14,                  // EQ
        0x60,
        0x08, // PUSH1 revert_dest
        0x57, // JUMPI
        0x00, // STOP
        0x5b, // JUMPDEST
        0x60,
        0x00, // PUSH1 0
        0x60,
        0x00, // PUSH1 0
        0xfd, // REVERT
    ])
}

fn count1_snapshot_guard_runtime(counter_address: Address) -> Bytes {
    let selector = keccak256("count1()");
    let mut runtime = vec![0x63];
    runtime.extend_from_slice(&selector[..4]);
    runtime.extend_from_slice(&[
        0x60, 0x00, 0x52, // mstore(0x00, selector)
        0x60, 0x20, // out size
        0x60, 0x00, // out offset
        0x60, 0x04, // in size
        0x60, 0x1c, // in offset
        0x73, // PUSH20 counter_address
    ]);
    runtime.extend_from_slice(counter_address.as_slice());
    runtime.extend_from_slice(&[
        0x61, 0xff, 0xff, // gas
        0xfa, // STATICCALL
        0x15, 0x60, 0x44, 0x57, // revert if call failed
        0x3d, 0x60, 0x20, 0x14, 0x15, 0x60, 0x44, 0x57, // revert if returndatasize != 32
        0x60, 0x00, 0x51, 0x60, 0x02, 0x14, 0x15, 0x60, 0x44, 0x57, // revert if count1() != 2
        0x60, 0x00, 0x60, 0x00, 0xf3, // return success with empty data
        0x5b, 0x60, 0x00, 0x60, 0x00, 0xfd, // revert
    ]);
    Bytes::from(runtime)
}

fn code_override(address: Address, code: Bytes) -> StateOverride {
    [(address, AccountOverride::default().with_code(code))].into_iter().collect()
}

fn u256_return_data(value: u64) -> Bytes {
    Bytes::copy_from_slice(&U256::from(value).to_be_bytes::<32>())
}

fn unique_l1_block_info_deposit_tx(block_number: u64) -> Bytes {
    let mut deposit = TxDeposit::decode_2718(&mut L1_BLOCK_INFO_DEPOSIT_TX.as_ref())
        .expect("L1_BLOCK_INFO_DEPOSIT_TX must decode as a deposit transaction");
    let mut source_hash = [0u8; 32];
    source_hash[24..].copy_from_slice(&block_number.to_be_bytes());
    deposit.source_hash = B256::from(source_hash);
    let mut buf = Vec::with_capacity(deposit.encode_2718_len());
    deposit.encode_2718(&mut buf);
    buf.into()
}

struct TestSetup {
    mode: FlashblocksMode,
    harness: FlashblocksHarness,
    txn_details: TransactionDetails,
    canonical_parent_hash: B256,
}

type TestWsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct TransactionDetails {
    counter_deployment_tx: Bytes,
    counter_address: Address,

    counter_increment_tx: Bytes,

    counter_increment2_tx: Bytes,

    alice_eth_transfer_tx: Bytes,
    alice_eth_transfer_hash: TxHash,

    // Log-emitting contracts for log tests
    log_emitter_b_deployment_tx: Bytes,
    log_emitter_b_address: Address,

    log_emitter_a_deployment_tx: Bytes,
    log_emitter_a_address: Address,

    log_trigger_tx: Bytes,
    log_trigger_hash: TxHash,

    // Balance transfer for balance test
    balance_transfer_tx: Bytes,
}

impl TestSetup {
    async fn new() -> Result<Self> {
        Self::new_with_mode(FlashblocksMode::Legacy).await
    }

    async fn new_with_mode(mode: FlashblocksMode) -> Result<Self> {
        let harness = FlashblocksHarness::new_with_mode(mode).await?;

        let provider = harness.provider();
        let canonical_parent_hash = harness.latest_block().hash();
        let deployer = Account::Deployer;
        let alice = Account::Alice;
        let bob = Account::Bob;

        // DoubleCounter deployment at nonce 0
        let (counter_deployment_tx, counter_address, _) = deployer
            .create_deployment_tx(DoubleCounter::BYTECODE.clone(), 0)
            .expect("should be able to sign DoubleCounter deployment txn");
        let counter = DoubleCounterInstance::new(counter_address, provider);
        let (increment1_tx, _) = deployer
            .sign_txn_request(counter.increment().into_transaction_request().nonce(1))
            .expect("should be able to sign increment() txn");
        let (increment2_tx, _) = deployer
            .sign_txn_request(counter.increment2().into_transaction_request().nonce(2))
            .expect("should be able to sign increment2() txn");

        // Alice's ETH transfer at nonce 0
        let (eth_transfer_tx, eth_transfer_hash) = alice
            .sign_txn_request(
                BaseTransactionRequest::default()
                    .from(alice.address())
                    .transaction_type(TransactionType::Eip1559.into())
                    .gas_limit(100_000)
                    .nonce(0)
                    .to(bob.address())
                    .value(U256::from_str("999999999000000000000000").unwrap()),
            )
            .expect("should be able to sign eth transfer txn");

        // Log-emitting contracts:
        // Deploy LogEmitterB at deployer nonce 3
        let log_emitter_b_address = deployer.address().create(3);
        let log_emitter_b_bytecode = wrap_in_init_code(LOG_EMITTER_B_RUNTIME);
        let (log_emitter_b_deployment_tx, _, _) = deployer
            .create_deployment_tx(log_emitter_b_bytecode, 3)
            .expect("should be able to sign LogEmitterB deployment txn");

        // Deploy LogEmitterA at deployer nonce 4 (knows LogEmitterB's address)
        let log_emitter_a_address = deployer.address().create(4);
        let log_emitter_a_runtime = log_emitter_a_runtime(log_emitter_b_address);
        let log_emitter_a_bytecode = wrap_in_init_code(&log_emitter_a_runtime);
        let (log_emitter_a_deployment_tx, _, _) = deployer
            .create_deployment_tx(log_emitter_a_bytecode, 4)
            .expect("should be able to sign LogEmitterA deployment txn");

        // Call LogEmitterA at deployer nonce 5 to trigger logs
        let (log_trigger_tx, log_trigger_hash) = deployer
            .sign_txn_request(
                BaseTransactionRequest::default()
                    .from(deployer.address())
                    .transaction_type(TransactionType::Eip1559.into())
                    .gas_limit(100_000)
                    .nonce(5)
                    .to(log_emitter_a_address),
            )
            .expect("should be able to sign log trigger txn");

        // Balance transfer: alice sends PENDING_BALANCE wei to TEST_ADDRESS at nonce 1
        let (balance_transfer_tx, _) = alice
            .sign_txn_request(
                BaseTransactionRequest::default()
                    .from(alice.address())
                    .transaction_type(TransactionType::Eip1559.into())
                    .gas_limit(21_000)
                    .nonce(1)
                    .to(TEST_ADDRESS)
                    .value(U256::from(PENDING_BALANCE)),
            )
            .expect("should be able to sign balance transfer txn");

        let txn_details = TransactionDetails {
            counter_deployment_tx,
            counter_address,
            counter_increment_tx: increment1_tx,
            counter_increment2_tx: increment2_tx,
            alice_eth_transfer_tx: eth_transfer_tx,
            alice_eth_transfer_hash: eth_transfer_hash,
            log_emitter_b_deployment_tx,
            log_emitter_b_address,
            log_emitter_a_deployment_tx,
            log_emitter_a_address,
            log_trigger_tx,
            log_trigger_hash,
            balance_transfer_tx,
        };

        Ok(Self { mode, harness, txn_details, canonical_parent_hash })
    }

    fn create_first_payload(&self) -> Flashblock {
        Flashblock {
            payload_id: PayloadId::new([0; 8]),
            index: 0,
            base: Some(ExecutionPayloadBaseV1 {
                parent_beacon_block_root: TEST_PARENT_BEACON_BLOCK_ROOT,
                parent_hash: self.canonical_parent_hash,
                fee_recipient: Address::ZERO,
                prev_randao: B256::default(),
                block_number: 1,
                gas_limit: 30_000_000,
                timestamp: 0,
                extra_data: Bytes::new(),
                base_fee_per_gas: U256::ZERO,
            }),
            diff: ExecutionPayloadFlashblockDeltaV1 {
                blob_gas_used: Some(0),
                transactions: vec![L1_BLOCK_INFO_DEPOSIT_TX],
                withdrawals_root: EMPTY_WITHDRAWALS,
                ..Default::default()
            },
            metadata: Metadata { block_number: 1 },
        }
    }

    fn create_second_payload(&self) -> Flashblock {
        Flashblock {
            payload_id: PayloadId::new([0; 8]),
            index: 1,
            base: None,
            diff: ExecutionPayloadFlashblockDeltaV1 {
                state_root: B256::default(),
                receipts_root: B256::default(),
                gas_used: 0,
                block_hash: B256::default(),
                blob_gas_used: Some(0),
                transactions: vec![
                    DEPOSIT_TX,
                    self.txn_details.alice_eth_transfer_tx.clone(),
                    self.txn_details.counter_deployment_tx.clone(),
                    self.txn_details.counter_increment_tx.clone(),
                    self.txn_details.counter_increment2_tx.clone(),
                    // Log-emitting contracts and trigger
                    self.txn_details.log_emitter_b_deployment_tx.clone(),
                    self.txn_details.log_emitter_a_deployment_tx.clone(),
                    self.txn_details.log_trigger_tx.clone(),
                    // Balance transfer to TEST_ADDRESS
                    self.txn_details.balance_transfer_tx.clone(),
                ],
                withdrawals: Vec::new(),
                logs_bloom: Default::default(),
                withdrawals_root: EMPTY_WITHDRAWALS,
            },
            metadata: Metadata { block_number: 1 },
        }
    }

    fn create_third_payload(&self, parent_hash: B256) -> Flashblock {
        Flashblock {
            payload_id: PayloadId::new([1; 8]),
            index: 0,
            base: Some(ExecutionPayloadBaseV1 {
                parent_beacon_block_root: TEST_PARENT_BEACON_BLOCK_ROOT,
                parent_hash,
                fee_recipient: Address::ZERO,
                prev_randao: B256::default(),
                block_number: 2,
                gas_limit: 30_000_000,
                timestamp: 1,
                extra_data: Bytes::new(),
                base_fee_per_gas: U256::ZERO,
            }),
            diff: ExecutionPayloadFlashblockDeltaV1 {
                blob_gas_used: Some(0),
                transactions: vec![unique_l1_block_info_deposit_tx(2)],
                withdrawals_root: EMPTY_WITHDRAWALS,
                ..Default::default()
            },
            metadata: Metadata { block_number: 2 },
        }
    }

    fn create_fourth_payload(&self) -> Flashblock {
        Flashblock {
            payload_id: PayloadId::new([1; 8]),
            index: 1,
            base: None,
            diff: ExecutionPayloadFlashblockDeltaV1 {
                state_root: B256::default(),
                receipts_root: B256::default(),
                gas_used: 0,
                block_hash: B256::with_last_byte(0x04),
                blob_gas_used: Some(0),
                transactions: vec![unique_l1_block_info_deposit_tx(3)],
                withdrawals: Vec::new(),
                logs_bloom: Default::default(),
                withdrawals_root: EMPTY_WITHDRAWALS,
            },
            metadata: Metadata { block_number: 2 },
        }
    }

    fn create_invalidating_gap_payload(&self) -> Flashblock {
        Flashblock {
            payload_id: PayloadId::new([0; 8]),
            index: 3,
            base: None,
            diff: ExecutionPayloadFlashblockDeltaV1 {
                state_root: B256::default(),
                receipts_root: B256::default(),
                gas_used: 0,
                block_hash: B256::with_last_byte(0xfe),
                blob_gas_used: Some(0),
                transactions: vec![],
                withdrawals: Vec::new(),
                logs_bloom: Default::default(),
                withdrawals_root: EMPTY_WITHDRAWALS,
            },
            metadata: Metadata { block_number: 1 },
        }
    }

    fn count1(&self) -> BaseTransactionRequest {
        let counter =
            DoubleCounterInstance::new(self.txn_details.counter_address, self.harness.provider());
        counter.count1().into_transaction_request()
    }

    fn count2(&self) -> BaseTransactionRequest {
        let counter =
            DoubleCounterInstance::new(self.txn_details.counter_address, self.harness.provider());
        counter.count2().into_transaction_request()
    }

    fn count1_from_alice(&self) -> BaseTransactionRequest {
        self.count1().from(Account::Alice.address()).gas_limit(100_000)
    }

    fn block_number_guard_create_from_alice(
        &self,
        expected_block_number: u8,
    ) -> BaseTransactionRequest {
        BaseTransactionRequest::default()
            .from(Account::Alice.address())
            .gas_limit(100_000)
            .input(TransactionInput::new(block_number_revert_if_eq_runtime(expected_block_number)))
    }

    async fn send_flashblock(&self, flashblock: Flashblock) -> Result<()> {
        self.harness.send_flashblock(flashblock).await
    }

    async fn send_test_payloads(&self) -> Result<()> {
        let base_payload = self.create_first_payload();
        self.send_flashblock(base_payload).await?;

        let second_payload = self.create_second_payload();
        self.send_flashblock(second_payload).await?;

        Ok(())
    }

    async fn send_test_payloads_and_wait_for_latest_hot_snapshot_id(
        &self,
    ) -> Result<FlashblockSnapshotId> {
        let mut first_payload = self.create_first_payload();
        first_payload.diff.block_hash = B256::with_last_byte(0x01);

        let mut second_payload = self.create_second_payload();
        second_payload.diff.block_hash = B256::with_last_byte(0x02);
        let expected_block_number = second_payload.metadata.block_number;
        let expected_flashblock_index = second_payload.index;

        self.send_flashblock(first_payload).await?;
        self.send_flashblock(second_payload).await?;

        self.wait_for_latest_hot_snapshot_id(expected_block_number, expected_flashblock_index).await
    }

    async fn wait_for_latest_hot_snapshot_id(
        &self,
        expected_block_number: u64,
        expected_flashblock_index: u64,
    ) -> Result<FlashblockSnapshotId> {
        let flashblocks_state = self.harness.flashblocks_state();
        let deadline = tokio::time::Instant::now() + HOT_SNAPSHOT_WAIT_TIMEOUT;

        loop {
            if let Some(snapshot) = flashblocks_state.get_latest_hot_snapshot() {
                let snapshot_id = snapshot.snapshot_id;
                if snapshot_id.block_number() == expected_block_number
                    && snapshot_id.flashblock_index() == expected_flashblock_index
                {
                    return Ok(snapshot_id);
                }
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(eyre::eyre!(
                    "timed out waiting for latest hot snapshot for block {} flashblock {}",
                    expected_block_number,
                    expected_flashblock_index,
                ));
            }

            tokio::time::sleep(HOT_SNAPSHOT_POLL_INTERVAL).await;
        }
    }

    async fn wait_for_latest_hot_dry_run_snapshot_id(
        &self,
        expected_snapshot_id: FlashblockSnapshotId,
    ) -> Result<FlashblockSnapshotId> {
        let flashblocks_state = self.harness.flashblocks_state();
        let deadline = tokio::time::Instant::now() + HOT_SNAPSHOT_WAIT_TIMEOUT;

        loop {
            if let Some(warm_state) = flashblocks_state.get_latest_hot_dry_run_state() {
                if warm_state.snapshot_id == expected_snapshot_id {
                    return Ok(warm_state.snapshot_id);
                }
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(eyre::eyre!(
                    "timed out waiting for latest hot dry-run state for snapshot {:?}",
                    expected_snapshot_id,
                ));
            }

            tokio::time::sleep(HOT_SNAPSHOT_POLL_INTERVAL).await;
        }
    }

    async fn wait_for_latest_hot_dry_run_state_clear(&self) -> Result<()> {
        let flashblocks_state = self.harness.flashblocks_state();
        let deadline = tokio::time::Instant::now() + HOT_SNAPSHOT_WAIT_TIMEOUT;

        loop {
            if flashblocks_state.get_latest_hot_dry_run_state().is_none() {
                return Ok(());
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(eyre::eyre!("timed out waiting for latest hot dry-run state to clear"));
            }

            tokio::time::sleep(HOT_SNAPSHOT_POLL_INTERVAL).await;
        }
    }

    async fn wait_for_latest_hot_snapshot_clear(&self) -> Result<()> {
        let flashblocks_state = self.harness.flashblocks_state();
        let deadline = tokio::time::Instant::now() + HOT_SNAPSHOT_WAIT_TIMEOUT;

        loop {
            if flashblocks_state.get_latest_hot_snapshot().is_none() {
                return Ok(());
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(eyre::eyre!("timed out waiting for latest hot snapshot to clear"));
            }

            tokio::time::sleep(HOT_SNAPSHOT_POLL_INTERVAL).await;
        }
    }

    async fn subscribe_fast_flashblock_logs(&self) -> Result<TestWsStream> {
        let ws_url = self.harness.ws_url();
        let (mut ws_stream, _) = connect_async(&ws_url).await?;

        ws_stream
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": ["newFastFlashblockLogs"]
                })
                .to_string()
                .into(),
            ))
            .await?;

        let response = ws_stream.next().await.unwrap()?;
        let sub: serde_json::Value = serde_json::from_str(response.to_text()?)?;
        assert_eq!(sub["jsonrpc"], "2.0");
        assert_eq!(sub["id"], 1);

        Ok(ws_stream)
    }

    async fn send_flashblock_and_collect_fast_delta(
        &self,
        ws_stream: &mut TestWsStream,
        flashblock: Flashblock,
    ) -> Result<FastFlashblockLogsDelta> {
        self.send_flashblock(flashblock).await?;

        let notification = tokio::time::timeout(FAST_DELTA_NOTIFICATION_TIMEOUT, ws_stream.next())
            .await
            .map_err(|_| {
                eyre::eyre!(
                    "timed out waiting for newFastFlashblockLogs notification after sending flashblock"
                )
            })?
            .ok_or_else(|| {
                eyre::eyre!("websocket fast delta stream closed before notification arrived")
            })??;
        let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
        Ok(serde_json::from_value(notif["params"]["result"].clone())?)
    }

    async fn collect_fast_deltas(
        &self,
        payloads: Vec<Flashblock>,
    ) -> Result<Vec<FastFlashblockLogsDelta>> {
        let mut ws_stream = self.subscribe_fast_flashblock_logs().await?;
        let mut deltas = Vec::with_capacity(payloads.len());

        for payload in payloads {
            deltas
                .push(self.send_flashblock_and_collect_fast_delta(&mut ws_stream, payload).await?);
        }

        Ok(deltas)
    }

    async fn collect_fast_deltas_with_rollover(
        &self,
    ) -> Result<(Vec<FastFlashblockLogsDelta>, B256)> {
        let mut ws_stream = self.subscribe_fast_flashblock_logs().await?;
        let first_delta = self
            .send_flashblock_and_collect_fast_delta(&mut ws_stream, self.create_first_payload())
            .await?;
        let second_delta = self
            .send_flashblock_and_collect_fast_delta(&mut ws_stream, self.create_second_payload())
            .await?;
        let next_block_parent_hash = self.pending_parent_hash_for_next_block().await?;
        let third_delta = self
            .send_flashblock_and_collect_fast_delta(
                &mut ws_stream,
                self.create_third_payload(next_block_parent_hash),
            )
            .await?;

        Ok((vec![first_delta, second_delta, third_delta], next_block_parent_hash))
    }

    async fn pending_parent_hash_for_next_block(&self) -> Result<B256> {
        match self.mode {
            FlashblocksMode::Legacy => Ok(self
                .harness
                .provider()
                .get_block_by_number(BlockNumberOrTag::Pending)
                .await?
                .expect("legacy mode should expose a pending block after the second flashblock")
                .hash()),
            FlashblocksMode::HotOnly => Ok(self
                .harness
                .flashblocks_state()
                .get_latest_hot_snapshot()
                .expect("latest hot snapshot should exist after fast delta")
                .latest_header
                .hash()),
        }
    }

    async fn send_raw_transaction_sync(
        &self,
        tx: Bytes,
        timeout_ms: Option<u64>,
    ) -> Result<RpcReceipt<Base>> {
        let url = self.harness.rpc_url();
        let client = RpcClient::new_http(url.parse()?);

        let receipt = client
            .request::<_, RpcReceipt<Base>>("eth_sendRawTransactionSync", (tx, timeout_ms))
            .await?;

        Ok(receipt)
    }

    async fn ws_rpc_request(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let (mut ws_stream, _) = connect_async(&self.harness.ws_url()).await?;

        ws_stream
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": method,
                    "params": params,
                })
                .to_string()
                .into(),
            ))
            .await?;

        let response = ws_stream.next().await.expect("websocket rpc response expected")?;

        Ok(serde_json::from_str(response.to_text()?)?)
    }

    async fn latest_dry_run(
        &self,
        transaction: BaseTransactionRequest,
    ) -> Result<FlashblockDryRunResult> {
        let response =
            self.ws_rpc_request("eth_baseDryRunLatestFlashblock", json!([transaction])).await?;

        Ok(serde_json::from_value(response["result"].clone())?)
    }

    async fn dry_run_at(
        &self,
        snapshot_id: FlashblockSnapshotId,
        transaction: BaseTransactionRequest,
    ) -> Result<FlashblockDryRunResult> {
        let response = self
            .ws_rpc_request("eth_baseDryRunAtFlashblock", json!([snapshot_id, transaction]))
            .await?;

        Ok(serde_json::from_value(response["result"].clone())?)
    }
}

// Test constants
const FAST_DELTA_NOTIFICATION_TIMEOUT: Duration = Duration::from_secs(5);
const HOT_SNAPSHOT_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const HOT_SNAPSHOT_POLL_INTERVAL: Duration = Duration::from_millis(25);

const TEST_ADDRESS: Address = address!("0x1234567890123456789012345678901234567890");
const PENDING_BALANCE: u64 = 4660;

const DEPOSIT_SENDER: Address = address!("0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001");
const DEPOSIT_TX: Bytes = bytes!(
    "0x7ef8f8a042a8ae5ec231af3d0f90f68543ec8bca1da4f7edd712d5b51b490688355a6db794deaddeaddeaddeaddeaddeaddeaddeaddead00019442000000000000000000000000000000000000158080830f424080b8a4440a5e200000044d000a118b00000000000000040000000067cb7cb0000000000077dbd4000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000014edd27304108914dd6503b19b9eeb9956982ef197febbeeed8a9eac3dbaaabdf000000000000000000000000fc56e7272eebbba5bc6c544e159483c4a38f8ba3"
);
const DEPOSIT_GAS_USED: u64 = 24770;
const DEPOSIT_TX_HASH: TxHash =
    b256!("0x2be2e6f8b01b03b87ae9f0ebca8bbd420f174bef0fbcc18c7802c5378b78f548");

// Test log topics - these represent common events
const TEST_LOG_TOPIC_0: B256 =
    b256!("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"); // Transfer event
const TEST_LOG_TOPIC_1: B256 =
    b256!("0x000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266"); // From address

// Test parent beacon block root for flashblock tests
const TEST_PARENT_BEACON_BLOCK_ROOT: B256 =
    b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");

fn latest_dry_run_path_counts() -> (u64, u64, u64) {
    FlashblockDryRunResult::latest_rpc_path_counts_for_testing()
}

fn assert_latest_dry_run_used_sidecar_hit(before: (u64, u64, u64), after: (u64, u64, u64)) {
    assert_eq!(after.0 - before.0, 1, "expected one sidecar-hit latest dry-run path");
    assert_eq!(after.1 - before.1, 0, "expected no direct latest dry-run fallback");
}

fn assert_latest_dry_run_used_direct_fallback(before: (u64, u64, u64), after: (u64, u64, u64)) {
    assert_eq!(after.0 - before.0, 0, "expected no sidecar-hit latest dry-run path");
    assert_eq!(after.1 - before.1, 1, "expected one direct latest dry-run fallback");
}

#[tokio::test]
async fn test_get_pending_block() -> Result<()> {
    let setup = TestSetup::new().await?;
    let provider = setup.harness.provider();

    let latest_block = provider
        .get_block_by_number(BlockNumberOrTag::Latest)
        .await?
        .expect("latest block expected");
    assert_eq!(latest_block.number(), 0);

    // Querying pending block when it does not exist yet
    let pending_block = provider
        .get_block_by_number(BlockNumberOrTag::Pending)
        .await?
        .expect("latest block expected");

    assert_eq!(pending_block.number(), latest_block.number());
    assert_eq!(pending_block.hash(), latest_block.hash());

    let base_payload = setup.create_first_payload();
    setup.send_flashblock(base_payload).await?;

    // Query pending block after sending the base payload with an empty delta
    let pending_block = provider
        .get_block_by_number(BlockNumberOrTag::Pending)
        .await?
        .expect("pending block expected");

    assert_eq!(pending_block.number(), 1);
    assert_eq!(pending_block.transactions.hashes().len(), 1); // L1Info transaction

    let second_payload = setup.create_second_payload();
    setup.send_flashblock(second_payload).await?;

    // Query pending block after sending the second payload with transactions
    let block = provider
        .get_block_by_number(BlockNumberOrTag::Pending)
        .await?
        .expect("pending block expected");

    assert_eq!(block.number(), 1);
    // First flashblock: 1 L1Info transaction
    // Second flashblock: 1 DEPOSIT_TX + 1 alice ETH transfer + 1 counter deploy + 1 counter increment + 1 counter increment2
    // + 1 LogEmitterB deploy + 1 LogEmitterA deploy + 1 log trigger + 1 balance transfer
    // Total: 1 + 9 = 10 transactions
    assert_eq!(block.transactions.hashes().len(), 10);

    Ok(())
}

#[tokio::test]
async fn test_get_balance_pending() -> Result<()> {
    let setup = TestSetup::new().await?;
    let provider = setup.harness.provider();

    setup.send_test_payloads().await?;

    let balance = provider.get_balance(TEST_ADDRESS).await?;
    assert_eq!(balance, U256::ZERO);

    let pending_balance = provider.get_balance(TEST_ADDRESS).pending().await?;
    assert_eq!(pending_balance, U256::from(PENDING_BALANCE));
    Ok(())
}

#[tokio::test]
async fn test_get_transaction_by_hash_pending() -> Result<()> {
    let setup = TestSetup::new().await?;
    let provider = setup.harness.provider();

    assert!(provider.get_transaction_by_hash(DEPOSIT_TX_HASH).await?.is_none());
    assert!(
        provider
            .get_transaction_by_hash(setup.txn_details.alice_eth_transfer_hash)
            .await?
            .is_none()
    );

    setup.send_test_payloads().await?;

    let tx1 = provider.get_transaction_by_hash(DEPOSIT_TX_HASH).await?.expect("tx1 expected");
    assert_eq!(tx1.tx_hash(), DEPOSIT_TX_HASH);
    assert_eq!(tx1.from(), DEPOSIT_SENDER);

    let tx2 = provider
        .get_transaction_by_hash(setup.txn_details.alice_eth_transfer_hash)
        .await?
        .expect("tx2 expected");
    assert_eq!(tx2.tx_hash(), setup.txn_details.alice_eth_transfer_hash);
    assert_eq!(tx2.from(), Account::Alice.address());
    assert_eq!(tx2.inner.inner.as_eip1559().unwrap().to().unwrap(), Account::Bob.address());

    Ok(())
}

#[tokio::test]
async fn test_get_transaction_receipt_pending() -> Result<()> {
    let setup = TestSetup::new().await?;
    let provider = setup.harness.provider();

    let receipt = provider.get_transaction_receipt(DEPOSIT_TX_HASH).await?;
    assert!(receipt.is_none());

    setup.send_test_payloads().await?;

    let receipt =
        provider.get_transaction_receipt(DEPOSIT_TX_HASH).await?.expect("receipt expected");
    assert_eq!(receipt.gas_used(), DEPOSIT_GAS_USED);

    let receipt = provider
        .get_transaction_receipt(setup.txn_details.alice_eth_transfer_hash)
        .await?
        .expect("receipt expected");
    assert_eq!(receipt.gas_used(), 21000);

    Ok(())
}

#[tokio::test]
async fn test_get_transaction_count() -> Result<()> {
    let setup = TestSetup::new().await?;
    let provider = setup.harness.provider();

    let deployer_addr = Account::Deployer.address();
    let alice_addr = Account::Alice.address();

    assert_eq!(provider.get_transaction_count(DEPOSIT_SENDER).pending().await?, 0);
    assert_eq!(provider.get_transaction_count(deployer_addr).pending().await?, 0);
    assert_eq!(provider.get_transaction_count(alice_addr).pending().await?, 0);

    setup.send_test_payloads().await?;

    assert_eq!(provider.get_transaction_count(DEPOSIT_SENDER).pending().await?, 2);
    // Deployer has: counter deploy (0), counter increment (1), counter increment2 (2),
    // LogEmitterB deploy (3), LogEmitterA deploy (4), log trigger (5) = nonce 6
    assert_eq!(provider.get_transaction_count(deployer_addr).pending().await?, 6);
    // Alice has: big ETH transfer (0), balance transfer to TEST_ADDRESS (1) = nonce 2
    assert_eq!(provider.get_transaction_count(alice_addr).pending().await?, 2);

    Ok(())
}

#[tokio::test]
async fn test_eth_call() -> Result<()> {
    let setup = TestSetup::new().await?;
    let provider = setup.harness.provider();

    // Initially, the big spend will succeed because we haven't sent the test payloads yet
    let big_spend = BaseTransactionRequest::default()
        .from(Account::Alice.address())
        .transaction_type(0)
        .gas_limit(200000)
        .nonce(0)
        .to(Account::Bob.address())
        .value(U256::from(9999999999849942300000u128));

    let res = provider.call(big_spend.clone()).block(BlockNumberOrTag::Pending.into()).await;
    assert!(res.is_ok());

    setup.send_test_payloads().await?;

    // We included a big spending transaction in the payloads
    // and now don't have enough funds for this request, so this eth_call will fail
    let res =
        provider.call(big_spend.clone().nonce(3)).block(BlockNumberOrTag::Pending.into()).await;
    assert!(res.is_err());
    assert!(
        res.unwrap_err().as_error_resp().unwrap().message.contains("insufficient funds for gas")
    );

    // read count1 from counter contract
    let res_count1 = provider.call(setup.count1()).await;
    assert!(res_count1.is_ok());
    assert_eq!(U256::from_str(res_count1.unwrap().to_string().as_str()).unwrap(), U256::from(2));

    // read count2 from counter contract
    let res_count2 = provider.call(setup.count2()).await;
    assert!(res_count2.is_ok());
    assert_eq!(U256::from_str(res_count2.unwrap().to_string().as_str()).unwrap(), U256::from(2));

    Ok(())
}

#[tokio::test]
async fn test_eth_estimate_gas() -> Result<()> {
    let setup = TestSetup::new().await?;
    let provider = setup.harness.provider();

    // We ensure that eth_estimate_gas will succeed because we are on plain state
    let send_estimate_gas = BaseTransactionRequest::default()
        .from(Account::Alice.address())
        .transaction_type(0)
        .gas_limit(200000)
        .nonce(0)
        .to(Account::Bob.address())
        .value(U256::from(9999999999849942300000u128))
        .input(TransactionInput::new(bytes!("0x")));

    let res = provider
        .estimate_gas(send_estimate_gas.clone())
        .block(BlockNumberOrTag::Pending.into())
        .await;

    assert!(res.is_ok());

    setup.send_test_payloads().await?;

    // We included a heavy spending transaction and now don't have enough funds for this request, so
    // this eth_estimate_gas will fail
    let res = provider
        .estimate_gas(send_estimate_gas.nonce(4))
        .block(BlockNumberOrTag::Pending.into())
        .await;

    assert!(res.is_err());
    assert!(
        res.unwrap_err().as_error_resp().unwrap().message.contains("insufficient funds for gas")
    );

    Ok(())
}

#[tokio::test]
async fn test_base_estimate_gas_at_flashblock() -> Result<()> {
    let setup = TestSetup::new().await?;
    let canonical_parent_number = setup
        .harness
        .provider()
        .get_block_by_number(BlockNumberOrTag::Latest)
        .await?
        .expect("latest block expected")
        .number();
    let ws_url = setup.harness.ws_url();
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    ws_stream
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newFastFlashblockLogs"]
            })
            .to_string()
            .into(),
        ))
        .await?;

    let response = ws_stream.next().await.unwrap()?;
    let sub: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    assert_eq!(sub["jsonrpc"], "2.0");
    assert_eq!(sub["id"], 1);

    let first_payload = setup.create_first_payload();
    let snapshot_block_number =
        first_payload.base.as_ref().expect("flashblock base payload expected").block_number;
    assert_ne!(snapshot_block_number, canonical_parent_number);

    setup.send_flashblock(first_payload).await?;
    let _first_notification = ws_stream.next().await.unwrap()?;

    setup.send_flashblock(setup.create_second_payload()).await?;
    let notification = ws_stream.next().await.unwrap()?;
    let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
    let snapshot_id = notif["params"]["result"]["snapshotId"].clone();
    assert_fast_flashblock_snapshot_id(&snapshot_id);

    let url = setup.harness.rpc_url();
    let client = RpcClient::new_http(url.parse()?);
    let estimate_guard_address = address!("0x1000000000000000000000000000000000000001");
    let estimate_overrides = code_override(
        estimate_guard_address,
        count1_snapshot_guard_runtime(setup.txn_details.counter_address),
    );
    let estimate_request = || {
        BaseTransactionRequest::default()
            .from(Account::Alice.address())
            .to(estimate_guard_address)
            .input(TransactionInput::new(bytes!("0x")))
    };

    let estimate: U256 = client
        .request(
            "eth_baseEstimateGasAtFlashblock",
            (snapshot_id.clone(), estimate_request(), Some(estimate_overrides.clone())),
        )
        .await?;

    assert!(estimate > U256::ZERO, "expected snapshot-pinned estimate to succeed");

    client
        .request::<_, U256>(
            "eth_estimateGas",
            (estimate_request(), Some(BlockNumberOrTag::Latest), Some(estimate_overrides.clone())),
        )
        .await
        .expect_err("latest estimate should fail without snapshot state pinning");

    let block_env_guard_address = address!("0x1000000000000000000000000000000000000003");
    let block_env_request = || {
        BaseTransactionRequest::default()
            .from(Account::Alice.address())
            .to(block_env_guard_address)
            .input(TransactionInput::new(bytes!("0x")))
    };

    let snapshot_block_number_result: Bytes = client
        .request(
            "eth_baseCallAtFlashblock",
            (
                snapshot_id.clone(),
                block_env_request(),
                Some(code_override(block_env_guard_address, block_env_reader_runtime(0x43))),
                None::<Box<BlockOverrides>>,
            ),
        )
        .await?;

    assert_eq!(
        snapshot_block_number_result,
        u256_return_data(snapshot_block_number),
        "base call should execute against the synthetic flashblock header env"
    );

    let parent_block_number_guard = code_override(
        block_env_guard_address,
        block_number_revert_if_eq_runtime(
            canonical_parent_number
                .try_into()
                .expect("test harness parent block number must fit in PUSH1"),
        ),
    );

    let call_result: Bytes = client
        .request(
            "eth_baseCallAtFlashblock",
            (
                snapshot_id.clone(),
                block_env_request(),
                Some(parent_block_number_guard.clone()),
                None::<Box<BlockOverrides>>,
            ),
        )
        .await?;

    assert!(
        call_result.is_empty(),
        "base call should not hit the canonical-parent block-number guard"
    );

    let error = client
        .request::<_, U256>(
            "eth_baseEstimateGasAtFlashblock",
            (snapshot_id.clone(), block_env_request(), Some(parent_block_number_guard)),
        )
        .await
        .expect_err("snapshot estimate should still use the canonical parent block env");

    let error = error.as_error_resp().expect("json-rpc error response expected");
    assert!(
        error.message.contains("revert"),
        "unexpected parent-env estimate error message: {}",
        error.message
    );

    let mut unknown_snapshot_id = snapshot_id;
    unknown_snapshot_id["nonce"] = json!("0xffff");
    let error = client
        .request::<_, U256>(
            "eth_baseEstimateGasAtFlashblock",
            (unknown_snapshot_id, estimate_request(), Some(estimate_overrides)),
        )
        .await
        .expect_err("unknown snapshot should fail");

    let error = error.as_error_resp().expect("json-rpc error response expected");
    assert_eq!(error.code, -32602);
    assert!(
        error.message.contains("unknown flashblock snapshot"),
        "unexpected error message: {}",
        error.message
    );

    Ok(())
}

#[tokio::test]
async fn test_base_estimate_gas_at_flashblock_hot_only() -> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let canonical_parent_number = setup
        .harness
        .provider()
        .get_block_by_number(BlockNumberOrTag::Latest)
        .await?
        .expect("latest block expected")
        .number();
    let snapshot_block_number = setup
        .create_first_payload()
        .base
        .as_ref()
        .expect("flashblock base payload expected")
        .block_number;

    let deltas = setup
        .collect_fast_deltas(vec![setup.create_first_payload(), setup.create_second_payload()])
        .await?;
    let snapshot_id = deltas[1].snapshot_id;

    let url = setup.harness.rpc_url();
    let client = RpcClient::new_http(url.parse()?);
    let estimate_guard_address = address!("0x1000000000000000000000000000000000000011");
    let estimate_overrides = code_override(
        estimate_guard_address,
        count1_snapshot_guard_runtime(setup.txn_details.counter_address),
    );
    let estimate_request = || {
        BaseTransactionRequest::default()
            .from(Account::Alice.address())
            .to(estimate_guard_address)
            .input(TransactionInput::new(bytes!("0x")))
    };

    let estimate: U256 = client
        .request(
            "eth_baseEstimateGasAtFlashblock",
            (snapshot_id, estimate_request(), Some(estimate_overrides.clone())),
        )
        .await?;

    assert!(estimate > U256::ZERO, "expected snapshot-pinned estimate to succeed");

    let block_env_guard_address = address!("0x1000000000000000000000000000000000000012");
    let block_env_request = || {
        BaseTransactionRequest::default()
            .from(Account::Alice.address())
            .to(block_env_guard_address)
            .input(TransactionInput::new(bytes!("0x")))
    };

    let snapshot_block_number_result: Bytes = client
        .request(
            "eth_baseCallAtFlashblock",
            (
                snapshot_id,
                block_env_request(),
                Some(code_override(block_env_guard_address, block_env_reader_runtime(0x43))),
                None::<Box<BlockOverrides>>,
            ),
        )
        .await?;

    assert_eq!(
        snapshot_block_number_result,
        u256_return_data(snapshot_block_number),
        "base call should execute against the synthetic flashblock header env"
    );

    let parent_block_number_guard = code_override(
        block_env_guard_address,
        block_number_revert_if_eq_runtime(
            canonical_parent_number
                .try_into()
                .expect("test harness parent block number must fit in PUSH1"),
        ),
    );

    let error = client
        .request::<_, U256>(
            "eth_baseEstimateGasAtFlashblock",
            (snapshot_id, block_env_request(), Some(parent_block_number_guard)),
        )
        .await
        .expect_err("snapshot estimate should still use the canonical parent block env");

    let error = error.as_error_resp().expect("json-rpc error response expected");
    assert!(
        error.message.contains("revert"),
        "unexpected parent-env estimate error message: {}",
        error.message
    );

    Ok(())
}

#[tokio::test]
async fn test_base_call_at_flashblock() -> Result<()> {
    let setup = TestSetup::new().await?;
    let ws_url = setup.harness.ws_url();
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    ws_stream
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newFastFlashblockLogs"]
            })
            .to_string()
            .into(),
        ))
        .await?;

    let response = ws_stream.next().await.unwrap()?;
    let sub: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    assert_eq!(sub["jsonrpc"], "2.0");
    assert_eq!(sub["id"], 1);

    setup.send_flashblock(setup.create_first_payload()).await?;
    let _first_notification = ws_stream.next().await.unwrap()?;

    setup.send_flashblock(setup.create_second_payload()).await?;
    let notification = ws_stream.next().await.unwrap()?;
    let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
    let snapshot_id = notif["params"]["result"]["snapshotId"].clone();
    assert_fast_flashblock_snapshot_id(&snapshot_id);

    let url = setup.harness.rpc_url();
    let client = RpcClient::new_http(url.parse()?);
    let result: Bytes = client
        .request(
            "eth_baseCallAtFlashblock",
            (
                snapshot_id.clone(),
                setup.count1(),
                None::<serde_json::Value>,
                None::<serde_json::Value>,
            ),
        )
        .await?;

    assert_eq!(
        result,
        bytes!("0x0000000000000000000000000000000000000000000000000000000000000002")
    );

    let block_env_reader_address = address!("0x1000000000000000000000000000000000000002");
    let block_env_request = || {
        BaseTransactionRequest::default()
            .from(Account::Alice.address())
            .to(block_env_reader_address)
            .input(TransactionInput::new(bytes!("0x")))
    };

    let snapshot_block_number: Bytes = client
        .request(
            "eth_baseCallAtFlashblock",
            (
                snapshot_id.clone(),
                block_env_request(),
                Some(code_override(block_env_reader_address, block_env_reader_runtime(0x43))),
                None::<Box<BlockOverrides>>,
            ),
        )
        .await?;

    assert_eq!(
        snapshot_block_number,
        u256_return_data(1),
        "snapshot header block number should be visible by default"
    );

    let overridden_block_number: Bytes = client
        .request(
            "eth_baseCallAtFlashblock",
            (
                snapshot_id.clone(),
                block_env_request(),
                Some(code_override(block_env_reader_address, block_env_reader_runtime(0x43))),
                Some(Box::new(BlockOverrides {
                    number: Some(U256::from(42)),
                    ..Default::default()
                })),
            ),
        )
        .await?;

    assert_eq!(
        overridden_block_number,
        u256_return_data(42),
        "user block overrides should take precedence over snapshot defaults"
    );

    let mut unknown_snapshot_id = snapshot_id;
    unknown_snapshot_id["nonce"] = json!("0xffff");
    let error = client
        .request::<_, Bytes>(
            "eth_baseCallAtFlashblock",
            (
                unknown_snapshot_id,
                setup.count1(),
                None::<serde_json::Value>,
                None::<serde_json::Value>,
            ),
        )
        .await
        .expect_err("unknown snapshot should fail");

    let error = error.as_error_resp().expect("json-rpc error response expected");
    assert_eq!(error.code, -32602);
    assert!(
        error.message.contains("unknown flashblock snapshot"),
        "unexpected error message: {}",
        error.message
    );

    Ok(())
}

#[tokio::test]
async fn test_base_call_at_flashblock_hot_only() -> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let deltas = setup
        .collect_fast_deltas(vec![setup.create_first_payload(), setup.create_second_payload()])
        .await?;
    let snapshot_id = deltas[1].snapshot_id;

    let url = setup.harness.rpc_url();
    let client = RpcClient::new_http(url.parse()?);
    let result: Bytes = client
        .request(
            "eth_baseCallAtFlashblock",
            (snapshot_id, setup.count1(), None::<serde_json::Value>, None::<serde_json::Value>),
        )
        .await?;

    assert_eq!(
        result,
        bytes!("0x0000000000000000000000000000000000000000000000000000000000000002")
    );

    let block_env_reader_address = address!("0x1000000000000000000000000000000000000013");
    let block_env_request = || {
        BaseTransactionRequest::default()
            .from(Account::Alice.address())
            .to(block_env_reader_address)
            .input(TransactionInput::new(bytes!("0x")))
    };

    let snapshot_block_number: Bytes = client
        .request(
            "eth_baseCallAtFlashblock",
            (
                snapshot_id,
                block_env_request(),
                Some(code_override(block_env_reader_address, block_env_reader_runtime(0x43))),
                None::<Box<BlockOverrides>>,
            ),
        )
        .await?;

    assert_eq!(
        snapshot_block_number,
        u256_return_data(1),
        "snapshot header block number should be visible by default"
    );

    let overridden_block_number: Bytes = client
        .request(
            "eth_baseCallAtFlashblock",
            (
                snapshot_id,
                block_env_request(),
                Some(code_override(block_env_reader_address, block_env_reader_runtime(0x43))),
                Some(Box::new(BlockOverrides {
                    number: Some(U256::from(42)),
                    ..Default::default()
                })),
            ),
        )
        .await?;

    assert_eq!(
        overridden_block_number,
        u256_return_data(42),
        "user block overrides should take precedence over snapshot defaults"
    );

    Ok(())
}

#[tokio::test]
async fn base_dry_run_latest_flashblock_returns_missing_snapshot_before_fast_delta() -> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let response = setup
        .ws_rpc_request("eth_baseDryRunLatestFlashblock", json!([setup.count1_from_alice()]))
        .await?;

    assert_eq!(response["error"]["code"], json!(-32602));
    assert_eq!(response["error"]["message"], json!("no latest hot flashblock snapshot"));

    Ok(())
}

#[tokio::test]
async fn base_dry_run_at_flashblock_unknown_snapshot_returns_invalid_params() -> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let snapshot_id = FlashblockSnapshotId::new(
        0x99,
        0x88,
        0x77,
        PayloadId::new([0x55; 8]),
        B256::repeat_byte(0x44),
    );

    let response = setup
        .ws_rpc_request(
            "eth_baseDryRunAtFlashblock",
            json!([snapshot_id, setup.count1_from_alice()]),
        )
        .await?;

    assert_eq!(response["error"]["code"], json!(-32602));
    assert_eq!(response["error"]["message"], json!("unknown flashblock snapshot"));

    Ok(())
}

#[tokio::test]
async fn base_dry_run_latest_flashblock_returns_snapshot_id_and_gas_used_for_simple_success()
-> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let expected_snapshot_id =
        setup.send_test_payloads_and_wait_for_latest_hot_snapshot_id().await?;

    let response = setup
        .ws_rpc_request("eth_baseDryRunLatestFlashblock", json!([setup.count1_from_alice()]))
        .await?;
    let result: FlashblockDryRunResult = serde_json::from_value(response["result"].clone())?;

    assert!(result.success);
    assert_eq!(result.revert, None);
    assert_eq!(result.halt, None);
    assert_eq!(result.snapshot_id, expected_snapshot_id);
    assert!(result.gas_used > 0, "expected positive gas used, got {}", result.gas_used);

    Ok(())
}

#[tokio::test]
#[serial]
async fn dry_run_latest_sidecar_matches_direct_for_success() -> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let snapshot_id = setup.send_test_payloads_and_wait_for_latest_hot_snapshot_id().await?;
    let _ = setup.wait_for_latest_hot_dry_run_snapshot_id(snapshot_id).await?;
    let before_path_counts = latest_dry_run_path_counts();

    let latest = setup.latest_dry_run(setup.count1_from_alice()).await?;
    let after_path_counts = latest_dry_run_path_counts();
    let direct = setup.dry_run_at(snapshot_id, setup.count1_from_alice()).await?;

    assert_latest_dry_run_used_sidecar_hit(before_path_counts, after_path_counts);
    assert_eq!(latest, direct);
    assert!(latest.success);
    assert_eq!(latest.revert, None);
    assert_eq!(latest.halt, None);

    Ok(())
}

#[tokio::test]
#[serial]
async fn dry_run_latest_sidecar_matches_direct_for_revert() -> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let snapshot_id = setup.send_test_payloads_and_wait_for_latest_hot_snapshot_id().await?;
    let _ = setup.wait_for_latest_hot_dry_run_snapshot_id(snapshot_id).await?;
    let transaction = setup.block_number_guard_create_from_alice(1);
    let before_path_counts = latest_dry_run_path_counts();

    let latest = setup.latest_dry_run(transaction.clone()).await?;
    let after_path_counts = latest_dry_run_path_counts();
    let direct = setup.dry_run_at(snapshot_id, transaction).await?;

    assert_latest_dry_run_used_sidecar_hit(before_path_counts, after_path_counts);
    assert_eq!(latest, direct);
    assert!(!latest.success);
    assert_eq!(latest.revert, Some(bytes!("0x")));
    assert_eq!(latest.halt, None);

    Ok(())
}

#[tokio::test]
#[serial]
async fn dry_run_latest_sidecar_matches_direct_for_halt() -> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let snapshot_id = setup.send_test_payloads_and_wait_for_latest_hot_snapshot_id().await?;
    let _ = setup.wait_for_latest_hot_dry_run_snapshot_id(snapshot_id).await?;
    let transaction = BaseTransactionRequest::default()
        .from(Account::Alice.address())
        .gas_limit(25_000)
        .to(setup.txn_details.log_emitter_a_address);
    let before_path_counts = latest_dry_run_path_counts();

    let latest = setup.latest_dry_run(transaction.clone()).await?;
    let after_path_counts = latest_dry_run_path_counts();
    let direct = setup.dry_run_at(snapshot_id, transaction).await?;

    assert_latest_dry_run_used_sidecar_hit(before_path_counts, after_path_counts);
    assert_eq!(latest, direct);
    assert!(!latest.success);
    assert_eq!(latest.revert, None);
    assert!(latest.halt.is_some(), "expected a halt result");

    Ok(())
}

#[tokio::test]
#[serial]
async fn dry_run_latest_sidecar_stale_snapshot_id_falls_back_to_direct_latest_path() -> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let mut first_payload = setup.create_first_payload();
    first_payload.diff.block_hash = B256::with_last_byte(0x01);
    setup.send_flashblock(first_payload).await?;
    let initial_snapshot_id = setup.wait_for_latest_hot_snapshot_id(1, 0).await?;
    let _ = setup.wait_for_latest_hot_dry_run_snapshot_id(initial_snapshot_id).await?;

    let mut ws_stream = setup.subscribe_fast_flashblock_logs().await?;
    setup.harness.flashblocks_state().hold_hot_dry_run_sidecar_worker_for_testing();
    let mut second_payload = setup.create_second_payload();
    second_payload.diff.block_hash = B256::with_last_byte(0x02);
    let fast_delta =
        setup.send_flashblock_and_collect_fast_delta(&mut ws_stream, second_payload).await?;
    let expected_snapshot_id = fast_delta.snapshot_id;
    let transaction = setup.count1_from_alice();
    let before_path_counts = latest_dry_run_path_counts();
    let latest = setup.latest_dry_run(transaction.clone()).await?;
    let after_path_counts = latest_dry_run_path_counts();
    setup.harness.flashblocks_state().release_hot_dry_run_sidecar_worker_for_testing();
    let direct = setup.dry_run_at(expected_snapshot_id, transaction).await?;

    assert_latest_dry_run_used_direct_fallback(before_path_counts, after_path_counts);
    assert!(
        after_path_counts.2 > before_path_counts.2,
        "expected stale sidecar mismatch accounting on direct fallback"
    );
    assert_eq!(latest.snapshot_id, expected_snapshot_id);
    assert_eq!(latest, direct);

    Ok(())
}

#[tokio::test]
#[serial]
async fn dry_run_latest_sidecar_missing_latest_falls_back_to_direct_latest_path() -> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let initial_snapshot_id =
        setup.send_test_payloads_and_wait_for_latest_hot_snapshot_id().await?;
    let _ = setup.wait_for_latest_hot_dry_run_snapshot_id(initial_snapshot_id).await?;

    setup
        .harness
        .flashblocks_state()
        .force_next_hot_dry_run_sidecar_after_send_failure_for_testing();

    let next_block_parent_hash = setup.pending_parent_hash_for_next_block().await?;
    let mut third_payload = setup.create_third_payload(next_block_parent_hash);
    third_payload.diff.block_hash = B256::with_last_byte(0x03);
    setup.send_flashblock(third_payload).await?;
    let expected_snapshot_id = setup.wait_for_latest_hot_snapshot_id(2, 0).await?;
    setup.wait_for_latest_hot_dry_run_state_clear().await?;
    let before_path_counts = latest_dry_run_path_counts();

    let latest = setup.latest_dry_run(setup.count1_from_alice()).await?;
    let after_path_counts = latest_dry_run_path_counts();
    let direct = setup.dry_run_at(expected_snapshot_id, setup.count1_from_alice()).await?;

    assert_latest_dry_run_used_direct_fallback(before_path_counts, after_path_counts);
    assert_eq!(after_path_counts.2 - before_path_counts.2, 0);
    assert_eq!(latest.snapshot_id, expected_snapshot_id);
    assert_eq!(latest, direct);

    Ok(())
}

#[tokio::test]
#[serial]
async fn dry_run_latest_sidecar_reset_clears_warm_state_and_falls_back_until_rebuilt() -> Result<()>
{
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let initial_snapshot_id =
        setup.send_test_payloads_and_wait_for_latest_hot_snapshot_id().await?;
    let _ = setup.wait_for_latest_hot_dry_run_snapshot_id(initial_snapshot_id).await?;

    setup.send_flashblock(setup.create_invalidating_gap_payload()).await?;
    setup.wait_for_latest_hot_snapshot_clear().await?;
    setup.wait_for_latest_hot_dry_run_state_clear().await?;

    setup.harness.flashblocks_state().hold_hot_dry_run_sidecar_worker_for_testing();
    let rebuilt_snapshot_id =
        setup.send_test_payloads_and_wait_for_latest_hot_snapshot_id().await?;
    let before_fallback_counts = latest_dry_run_path_counts();
    let latest_during_rebuild = setup.latest_dry_run(setup.count1_from_alice()).await?;
    let after_fallback_counts = latest_dry_run_path_counts();
    setup.harness.flashblocks_state().release_hot_dry_run_sidecar_worker_for_testing();
    let direct = setup.dry_run_at(rebuilt_snapshot_id, setup.count1_from_alice()).await?;

    assert_latest_dry_run_used_direct_fallback(before_fallback_counts, after_fallback_counts);
    assert_eq!(latest_during_rebuild.snapshot_id, rebuilt_snapshot_id);
    assert_eq!(latest_during_rebuild, direct);

    let _ = setup.wait_for_latest_hot_dry_run_snapshot_id(rebuilt_snapshot_id).await?;
    let before_sidecar_counts = latest_dry_run_path_counts();
    let latest_after_rebuild = setup.latest_dry_run(setup.count1_from_alice()).await?;
    let after_sidecar_counts = latest_dry_run_path_counts();

    assert_latest_dry_run_used_sidecar_hit(before_sidecar_counts, after_sidecar_counts);
    assert_eq!(latest_after_rebuild, direct);

    Ok(())
}

#[tokio::test]
async fn sidecar_overflow_does_not_block_fast_logs() -> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let mut first_payload = setup.create_first_payload();
    first_payload.diff.block_hash = B256::with_last_byte(0x01);
    setup.send_flashblock(first_payload).await?;
    let _ = setup.wait_for_latest_hot_snapshot_id(1, 0).await?;

    let mut ws_stream = setup.subscribe_fast_flashblock_logs().await?;
    setup
        .harness
        .flashblocks_state()
        .force_next_hot_dry_run_sidecar_after_send_failure_for_testing();
    let mut second_payload = setup.create_second_payload();
    second_payload.diff.block_hash = B256::with_last_byte(0x02);

    let fast_delta =
        setup.send_flashblock_and_collect_fast_delta(&mut ws_stream, second_payload).await?;

    let response = setup
        .ws_rpc_request("eth_baseDryRunLatestFlashblock", json!([setup.count1_from_alice()]))
        .await?;
    let result: FlashblockDryRunResult = serde_json::from_value(response["result"].clone())?;

    assert!(result.success);
    assert_eq!(result.revert, None);
    assert_eq!(result.halt, None);
    assert_eq!(result.snapshot_id, fast_delta.snapshot_id);
    assert!(result.gas_used > 0, "expected positive gas used, got {}", result.gas_used);

    Ok(())
}

#[tokio::test]
async fn sidecar_publishes_matching_latest_warm_state() -> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let expected_snapshot_id =
        setup.send_test_payloads_and_wait_for_latest_hot_snapshot_id().await?;
    let flashblocks_state = setup.harness.flashblocks_state();

    let main_latest =
        flashblocks_state.get_latest_hot_snapshot().expect("latest hot snapshot should exist");
    assert_eq!(main_latest.snapshot_id, expected_snapshot_id);

    let sidecar_snapshot_id =
        setup.wait_for_latest_hot_dry_run_snapshot_id(expected_snapshot_id).await?;
    assert_eq!(sidecar_snapshot_id, main_latest.snapshot_id);

    setup.send_flashblock(setup.create_invalidating_gap_payload()).await?;
    setup.wait_for_latest_hot_snapshot_clear().await?;
    setup.wait_for_latest_hot_dry_run_state_clear().await?;

    Ok(())
}

#[tokio::test]
async fn sidecar_recovers_from_drop_using_authoritative_rebuild() -> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let initial_snapshot_id =
        setup.send_test_payloads_and_wait_for_latest_hot_snapshot_id().await?;
    let _ = setup.wait_for_latest_hot_dry_run_snapshot_id(initial_snapshot_id).await?;

    setup
        .harness
        .flashblocks_state()
        .force_next_hot_dry_run_sidecar_after_send_failure_for_testing();

    let next_block_parent_hash = setup.pending_parent_hash_for_next_block().await?;
    let mut third_payload = setup.create_third_payload(next_block_parent_hash);
    third_payload.diff.block_hash = B256::with_last_byte(0x03);
    setup.send_flashblock(third_payload).await?;
    let dropped_snapshot_id = setup.wait_for_latest_hot_snapshot_id(2, 0).await?;
    setup.wait_for_latest_hot_dry_run_state_clear().await?;

    setup.send_flashblock(setup.create_fourth_payload()).await?;
    let rebuilt_snapshot_id = setup.wait_for_latest_hot_snapshot_id(2, 1).await?;
    assert_ne!(rebuilt_snapshot_id, dropped_snapshot_id);

    let sidecar_snapshot_id =
        setup.wait_for_latest_hot_dry_run_snapshot_id(rebuilt_snapshot_id).await?;
    assert_eq!(sidecar_snapshot_id, rebuilt_snapshot_id);

    let response = setup
        .ws_rpc_request("eth_baseDryRunLatestFlashblock", json!([setup.count1_from_alice()]))
        .await?;
    let result: FlashblockDryRunResult = serde_json::from_value(response["result"].clone())?;
    assert_eq!(result.snapshot_id, rebuilt_snapshot_id);

    Ok(())
}

#[tokio::test]
async fn sidecar_recovers_after_reset_with_authoritative_snapshot_nonce() -> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let initial_snapshot_id =
        setup.send_test_payloads_and_wait_for_latest_hot_snapshot_id().await?;
    let _ = setup.wait_for_latest_hot_dry_run_snapshot_id(initial_snapshot_id).await?;

    setup.send_flashblock(setup.create_invalidating_gap_payload()).await?;
    setup.wait_for_latest_hot_snapshot_clear().await?;
    setup.wait_for_latest_hot_dry_run_state_clear().await?;

    let rebuilt_snapshot_id =
        setup.send_test_payloads_and_wait_for_latest_hot_snapshot_id().await?;
    assert!(
        rebuilt_snapshot_id.nonce() > initial_snapshot_id.nonce(),
        "expected reset recovery snapshot nonce to advance beyond the initial warm state"
    );
    assert!(
        rebuilt_snapshot_id.nonce() > 2,
        "expected authoritative snapshot nonce to exceed the fresh rebuild default"
    );

    let sidecar_snapshot_id =
        setup.wait_for_latest_hot_dry_run_snapshot_id(rebuilt_snapshot_id).await?;
    assert_eq!(sidecar_snapshot_id, rebuilt_snapshot_id);

    let response = setup
        .ws_rpc_request("eth_baseDryRunLatestFlashblock", json!([setup.count1_from_alice()]))
        .await?;
    let result: FlashblockDryRunResult = serde_json::from_value(response["result"].clone())?;
    assert_eq!(result.snapshot_id, rebuilt_snapshot_id);

    Ok(())
}

#[tokio::test]
async fn base_dry_run_latest_flashblock_returns_revert_bytes_for_simple_revert() -> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let expected_snapshot_id =
        setup.send_test_payloads_and_wait_for_latest_hot_snapshot_id().await?;

    let response = setup
        .ws_rpc_request(
            "eth_baseDryRunLatestFlashblock",
            json!([setup.block_number_guard_create_from_alice(1)]),
        )
        .await?;
    let result: FlashblockDryRunResult = serde_json::from_value(response["result"].clone())?;

    assert!(!result.success);
    assert_eq!(result.revert, Some(bytes!("0x")));
    assert_eq!(result.halt, None);
    assert_eq!(result.snapshot_id, expected_snapshot_id);
    assert!(result.gas_used > 0, "expected positive gas used, got {}", result.gas_used);

    Ok(())
}

#[tokio::test]
#[serial]
async fn sidecar_hit_dry_runs_do_not_mutate_warm_state() -> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let snapshot_id = setup.send_test_payloads_and_wait_for_latest_hot_snapshot_id().await?;
    let _ = setup.wait_for_latest_hot_dry_run_snapshot_id(snapshot_id).await?;
    let expected = setup.dry_run_at(snapshot_id, setup.count1_from_alice()).await?;
    let sequential_calls = 3u64;
    let concurrent_calls = 8u64;
    let before_path_counts = latest_dry_run_path_counts();

    for _ in 0..sequential_calls {
        let latest = setup.latest_dry_run(setup.count1_from_alice()).await?;
        assert_eq!(latest, expected);
    }

    let concurrent_results =
        join_all((0..concurrent_calls).map(|_| setup.latest_dry_run(setup.count1_from_alice())))
            .await;
    for result in concurrent_results {
        assert_eq!(result?, expected);
    }

    let after_path_counts = latest_dry_run_path_counts();
    assert_eq!(after_path_counts.0 - before_path_counts.0, sequential_calls + concurrent_calls);
    assert_eq!(after_path_counts.1 - before_path_counts.1, 0);

    let direct_after = setup.dry_run_at(snapshot_id, setup.count1_from_alice()).await?;
    assert_eq!(direct_after, expected);

    Ok(())
}

#[tokio::test]
async fn base_dry_run_latest_flashblock_matches_base_call_at_flashblock_for_success_and_revert_parity()
-> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let snapshot_id = setup.send_test_payloads_and_wait_for_latest_hot_snapshot_id().await?;

    let dry_run_success_response = setup
        .ws_rpc_request(
            "eth_baseDryRunAtFlashblock",
            json!([snapshot_id, setup.count1_from_alice()]),
        )
        .await?;
    let dry_run_success: FlashblockDryRunResult =
        serde_json::from_value(dry_run_success_response["result"].clone())?;
    let base_call_success_response = setup
        .ws_rpc_request(
            "eth_baseCallAtFlashblock",
            json!([snapshot_id, setup.count1_from_alice(), null, null]),
        )
        .await?;
    let base_call_success: Bytes =
        serde_json::from_value(base_call_success_response["result"].clone())?;

    assert!(dry_run_success.success);
    assert_eq!(dry_run_success.revert, None);
    assert_eq!(base_call_success, u256_return_data(2));

    let dry_run_revert_response = setup
        .ws_rpc_request(
            "eth_baseDryRunAtFlashblock",
            json!([snapshot_id, setup.block_number_guard_create_from_alice(1)]),
        )
        .await?;
    let dry_run_revert: FlashblockDryRunResult =
        serde_json::from_value(dry_run_revert_response["result"].clone())?;

    assert!(!dry_run_revert.success);
    assert_eq!(dry_run_revert.revert, Some(bytes!("0x")));

    let base_call_revert_response = setup
        .ws_rpc_request(
            "eth_baseCallAtFlashblock",
            json!([snapshot_id, setup.block_number_guard_create_from_alice(1), null, null]),
        )
        .await?;
    let error_message = base_call_revert_response["error"]["message"]
        .as_str()
        .expect("json-rpc error response expected");
    assert!(error_message.contains("revert"), "unexpected revert error: {}", error_message);

    Ok(())
}

#[tokio::test]
async fn base_dry_run_latest_flashblock_reports_positive_gas_used_for_success_and_revert()
-> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let _snapshot_id = setup.send_test_payloads_and_wait_for_latest_hot_snapshot_id().await?;

    let success_response = setup
        .ws_rpc_request("eth_baseDryRunLatestFlashblock", json!([setup.count1_from_alice()]))
        .await?;
    let success: FlashblockDryRunResult =
        serde_json::from_value(success_response["result"].clone())?;

    let revert_response = setup
        .ws_rpc_request(
            "eth_baseDryRunLatestFlashblock",
            json!([setup.block_number_guard_create_from_alice(1)]),
        )
        .await?;
    let revert: FlashblockDryRunResult = serde_json::from_value(revert_response["result"].clone())?;

    assert!(success.gas_used > 0, "expected positive success gas used, got {}", success.gas_used);
    assert!(revert.gas_used > 0, "expected positive revert gas used, got {}", revert.gas_used);

    Ok(())
}

#[tokio::test]
async fn test_pinned_flashblock_rpc_accepts_multi_block_pending_snapshot() -> Result<()> {
    let setup = TestSetup::new().await?;
    let provider = setup.harness.provider();
    let canonical_parent_number = provider
        .get_block_by_number(BlockNumberOrTag::Latest)
        .await?
        .expect("latest block expected")
        .number();
    let ws_url = setup.harness.ws_url();
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    ws_stream
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newFastFlashblockLogs"]
            })
            .to_string()
            .into(),
        ))
        .await?;

    let response = ws_stream.next().await.unwrap()?;
    let sub: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    assert_eq!(sub["jsonrpc"], "2.0");
    assert_eq!(sub["id"], 1);

    setup.send_flashblock(setup.create_first_payload()).await?;
    let _first_notification = ws_stream.next().await.unwrap()?;

    setup.send_flashblock(setup.create_second_payload()).await?;
    let _second_notification = ws_stream.next().await.unwrap()?;

    let first_pending_block = provider
        .get_block_by_number(BlockNumberOrTag::Pending)
        .await?
        .expect("pending block expected");
    let first_pending_hash = first_pending_block.hash();
    assert_ne!(first_pending_hash, setup.canonical_parent_hash);

    setup.send_flashblock(setup.create_third_payload(first_pending_hash)).await?;
    let notification = ws_stream.next().await.unwrap()?;
    let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
    let snapshot_id = notif["params"]["result"]["snapshotId"].clone();
    assert_fast_flashblock_snapshot_id(&snapshot_id);
    assert_eq!(snapshot_id["blockNumber"], "0x2");
    assert_eq!(snapshot_id["flashblockIndex"], "0x0");
    assert_eq!(snapshot_id["parentHash"], json!(first_pending_hash));
    assert_ne!(snapshot_id["parentHash"], json!(setup.canonical_parent_hash));

    let url = setup.harness.rpc_url();
    let client = RpcClient::new_http(url.parse()?);
    let estimate_guard_address = address!("0x1000000000000000000000000000000000000004");
    let estimate_request = || {
        BaseTransactionRequest::default()
            .from(Account::Alice.address())
            .to(estimate_guard_address)
            .input(TransactionInput::new(bytes!("0x")))
    };

    let estimate: U256 = client
        .request(
            "eth_baseEstimateGasAtFlashblock",
            (
                snapshot_id.clone(),
                estimate_request(),
                Some(code_override(
                    estimate_guard_address,
                    count1_snapshot_guard_runtime(setup.txn_details.counter_address),
                )),
            ),
        )
        .await?;

    assert!(estimate > U256::ZERO, "expected multi-block snapshot estimate to succeed");

    let block_env_guard_address = address!("0x1000000000000000000000000000000000000005");
    let block_env_request = || {
        BaseTransactionRequest::default()
            .from(Account::Alice.address())
            .to(block_env_guard_address)
            .input(TransactionInput::new(bytes!("0x")))
    };

    let snapshot_block_number: Bytes = client
        .request(
            "eth_baseCallAtFlashblock",
            (
                snapshot_id.clone(),
                block_env_request(),
                Some(code_override(block_env_guard_address, block_env_reader_runtime(0x43))),
                None::<Box<BlockOverrides>>,
            ),
        )
        .await?;

    assert_eq!(
        snapshot_block_number,
        u256_return_data(2),
        "base call should see the latest pending block header for a multi-block snapshot"
    );

    let estimate_error = client
        .request::<_, U256>(
            "eth_baseEstimateGasAtFlashblock",
            (
                snapshot_id,
                block_env_request(),
                Some(code_override(
                    block_env_guard_address,
                    block_number_revert_if_eq_runtime(
                        canonical_parent_number
                            .try_into()
                            .expect("test harness parent block number must fit in PUSH1"),
                    ),
                )),
            ),
        )
        .await
        .expect_err("snapshot estimate should still use the canonical parent block env");

    let estimate_error = estimate_error.as_error_resp().expect("json-rpc error response expected");
    assert!(
        estimate_error.message.contains("revert"),
        "unexpected parent-env estimate error message: {}",
        estimate_error.message
    );

    Ok(())
}

#[tokio::test]
async fn test_eth_simulate_v1() -> Result<()> {
    let setup = TestSetup::new().await?;
    let provider = setup.harness.provider();
    setup.send_test_payloads().await?;

    let simulate_call = SimulatePayload {
        block_state_calls: vec![SimBlock {
            calls: vec![
                // read count1() from counter contract
                setup.count1().gas_limit(100_000).into(),
                // increment() value in contract
                BaseTransactionRequest::default()
                    .from(address!("0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"))
                    .transaction_type(0)
                    .gas_limit(200000)
                    .to(setup.txn_details.counter_address)
                    .input(TransactionInput::new(bytes!("0xd09de08a")))
                    .into(),
                // read count1() from counter contract
                setup.count1().gas_limit(100_000).into(),
            ],
            block_overrides: None,
            state_overrides: None,
        }],
        trace_transfers: false,
        validation: true,
        return_full_transactions: true,
    };
    let simulate_res =
        provider.simulate(&simulate_call).block_id(BlockNumberOrTag::Pending.into()).await;
    assert!(simulate_res.is_ok());
    let block = simulate_res.unwrap();
    assert_eq!(block.len(), 1);
    assert_eq!(block[0].calls.len(), 3);
    assert_eq!(
        block[0].calls[0].return_data,
        bytes!("0x0000000000000000000000000000000000000000000000000000000000000002")
    );
    assert_eq!(block[0].calls[1].return_data, bytes!("0x"));
    assert_eq!(
        block[0].calls[2].return_data,
        bytes!("0x0000000000000000000000000000000000000000000000000000000000000003")
    );

    Ok(())
}

#[tokio::test]
async fn test_send_raw_transaction_sync() -> Result<()> {
    let setup = TestSetup::new().await?;

    setup.send_flashblock(setup.create_first_payload()).await?;

    // run the Tx sync and, in parallel, deliver the payload that contains the Tx
    let second_payload = setup.create_second_payload();
    let (receipt_result, payload_result) = tokio::join!(
        setup.send_raw_transaction_sync(setup.txn_details.alice_eth_transfer_tx.clone(), None),
        async {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            setup.send_flashblock(second_payload).await
        }
    );

    payload_result?;
    let receipt = receipt_result?;

    assert_eq!(receipt.transaction_hash(), setup.txn_details.alice_eth_transfer_hash);
    Ok(())
}

#[tokio::test]
async fn test_send_raw_transaction_sync_timeout() {
    let setup = TestSetup::new().await.unwrap();

    // fail request immediately by passing a timeout of 0 ms
    let receipt_result = setup
        .send_raw_transaction_sync(setup.txn_details.alice_eth_transfer_tx.clone(), Some(0))
        .await;

    let error_code = EthRpcErrorCode::TransactionConfirmationTimeout.code();
    assert!(receipt_result.err().unwrap().to_string().contains(format!("{error_code}").as_str()));
}

#[tokio::test]
async fn test_get_logs_pending() -> Result<()> {
    let setup = TestSetup::new().await?;
    let provider = setup.harness.provider();

    // Test no logs when no flashblocks sent
    let logs = provider
        .get_logs(
            &alloy_rpc_types_eth::Filter::default().select(alloy_eips::BlockNumberOrTag::Pending),
        )
        .await?;
    assert_eq!(logs.len(), 0);

    // Send payloads with transactions
    setup.send_test_payloads().await?;

    // Test getting pending logs - must use both fromBlock and toBlock as "pending"
    let logs = provider
        .get_logs(
            &alloy_rpc_types_eth::Filter::default()
                .from_block(alloy_eips::BlockNumberOrTag::Pending)
                .to_block(alloy_eips::BlockNumberOrTag::Pending),
        )
        .await?;

    // We should now have 2 logs from the log_trigger_tx transaction
    assert_eq!(logs.len(), 2);

    // Verify the first log is from LogEmitterA
    assert_eq!(logs[0].address(), setup.txn_details.log_emitter_a_address);
    assert_eq!(logs[0].topics()[0], TEST_LOG_TOPIC_0);
    assert_eq!(logs[0].transaction_hash, Some(setup.txn_details.log_trigger_hash));

    // Verify the second log is from LogEmitterB
    assert_eq!(logs[1].address(), setup.txn_details.log_emitter_b_address);
    assert_eq!(logs[1].topics()[0], TEST_LOG_TOPIC_0);
    assert_eq!(logs[1].transaction_hash, Some(setup.txn_details.log_trigger_hash));

    Ok(())
}

#[tokio::test]
async fn test_get_logs_filter_by_address() -> Result<()> {
    let setup = TestSetup::new().await?;
    let provider = setup.harness.provider();

    setup.send_test_payloads().await?;

    // Test filtering by LogEmitterA address
    let logs = provider
        .get_logs(
            &alloy_rpc_types_eth::Filter::default()
                .address(setup.txn_details.log_emitter_a_address)
                .from_block(alloy_eips::BlockNumberOrTag::Pending)
                .to_block(alloy_eips::BlockNumberOrTag::Pending),
        )
        .await?;

    // Should get only 1 log from LogEmitterA
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].address(), setup.txn_details.log_emitter_a_address);
    assert_eq!(logs[0].transaction_hash, Some(setup.txn_details.log_trigger_hash));

    // Test filtering by LogEmitterB address
    let logs = provider
        .get_logs(
            &alloy_rpc_types_eth::Filter::default()
                .address(setup.txn_details.log_emitter_b_address)
                .from_block(alloy_eips::BlockNumberOrTag::Pending)
                .to_block(alloy_eips::BlockNumberOrTag::Pending),
        )
        .await?;

    // Should get only 1 log from LogEmitterB
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].address(), setup.txn_details.log_emitter_b_address);
    assert_eq!(logs[0].transaction_hash, Some(setup.txn_details.log_trigger_hash));

    Ok(())
}

#[tokio::test]
async fn test_get_logs_topic_filtering() -> Result<()> {
    let setup = TestSetup::new().await?;
    let provider = setup.harness.provider();

    setup.send_test_payloads().await?;

    // Test filtering by topic - should match both logs
    let logs = provider
        .get_logs(
            &alloy_rpc_types_eth::Filter::default()
                .event_signature(TEST_LOG_TOPIC_0)
                .from_block(alloy_eips::BlockNumberOrTag::Pending)
                .to_block(alloy_eips::BlockNumberOrTag::Pending),
        )
        .await?;

    assert_eq!(logs.len(), 2);
    assert!(logs.iter().all(|log| log.topics()[0] == TEST_LOG_TOPIC_0));

    // Test filtering by specific topic combination - should match only LogEmitterA (has 2 topics)
    let filter = alloy_rpc_types_eth::Filter::default()
        .topic1(TEST_LOG_TOPIC_1)
        .from_block(alloy_eips::BlockNumberOrTag::Pending)
        .to_block(alloy_eips::BlockNumberOrTag::Pending);

    let logs = provider.get_logs(&filter).await?;

    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].address(), setup.txn_details.log_emitter_a_address);
    assert_eq!(logs[0].topics()[1], TEST_LOG_TOPIC_1);

    Ok(())
}

#[tokio::test]
async fn test_get_logs_mixed_block_ranges() -> Result<()> {
    let setup = TestSetup::new().await?;
    let provider = setup.harness.provider();

    setup.send_test_payloads().await?;

    // Test fromBlock: 0, toBlock: pending (should include both historical and pending)
    let logs = provider
        .get_logs(
            &alloy_rpc_types_eth::Filter::default()
                .from_block(0)
                .to_block(alloy_eips::BlockNumberOrTag::Pending),
        )
        .await?;

    // Should now include pending logs (2 logs from our test setup)
    assert_eq!(logs.len(), 2);
    assert!(
        logs.iter().all(|log| log.transaction_hash == Some(setup.txn_details.log_trigger_hash))
    );

    // Test fromBlock: latest, toBlock: pending
    let logs = provider
        .get_logs(
            &alloy_rpc_types_eth::Filter::default()
                .from_block(alloy_eips::BlockNumberOrTag::Latest)
                .to_block(alloy_eips::BlockNumberOrTag::Pending),
        )
        .await?;

    // Should include pending logs (historical part is empty in our test setup)
    assert_eq!(logs.len(), 2);
    assert!(
        logs.iter().all(|log| log.transaction_hash == Some(setup.txn_details.log_trigger_hash))
    );

    // Test fromBlock: earliest, toBlock: pending
    let logs = provider
        .get_logs(
            &alloy_rpc_types_eth::Filter::default()
                .from_block(alloy_eips::BlockNumberOrTag::Earliest)
                .to_block(alloy_eips::BlockNumberOrTag::Pending),
        )
        .await?;

    // Should include pending logs (historical part is empty in our test setup)
    assert_eq!(logs.len(), 2);
    assert!(
        logs.iter().all(|log| log.transaction_hash == Some(setup.txn_details.log_trigger_hash))
    );

    Ok(())
}

// eth_ subscription methods for flashblocks
#[tokio::test]
async fn test_eth_subscribe_new_flashblocks() -> eyre::Result<()> {
    let setup = TestSetup::new().await?;
    let _provider = setup.harness.provider();
    let ws_url = setup.harness.ws_url();
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    ws_stream
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newFlashblocks"]
            })
            .to_string()
            .into(),
        ))
        .await?;

    let response = ws_stream.next().await.unwrap()?;
    let sub: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    assert_eq!(sub["jsonrpc"], "2.0");
    assert_eq!(sub["id"], 1);
    let subscription_id = sub["result"].as_str().expect("subscription id expected");

    setup.send_flashblock(setup.create_first_payload()).await?;

    let notification = ws_stream.next().await.unwrap()?;
    let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
    assert_eq!(notif["method"], "eth_subscription");
    assert_eq!(notif["params"]["subscription"], subscription_id);

    let block = &notif["params"]["result"];
    assert_eq!(block["number"], "0x1");
    assert!(block["hash"].is_string());
    assert!(block["parentHash"].is_string());
    assert!(block["transactions"].is_array());
    assert_eq!(block["transactions"].as_array().unwrap().len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_eth_subscribe_multiple_flashblocks() -> eyre::Result<()> {
    let setup = TestSetup::new().await?;
    let _provider = setup.harness.provider();
    let ws_url = setup.harness.ws_url();
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    ws_stream
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newFlashblocks"]
            })
            .to_string()
            .into(),
        ))
        .await?;

    let response = ws_stream.next().await.unwrap()?;
    let sub: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    let subscription_id = sub["result"].as_str().expect("subscription id expected");

    setup.send_flashblock(setup.create_first_payload()).await?;

    let notif1 = ws_stream.next().await.unwrap()?;
    let notif1: serde_json::Value = serde_json::from_str(notif1.to_text()?)?;
    assert_eq!(notif1["params"]["subscription"], subscription_id);

    let block1 = &notif1["params"]["result"];
    assert_eq!(block1["number"], "0x1");
    assert_eq!(block1["transactions"].as_array().unwrap().len(), 1);

    setup.send_flashblock(setup.create_second_payload()).await?;

    let notif2 = ws_stream.next().await.unwrap()?;
    let notif2: serde_json::Value = serde_json::from_str(notif2.to_text()?)?;
    assert_eq!(notif2["params"]["subscription"], subscription_id);

    let block2 = &notif2["params"]["result"];
    assert_eq!(block1["number"], block2["number"]); // Same block, incremental updates
    assert_eq!(block2["transactions"].as_array().unwrap().len(), 10); // 1 from first + 9 from second

    Ok(())
}

#[tokio::test]
async fn test_eth_unsubscribe() -> eyre::Result<()> {
    let setup = TestSetup::new().await?;
    let _provider = setup.harness.provider();
    let ws_url = setup.harness.ws_url();
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    ws_stream
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newFlashblocks"]
            })
            .to_string()
            .into(),
        ))
        .await?;

    let response = ws_stream.next().await.unwrap()?;
    let sub: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    let subscription_id = sub["result"].as_str().expect("subscription id expected");

    ws_stream
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "eth_unsubscribe",
                "params": [subscription_id]
            })
            .to_string()
            .into(),
        ))
        .await?;

    let unsub = ws_stream.next().await.unwrap()?;
    let unsub: serde_json::Value = serde_json::from_str(unsub.to_text()?)?;
    assert_eq!(unsub["jsonrpc"], "2.0");
    assert_eq!(unsub["id"], 2);
    assert_eq!(unsub["result"], true);

    Ok(())
}

#[tokio::test]
async fn test_eth_subscribe_multiple_clients() -> eyre::Result<()> {
    let setup = TestSetup::new().await?;
    let _provider = setup.harness.provider();
    let ws_url = setup.harness.ws_url();
    let (mut ws1, _) = connect_async(&ws_url).await?;
    let (mut ws2, _) = connect_async(&ws_url).await?;

    let req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": ["newFlashblocks"]
    });
    ws1.send(Message::Text(req.to_string().into())).await?;
    ws2.send(Message::Text(req.to_string().into())).await?;

    let _sub1 = ws1.next().await.unwrap()?;
    let _sub2 = ws2.next().await.unwrap()?;

    setup.send_flashblock(setup.create_first_payload()).await?;

    let notif1 = ws1.next().await.unwrap()?;
    let notif1: serde_json::Value = serde_json::from_str(notif1.to_text()?)?;
    let notif2 = ws2.next().await.unwrap()?;
    let notif2: serde_json::Value = serde_json::from_str(notif2.to_text()?)?;

    assert_eq!(notif1["method"], "eth_subscription");
    assert_eq!(notif2["method"], "eth_subscription");

    let block1 = &notif1["params"]["result"];
    let block2 = &notif2["params"]["result"];
    assert_eq!(block1["number"], "0x1");
    assert_eq!(block1["number"], block2["number"]);
    assert_eq!(block1["hash"], block2["hash"]);

    Ok(())
}

/// Test that standard subscription types (newHeads) work correctly.
/// This verifies that our `ExtendedSubscriptionKind` properly proxies to reth's implementation.
#[tokio::test]
async fn test_eth_subscribe_new_heads() -> eyre::Result<()> {
    let setup = TestSetup::new().await?;
    let _provider = setup.harness.provider();
    let ws_url = setup.harness.ws_url();
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    // Subscribe to newHeads - this should be proxied to reth's standard implementation
    ws_stream
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newHeads"]
            })
            .to_string()
            .into(),
        ))
        .await?;

    let response = ws_stream.next().await.unwrap()?;
    let sub: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    assert_eq!(sub["jsonrpc"], "2.0");
    assert_eq!(sub["id"], 1);
    // Should return a subscription ID, confirming the subscription was accepted
    assert!(sub["result"].is_string(), "Expected subscription ID, got: {sub:?}");

    Ok(())
}

fn assert_hex_string(value: &serde_json::Value, field_name: &str) {
    let string = value
        .as_str()
        .unwrap_or_else(|| panic!("expected {field_name} hex string, got: {value:?}"));
    assert!(string.starts_with("0x"), "expected {field_name} to start with 0x, got: {string}");
}

fn assert_flashblock_logs_batch_log(log: &serde_json::Value) {
    assert_hex_string(&log["txHash"], "txHash");
    assert_hex_string(&log["txIndex"], "txIndex");
    assert_hex_string(&log["logIndexInTx"], "logIndexInTx");
    assert_hex_string(&log["logIndexInBlock"], "logIndexInBlock");
    assert!(log["address"].is_string(), "expected address string, got: {log:?}");
    assert!(log["topics"].is_array(), "expected topics array, got: {log:?}");
    assert_hex_string(&log["data"], "data");
    assert!(log["removed"].is_boolean(), "expected removed bool, got: {log:?}");
}

fn assert_flashblock_logs_batch_transaction(tx: &serde_json::Value) {
    assert_hex_string(&tx["hash"], "hash");
    assert_hex_string(&tx["index"], "index");
    assert_hex_string(&tx["status"], "status");
}

fn assert_fast_flashblock_snapshot_id(snapshot_id: &serde_json::Value) {
    assert_hex_string(&snapshot_id["nonce"], "snapshotId.nonce");
    assert_hex_string(&snapshot_id["blockNumber"], "snapshotId.blockNumber");
    assert_hex_string(&snapshot_id["flashblockIndex"], "snapshotId.flashblockIndex");
    assert_hex_string(&snapshot_id["payloadId"], "snapshotId.payloadId");
    assert_hex_string(&snapshot_id["parentHash"], "snapshotId.parentHash");
}

fn assert_fast_flashblock_log(log: &serde_json::Value) {
    assert_hex_string(&log["txHash"], "txHash");
    assert_hex_string(&log["txIndex"], "txIndex");
    assert_hex_string(&log["logIndexInTx"], "logIndexInTx");
    assert_hex_string(&log["logIndexInBlock"], "logIndexInBlock");
    assert!(log["address"].is_string(), "expected address string, got: {log:?}");
    assert!(log["topics"].is_array(), "expected topics array, got: {log:?}");
    assert_hex_string(&log["data"], "data");
    assert!(log.get("removed").is_none(), "did not expect removed flag, got: {log:?}");
}

fn assert_fast_flashblock_transaction(tx: &serde_json::Value) {
    assert_hex_string(&tx["hash"], "hash");
    assert_hex_string(&tx["index"], "index");
    assert_hex_string(&tx["status"], "status");
}

fn assert_hot_only_unsupported_response(code: i64, message: &str, surface: &str) {
    assert_eq!(code, -32602, "unexpected error code for {surface}: {code}");
    assert!(
        message.contains("unsupported in flashblocks hot-only mode"),
        "unexpected hot-only error message for {surface}: {message}"
    );
    assert!(message.contains(surface), "expected {surface} in error message: {message}");
}

fn assert_fast_delta_matches(
    hot_delta: &FastFlashblockLogsDelta,
    legacy_delta: &FastFlashblockLogsDelta,
) {
    assert_eq!(hot_delta.block_number, legacy_delta.block_number);
    assert_eq!(hot_delta.flashblock_index, legacy_delta.flashblock_index);
    assert_eq!(hot_delta.payload_id, legacy_delta.payload_id);
    assert_eq!(hot_delta.parent_hash, legacy_delta.parent_hash);
    assert_eq!(hot_delta.block_timestamp, legacy_delta.block_timestamp);
    assert_eq!(hot_delta.logs, legacy_delta.logs);
    assert_eq!(hot_delta.transactions, legacy_delta.transactions);
}

fn assert_fast_delta_matches_ignoring_parent_hash(
    hot_delta: &FastFlashblockLogsDelta,
    legacy_delta: &FastFlashblockLogsDelta,
) {
    assert_eq!(hot_delta.block_number, legacy_delta.block_number);
    assert_eq!(hot_delta.flashblock_index, legacy_delta.flashblock_index);
    assert_eq!(hot_delta.payload_id, legacy_delta.payload_id);
    assert_eq!(hot_delta.block_timestamp, legacy_delta.block_timestamp);
    assert_eq!(hot_delta.logs, legacy_delta.logs);
    assert_eq!(hot_delta.transactions, legacy_delta.transactions);
}

fn assert_fast_delta_sequence_matches(
    hot_deltas: &[FastFlashblockLogsDelta],
    legacy_deltas: &[FastFlashblockLogsDelta],
) {
    assert_eq!(hot_deltas.len(), legacy_deltas.len());
    for (hot_delta, legacy_delta) in hot_deltas.iter().zip(legacy_deltas.iter()) {
        assert_fast_delta_matches(hot_delta, legacy_delta);
    }
}

#[tokio::test]
async fn test_eth_subscribe_new_flashblock_transactions_hashes() -> eyre::Result<()> {
    let setup = TestSetup::new().await?;
    let _provider = setup.harness.provider();
    let ws_url = setup.harness.ws_url();
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    // Subscribe to newFlashblockTransactions with default (hash only) mode
    ws_stream
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newFlashblockTransactions"]
            })
            .to_string()
            .into(),
        ))
        .await?;

    let response = ws_stream.next().await.unwrap()?;
    let sub: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    assert_eq!(sub["jsonrpc"], "2.0");
    assert_eq!(sub["id"], 1);
    let subscription_id = sub["result"].as_str().expect("subscription id expected");

    // Send first flashblock with L1 deposit tx
    setup.send_flashblock(setup.create_first_payload()).await?;

    // Each transaction is now sent as a separate message (one tx per message)
    let notification = ws_stream.next().await.unwrap()?;
    let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
    assert_eq!(notif["method"], "eth_subscription");
    assert_eq!(notif["params"]["subscription"], subscription_id);

    // Result should be a single transaction hash (string), not an array
    let tx_hash = &notif["params"]["result"];
    assert!(tx_hash.is_string(), "Expected hash string, got: {tx_hash:?}");

    // Send second flashblock with 9 more transactions (delta only, not cumulative)
    setup.send_flashblock(setup.create_second_payload()).await?;

    // Receive 9 separate messages (one per transaction in the delta)
    let mut received_hashes = Vec::new();
    for _ in 0..9 {
        let notification = ws_stream.next().await.unwrap()?;
        let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
        assert_eq!(notif["params"]["subscription"], subscription_id);
        let tx_hash = notif["params"]["result"].as_str().expect("expected hash string");
        received_hashes.push(tx_hash.to_string());
    }
    assert_eq!(received_hashes.len(), 9);

    Ok(())
}

#[tokio::test]
async fn test_eth_subscribe_new_flashblock_transactions_full() -> eyre::Result<()> {
    let setup = TestSetup::new().await?;
    let _provider = setup.harness.provider();
    let ws_url = setup.harness.ws_url();
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    // Subscribe to newFlashblockTransactions with full transaction objects (true)
    ws_stream
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newFlashblockTransactions", true]
            })
            .to_string()
            .into(),
        ))
        .await?;

    let response = ws_stream.next().await.unwrap()?;
    let sub: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    assert_eq!(sub["jsonrpc"], "2.0");
    assert_eq!(sub["id"], 1);
    let subscription_id = sub["result"].as_str().expect("subscription id expected");

    // Send flashblocks
    setup.send_flashblock(setup.create_first_payload()).await?;

    // Each transaction is now sent as a separate message (one tx per message)
    let notification = ws_stream.next().await.unwrap()?;
    let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
    assert_eq!(notif["method"], "eth_subscription");
    assert_eq!(notif["params"]["subscription"], subscription_id);

    // Result should be a single full transaction object with logs, not an array
    let tx = &notif["params"]["result"];
    assert!(tx.is_object(), "Expected transaction object, got: {tx:?}");
    assert!(tx["hash"].is_string(), "Expected full tx with hash field");
    assert!(tx["blockNumber"].is_string(), "Expected full tx with blockNumber field");
    assert!(tx["logs"].is_array(), "Expected logs array in full transaction");
    let gas_used = tx["gasUsed"].as_str().expect("expected gasUsed hex string");
    assert!(gas_used.starts_with("0x"), "Expected receipt-style gasUsed field, got: {gas_used}");
    assert_eq!(tx["status"], "0x1", "Expected receipt-style status field");
    assert!(
        tx["cumulativeGasUsed"].is_string(),
        "Expected cumulativeGasUsed field in full transaction"
    );
    assert!(
        tx["contractAddress"].is_null() || tx["contractAddress"].is_string(),
        "Expected contractAddress field in full transaction"
    );
    assert!(tx["logsBloom"].is_string(), "Expected logsBloom field in full transaction");

    // Send second flashblock with 9 more transactions (delta only, not cumulative)
    setup.send_flashblock(setup.create_second_payload()).await?;

    // Receive 9 separate messages (one per transaction in the delta)
    let mut received_count = 0;
    for _ in 0..9 {
        let notification = ws_stream.next().await.unwrap()?;
        let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
        assert_eq!(notif["params"]["subscription"], subscription_id);
        let tx = &notif["params"]["result"];
        assert!(tx["hash"].is_string() && tx["blockNumber"].is_string());
        assert!(tx["logs"].is_array(), "Expected logs array in full transaction");
        let gas_used = tx["gasUsed"].as_str().expect("expected gasUsed hex string");
        assert!(
            gas_used.starts_with("0x"),
            "Expected receipt-style gasUsed field, got: {gas_used}"
        );
        assert_eq!(tx["status"], "0x1", "Expected receipt-style status field");
        assert!(
            tx["cumulativeGasUsed"].is_string(),
            "Expected cumulativeGasUsed field in full transaction"
        );
        assert!(
            tx["contractAddress"].is_null() || tx["contractAddress"].is_string(),
            "Expected contractAddress field in full transaction"
        );
        assert!(tx["logsBloom"].is_string(), "Expected logsBloom field in full transaction");
        received_count += 1;
    }
    assert_eq!(received_count, 9);

    Ok(())
}

#[tokio::test]
async fn test_eth_subscribe_new_flashblock_logs_batch_unfiltered() -> eyre::Result<()> {
    let setup = TestSetup::new().await?;
    let _provider = setup.harness.provider();
    let ws_url = setup.harness.ws_url();
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    ws_stream
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newFlashblockLogsBatch"]
            })
            .to_string()
            .into(),
        ))
        .await?;

    let response = ws_stream.next().await.unwrap()?;
    let sub: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    assert_eq!(sub["jsonrpc"], "2.0");
    assert_eq!(sub["id"], 1);
    let subscription_id = sub["result"].as_str().expect("subscription id expected");

    setup.send_flashblock(setup.create_first_payload()).await?;

    let notification = ws_stream.next().await.unwrap()?;
    let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
    assert_eq!(notif["method"], "eth_subscription");
    assert_eq!(notif["params"]["subscription"], subscription_id);

    let batch = &notif["params"]["result"];
    assert_eq!(batch["blockNumber"], "0x1");
    assert_eq!(batch["flashblockIndex"], "0x0");
    assert!(batch["logs"].is_array(), "expected logs array, got: {batch:?}");
    assert!(batch["transactions"].is_array(), "expected transactions array, got: {batch:?}");
    assert_hex_string(&batch["batchHash"], "batchHash");

    setup.send_flashblock(setup.create_second_payload()).await?;

    let notification = ws_stream.next().await.unwrap()?;
    let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
    assert_eq!(notif["params"]["subscription"], subscription_id);

    let batch = &notif["params"]["result"];
    assert_eq!(batch["blockNumber"], "0x1");
    assert_eq!(batch["flashblockIndex"], "0x1");

    let logs = batch["logs"].as_array().expect("logs array expected");
    assert!(logs.len() >= 2, "expected at least 2 logs, got: {logs:?}");
    for log in logs {
        assert_flashblock_logs_batch_log(log);
    }

    let transactions = batch["transactions"].as_array().expect("transactions array expected");
    assert!(
        !transactions.is_empty(),
        "expected at least 1 transaction metadata entry, got: {transactions:?}"
    );
    for tx in transactions {
        assert_flashblock_logs_batch_transaction(tx);
    }

    Ok(())
}

#[tokio::test]
async fn test_eth_subscribe_new_flashblock_logs_batch_filter() -> eyre::Result<()> {
    let setup = TestSetup::new().await?;
    let _provider = setup.harness.provider();
    let ws_url = setup.harness.ws_url();
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    ws_stream
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": [
                    "newFlashblockLogsBatch",
                    { "address": setup.txn_details.log_emitter_a_address }
                ]
            })
            .to_string()
            .into(),
        ))
        .await?;

    let response = ws_stream.next().await.unwrap()?;
    let sub: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    assert_eq!(sub["jsonrpc"], "2.0");
    assert_eq!(sub["id"], 1);
    let subscription_id = sub["result"].as_str().expect("subscription id expected");

    setup.send_flashblock(setup.create_first_payload()).await?;
    let notification = ws_stream.next().await.unwrap()?;
    let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
    assert_eq!(notif["params"]["subscription"], subscription_id);

    setup.send_flashblock(setup.create_second_payload()).await?;

    let notification = ws_stream.next().await.unwrap()?;
    let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
    assert_eq!(notif["params"]["subscription"], subscription_id);

    let batch = &notif["params"]["result"];
    let logs = batch["logs"].as_array().expect("logs array expected");
    assert!(!logs.is_empty(), "expected filtered logs notification to contain logs");

    let filter_address = setup.txn_details.log_emitter_a_address.to_string().to_lowercase();
    for log in logs {
        assert_flashblock_logs_batch_log(log);
        let address = log["address"].as_str().expect("log address string expected");
        assert_eq!(address.to_lowercase(), filter_address);
    }

    let transactions = batch["transactions"].as_array().expect("transactions array expected");
    assert!(
        transactions.len() >= logs.len(),
        "expected tx metadata to be retained with filtered logs: txs={transactions:?} logs={logs:?}"
    );
    for tx in transactions {
        assert_flashblock_logs_batch_transaction(tx);
    }

    Ok(())
}

#[tokio::test]
async fn test_eth_subscribe_new_flashblock_logs_batch_invalid_params() -> eyre::Result<()> {
    let setup = TestSetup::new().await?;
    let _provider = setup.harness.provider();
    let ws_url = setup.harness.ws_url();
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    ws_stream
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newFlashblockLogsBatch", true]
            })
            .to_string()
            .into(),
        ))
        .await?;

    let response = ws_stream.next().await.unwrap()?;
    let error: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    assert_eq!(error["jsonrpc"], "2.0");
    assert_eq!(error["id"], 1);
    assert!(error.get("error").is_some(), "expected error response, got: {error:?}");
    let message = error["error"]["message"].as_str().expect("error message expected");
    assert!(message.contains("newFlashblockLogsBatch"), "unexpected error message: {message}");

    Ok(())
}

#[tokio::test]
async fn test_eth_subscribe_new_flashblock_logs_batch_null_params() -> eyre::Result<()> {
    let setup = TestSetup::new().await?;
    let _provider = setup.harness.provider();
    let ws_url = setup.harness.ws_url();
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    ws_stream
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newFlashblockLogsBatch", null]
            })
            .to_string()
            .into(),
        ))
        .await?;

    let response = ws_stream.next().await.unwrap()?;
    let sub: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    assert_eq!(sub["jsonrpc"], "2.0");
    assert_eq!(sub["id"], 1);
    let subscription_id = sub["result"].as_str().expect("subscription id expected");

    setup.send_flashblock(setup.create_first_payload()).await?;

    let notification = ws_stream.next().await.unwrap()?;
    let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
    assert_eq!(notif["method"], "eth_subscription");
    assert_eq!(notif["params"]["subscription"], subscription_id);

    let batch = &notif["params"]["result"];
    assert_eq!(batch["blockNumber"], "0x1");
    assert_eq!(batch["flashblockIndex"], "0x0");
    assert!(batch["logs"].is_array(), "expected logs array, got: {batch:?}");
    assert!(batch["transactions"].is_array(), "expected transactions array, got: {batch:?}");

    setup.send_flashblock(setup.create_second_payload()).await?;

    let notification = ws_stream.next().await.unwrap()?;
    let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
    assert_eq!(notif["params"]["subscription"], subscription_id);

    let batch = &notif["params"]["result"];
    assert_eq!(batch["blockNumber"], "0x1");
    assert_eq!(batch["flashblockIndex"], "0x1");

    let logs = batch["logs"].as_array().expect("logs array expected");
    assert!(logs.len() >= 2, "expected at least 2 logs, got: {logs:?}");

    let expected_trigger_hash = setup.txn_details.log_trigger_hash.to_string().to_lowercase();
    let expected_log_emitter_a = setup.txn_details.log_emitter_a_address.to_string().to_lowercase();
    let expected_log_emitter_b = setup.txn_details.log_emitter_b_address.to_string().to_lowercase();
    let mut seen_log_emitter_a = false;
    let mut seen_log_emitter_b = false;

    for log in logs {
        assert_flashblock_logs_batch_log(log);

        let address = log["address"].as_str().expect("log address string expected").to_lowercase();
        let tx_hash = log["txHash"].as_str().expect("log txHash string expected").to_lowercase();
        assert_eq!(tx_hash, expected_trigger_hash);

        if address == expected_log_emitter_a {
            seen_log_emitter_a = true;
        }
        if address == expected_log_emitter_b {
            seen_log_emitter_b = true;
        }
    }

    assert!(seen_log_emitter_a, "expected unfiltered logs to include LogEmitterA");
    assert!(seen_log_emitter_b, "expected unfiltered logs to include LogEmitterB");

    let transactions = batch["transactions"].as_array().expect("transactions array expected");
    assert!(
        !transactions.is_empty(),
        "expected at least 1 transaction metadata entry, got: {transactions:?}"
    );

    let mut saw_trigger_tx = false;
    for tx in transactions {
        assert_flashblock_logs_batch_transaction(tx);

        if tx["hash"]
            .as_str()
            .is_some_and(|hash| hash.eq_ignore_ascii_case(expected_trigger_hash.as_str()))
        {
            saw_trigger_tx = true;
        }
    }

    assert!(saw_trigger_tx, "expected tx metadata for log trigger tx, got: {transactions:?}");

    Ok(())
}

#[tokio::test]
async fn test_new_flashblock_logs_batch_hot_only_is_derived_from_fast_delta() -> eyre::Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let ws_url = setup.harness.ws_url();

    let (mut batch_ws, _) = connect_async(&ws_url).await?;
    batch_ws
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newFlashblockLogsBatch"]
            })
            .to_string()
            .into(),
        ))
        .await?;
    let batch_sub = batch_ws.next().await.unwrap()?;
    let batch_sub: serde_json::Value = serde_json::from_str(batch_sub.to_text()?)?;
    assert!(batch_sub["result"].is_string(), "batch subscription should be accepted");

    let (mut fast_ws, _) = connect_async(&ws_url).await?;
    fast_ws
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "eth_subscribe",
                "params": ["newFastFlashblockLogs"]
            })
            .to_string()
            .into(),
        ))
        .await?;
    let fast_sub = fast_ws.next().await.unwrap()?;
    let fast_sub: serde_json::Value = serde_json::from_str(fast_sub.to_text()?)?;
    assert!(fast_sub["result"].is_string(), "fast subscription should be accepted");

    for flashblock in [setup.create_first_payload(), setup.create_second_payload()] {
        setup.send_flashblock(flashblock).await?;
        assert!(
            setup.harness.flashblocks_state().get_pending_blocks().as_ref().is_none(),
            "hot-only logs batch must not depend on PendingBlocks"
        );

        let fast_notification = fast_ws.next().await.unwrap()?;
        let fast_notification: serde_json::Value =
            serde_json::from_str(fast_notification.to_text()?)?;
        let fast_delta: FastFlashblockLogsDelta =
            serde_json::from_value(fast_notification["params"]["result"].clone())?;

        let batch_notification = batch_ws.next().await.unwrap()?;
        let batch_notification: serde_json::Value =
            serde_json::from_str(batch_notification.to_text()?)?;
        let batch: FlashblockLogsBatch =
            serde_json::from_value(batch_notification["params"]["result"].clone())?;

        assert_eq!(batch, FlashblockLogsBatch::from_fast_delta(&fast_delta));
    }

    Ok(())
}

#[tokio::test]
async fn test_eth_subscribe_new_fast_flashblock_logs_hot_only_mode() -> eyre::Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let ws_url = setup.harness.ws_url();
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    ws_stream
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newFastFlashblockLogs"]
            })
            .to_string()
            .into(),
        ))
        .await?;

    let response = ws_stream.next().await.unwrap()?;
    let sub: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    assert_eq!(sub["jsonrpc"], "2.0");
    assert_eq!(sub["id"], 1);

    setup.send_flashblock(setup.create_first_payload()).await?;
    let notification = ws_stream.next().await.unwrap()?;
    let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;

    assert_fast_flashblock_snapshot_id(&notif["params"]["result"]["snapshotId"]);
    let snapshot_id: FlashblockSnapshotId =
        serde_json::from_value(notif["params"]["result"]["snapshotId"].clone())?;
    let latest = setup
        .harness
        .flashblocks_state()
        .get_latest_hot_snapshot()
        .expect("latest hot snapshot should exist after fast delta");
    assert_eq!(latest.snapshot_id, snapshot_id);
    assert!(notif["params"]["result"]["logs"].is_array());
    assert!(notif["params"]["result"]["transactions"].is_array());

    Ok(())
}

#[tokio::test]
async fn test_hot_only_pending_compatibility_subscriptions_are_rejected() -> eyre::Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let ws_url = setup.harness.ws_url();
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    for (id, params, surface) in [
        (1, json!(["newFlashblocks"]), "newFlashblocks"),
        (2, json!(["pendingLogs"]), "pendingLogs"),
        (3, json!(["newFlashblockTransactions"]), "newFlashblockTransactions"),
    ] {
        ws_stream
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": "eth_subscribe",
                    "params": params,
                })
                .to_string()
                .into(),
            ))
            .await?;

        let response = ws_stream.next().await.unwrap()?;
        let response: serde_json::Value = serde_json::from_str(response.to_text()?)?;
        assert_hot_only_unsupported_response(
            response["error"]["code"].as_i64().expect("error code expected"),
            response["error"]["message"].as_str().expect("error message expected"),
            surface,
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_hot_only_pending_style_rpcs_are_rejected() -> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let provider = setup.harness.provider();
    let url = setup.harness.rpc_url();
    let client = RpcClient::new_http(url.parse()?);

    let call_request = BaseTransactionRequest::default()
        .from(Account::Alice.address())
        .to(Account::Bob.address())
        .gas_limit(21_000)
        .value(U256::from(1u64))
        .input(TransactionInput::new(bytes!("0x")));

    let error = provider
        .get_block_by_number(BlockNumberOrTag::Pending)
        .await
        .expect_err("pending block must be rejected in hot-only mode");
    let error = error.as_error_resp().expect("json-rpc error response expected");
    assert_hot_only_unsupported_response(
        i64::from(error.code),
        &error.message,
        "eth_getBlockByNumber",
    );

    let error = provider
        .get_balance(TEST_ADDRESS)
        .pending()
        .await
        .expect_err("pending balance must be rejected in hot-only mode");
    let error = error.as_error_resp().expect("json-rpc error response expected");
    assert_hot_only_unsupported_response(i64::from(error.code), &error.message, "eth_getBalance");

    let error = provider
        .get_transaction_count(Account::Alice.address())
        .pending()
        .await
        .expect_err("pending transaction count must be rejected in hot-only mode");
    let error = error.as_error_resp().expect("json-rpc error response expected");
    assert_hot_only_unsupported_response(
        i64::from(error.code),
        &error.message,
        "eth_getTransactionCount",
    );

    let error = provider
        .call(call_request.clone())
        .block(BlockNumberOrTag::Pending.into())
        .await
        .expect_err("pending eth_call must be rejected in hot-only mode");
    let error = error.as_error_resp().expect("json-rpc error response expected");
    assert_hot_only_unsupported_response(i64::from(error.code), &error.message, "eth_call");
    assert!(
        error.message.contains("eth_baseCallAtFlashblock"),
        "pending eth_call should recommend eth_baseCallAtFlashblock: {}",
        error.message
    );

    let error = provider
        .estimate_gas(call_request.clone())
        .block(BlockNumberOrTag::Pending.into())
        .await
        .expect_err("pending estimate gas must be rejected in hot-only mode");
    let error = error.as_error_resp().expect("json-rpc error response expected");
    assert_hot_only_unsupported_response(i64::from(error.code), &error.message, "eth_estimateGas");
    assert!(
        error.message.contains("eth_baseEstimateGasAtFlashblock"),
        "pending eth_estimateGas should recommend eth_baseEstimateGasAtFlashblock: {}",
        error.message
    );

    let simulate_payload = SimulatePayload {
        block_state_calls: vec![SimBlock {
            calls: vec![call_request.clone().into()],
            block_overrides: None,
            state_overrides: None,
        }],
        trace_transfers: false,
        validation: true,
        return_full_transactions: true,
    };
    let error = provider
        .simulate(&simulate_payload)
        .block_id(BlockNumberOrTag::Pending.into())
        .await
        .expect_err("pending simulateV1 must be rejected in hot-only mode");
    let error = error.as_error_resp().expect("json-rpc error response expected");
    assert_hot_only_unsupported_response(i64::from(error.code), &error.message, "eth_simulateV1");

    let error = provider
        .get_logs(
            &alloy_rpc_types_eth::Filter::default()
                .from_block(BlockNumberOrTag::Latest)
                .to_block(BlockNumberOrTag::Pending),
        )
        .await
        .expect_err("pending getLogs must be rejected in hot-only mode");
    let error = error.as_error_resp().expect("json-rpc error response expected");
    assert_hot_only_unsupported_response(i64::from(error.code), &error.message, "eth_getLogs");
    assert!(
        error.message.contains("newFastFlashblockLogs"),
        "pending getLogs should recommend newFastFlashblockLogs: {}",
        error.message
    );

    let error = client
        .request::<_, Option<U256>>("eth_getBlockTransactionCountByNumber", ("pending",))
        .await
        .expect_err("pending block transaction count must be rejected in hot-only mode");
    let error = error.as_error_resp().expect("json-rpc error response expected");
    assert_hot_only_unsupported_response(
        i64::from(error.code),
        &error.message,
        "eth_getBlockTransactionCountByNumber",
    );

    let error = client
        .request::<_, RpcReceipt<Base>>(
            "eth_sendRawTransactionSync",
            (setup.txn_details.alice_eth_transfer_tx.clone(), Option::<u64>::None),
        )
        .await
        .expect_err("sendRawTransactionSync must be rejected in hot-only mode");
    let error = error.as_error_resp().expect("json-rpc error response expected");
    assert_hot_only_unsupported_response(
        i64::from(error.code),
        &error.message,
        "eth_sendRawTransactionSync",
    );

    Ok(())
}

#[tokio::test]
async fn test_hot_only_get_transaction_by_hash_uses_canonical_only() -> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let provider = setup.harness.provider();

    setup.send_test_payloads().await?;

    assert!(provider.get_transaction_by_hash(DEPOSIT_TX_HASH).await?.is_none());
    assert!(
        provider
            .get_transaction_by_hash(setup.txn_details.alice_eth_transfer_hash)
            .await?
            .is_none()
    );

    Ok(())
}

#[tokio::test]
async fn test_hot_only_get_transaction_receipt_uses_canonical_only() -> Result<()> {
    let setup = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;
    let provider = setup.harness.provider();

    setup.send_test_payloads().await?;

    assert!(provider.get_transaction_receipt(DEPOSIT_TX_HASH).await?.is_none());
    assert!(
        provider
            .get_transaction_receipt(setup.txn_details.alice_eth_transfer_hash)
            .await?
            .is_none()
    );

    Ok(())
}

#[tokio::test]
async fn test_hot_only_fast_delta_matches_legacy_rebuild_output() -> Result<()> {
    let legacy = TestSetup::new_with_mode(FlashblocksMode::Legacy).await?;
    let hot = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;

    let legacy_deltas = legacy
        .collect_fast_deltas(vec![legacy.create_first_payload(), legacy.create_second_payload()])
        .await?;
    let hot_deltas = hot
        .collect_fast_deltas(vec![hot.create_first_payload(), hot.create_second_payload()])
        .await?;

    assert_fast_delta_sequence_matches(&hot_deltas, &legacy_deltas);

    Ok(())
}

#[tokio::test]
async fn test_hot_only_fast_delta_matches_legacy_rebuild_output_across_rollover() -> Result<()> {
    let legacy = TestSetup::new_with_mode(FlashblocksMode::Legacy).await?;
    let hot = TestSetup::new_with_mode(FlashblocksMode::HotOnly).await?;

    let (hot_deltas, hot_pending_hash) = hot.collect_fast_deltas_with_rollover().await?;
    let (legacy_deltas, legacy_pending_hash) = legacy.collect_fast_deltas_with_rollover().await?;

    assert_fast_delta_sequence_matches(&hot_deltas[..2], &legacy_deltas[..2]);
    assert_eq!(legacy_deltas[2].parent_hash, legacy_pending_hash);
    assert_eq!(hot_deltas[2].parent_hash, hot_pending_hash);
    assert_ne!(legacy_deltas[2].parent_hash, legacy.canonical_parent_hash);
    assert_ne!(hot_deltas[2].parent_hash, hot.canonical_parent_hash);
    assert_fast_delta_matches_ignoring_parent_hash(&hot_deltas[2], &legacy_deltas[2]);

    Ok(())
}

#[tokio::test]
async fn test_eth_subscribe_new_fast_flashblock_logs_unfiltered() -> eyre::Result<()> {
    let setup = TestSetup::new().await?;
    let _provider = setup.harness.provider();
    let ws_url = setup.harness.ws_url();
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    ws_stream
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newFastFlashblockLogs"]
            })
            .to_string()
            .into(),
        ))
        .await?;

    let response = ws_stream.next().await.unwrap()?;
    let sub: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    assert_eq!(sub["jsonrpc"], "2.0");
    assert_eq!(sub["id"], 1);
    let subscription_id = sub["result"].as_str().expect("subscription id expected");

    setup.send_flashblock(setup.create_first_payload()).await?;

    let notification = ws_stream.next().await.unwrap()?;
    let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
    assert_eq!(notif["method"], "eth_subscription");
    assert_eq!(notif["params"]["subscription"], subscription_id);

    let delta = &notif["params"]["result"];
    assert_eq!(delta["blockNumber"], "0x1");
    assert_eq!(delta["flashblockIndex"], "0x0");
    assert!(delta.get("batchHash").is_none(), "did not expect batchHash in fast delta: {delta:?}");
    assert_fast_flashblock_snapshot_id(&delta["snapshotId"]);
    assert_eq!(delta["snapshotId"]["blockNumber"], delta["blockNumber"]);
    assert_eq!(delta["snapshotId"]["flashblockIndex"], delta["flashblockIndex"]);
    let snapshot_id: FlashblockSnapshotId = serde_json::from_value(delta["snapshotId"].clone())?;
    assert!(
        setup.harness.flashblocks_state().get_snapshot(snapshot_id).is_some(),
        "expected first fast update snapshot to be cached"
    );
    assert!(delta["logs"].is_array(), "expected logs array, got: {delta:?}");
    assert!(delta["transactions"].is_array(), "expected transactions array, got: {delta:?}");

    setup.send_flashblock(setup.create_second_payload()).await?;

    let notification = ws_stream.next().await.unwrap()?;
    let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
    assert_eq!(notif["params"]["subscription"], subscription_id);

    let delta = &notif["params"]["result"];
    assert_eq!(delta["blockNumber"], "0x1");
    assert_eq!(delta["flashblockIndex"], "0x1");
    assert!(delta.get("batchHash").is_none(), "did not expect batchHash in fast delta: {delta:?}");
    assert_fast_flashblock_snapshot_id(&delta["snapshotId"]);

    let logs = delta["logs"].as_array().expect("logs array expected");
    assert!(logs.len() >= 2, "expected at least 2 logs, got: {logs:?}");

    let expected_trigger_hash = setup.txn_details.log_trigger_hash.to_string().to_lowercase();
    let expected_log_emitter_a = setup.txn_details.log_emitter_a_address.to_string().to_lowercase();
    let expected_log_emitter_b = setup.txn_details.log_emitter_b_address.to_string().to_lowercase();
    let mut seen_log_emitter_a = false;
    let mut seen_log_emitter_b = false;

    for log in logs {
        assert_fast_flashblock_log(log);

        let address = log["address"].as_str().expect("log address string expected").to_lowercase();
        let tx_hash = log["txHash"].as_str().expect("log txHash string expected").to_lowercase();
        assert_eq!(tx_hash, expected_trigger_hash);

        if address == expected_log_emitter_a {
            seen_log_emitter_a = true;
        }
        if address == expected_log_emitter_b {
            seen_log_emitter_b = true;
        }
    }

    assert!(seen_log_emitter_a, "expected unfiltered logs to include LogEmitterA");
    assert!(seen_log_emitter_b, "expected unfiltered logs to include LogEmitterB");

    let transactions = delta["transactions"].as_array().expect("transactions array expected");
    assert!(
        !transactions.is_empty(),
        "expected at least 1 transaction metadata entry, got: {transactions:?}"
    );

    let mut saw_trigger_tx = false;
    for tx in transactions {
        assert_fast_flashblock_transaction(tx);

        if tx["hash"]
            .as_str()
            .is_some_and(|hash| hash.eq_ignore_ascii_case(expected_trigger_hash.as_str()))
        {
            saw_trigger_tx = true;
        }
    }

    assert!(saw_trigger_tx, "expected tx metadata for log trigger tx, got: {transactions:?}");

    Ok(())
}

#[tokio::test]
async fn test_eth_subscribe_new_fast_flashblock_logs_filter() -> eyre::Result<()> {
    let setup = TestSetup::new().await?;
    let _provider = setup.harness.provider();
    let ws_url = setup.harness.ws_url();
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    ws_stream
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": [
                    "newFastFlashblockLogs",
                    { "address": setup.txn_details.log_emitter_a_address }
                ]
            })
            .to_string()
            .into(),
        ))
        .await?;

    let response = ws_stream.next().await.unwrap()?;
    let sub: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    assert_eq!(sub["jsonrpc"], "2.0");
    assert_eq!(sub["id"], 1);
    let subscription_id = sub["result"].as_str().expect("subscription id expected");

    setup.send_flashblock(setup.create_first_payload()).await?;

    let notification = ws_stream.next().await.unwrap()?;
    let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
    assert_eq!(notif["params"]["subscription"], subscription_id);

    let delta = &notif["params"]["result"];
    assert!(delta.get("batchHash").is_none(), "did not expect batchHash in fast delta: {delta:?}");
    assert!(delta["logs"].as_array().expect("logs array expected").is_empty());
    assert!(delta["transactions"].as_array().expect("transactions array expected").is_empty());

    setup.send_flashblock(setup.create_second_payload()).await?;

    let notification = ws_stream.next().await.unwrap()?;
    let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
    assert_eq!(notif["params"]["subscription"], subscription_id);

    let delta = &notif["params"]["result"];
    let logs = delta["logs"].as_array().expect("logs array expected");
    assert_eq!(logs.len(), 1, "expected exactly one filtered log, got: {logs:?}");

    let expected_trigger_hash = setup.txn_details.log_trigger_hash.to_string().to_lowercase();
    let filter_address = setup.txn_details.log_emitter_a_address.to_string().to_lowercase();
    for log in logs {
        assert_fast_flashblock_log(log);
        let address = log["address"].as_str().expect("log address string expected");
        let tx_hash = log["txHash"].as_str().expect("log txHash string expected");
        assert_eq!(address.to_lowercase(), filter_address);
        assert_eq!(tx_hash.to_lowercase(), expected_trigger_hash);
    }

    let transactions = delta["transactions"].as_array().expect("transactions array expected");
    assert_eq!(
        transactions.len(),
        1,
        "expected only referenced tx metadata to remain, got: {transactions:?}"
    );
    assert_fast_flashblock_transaction(&transactions[0]);
    assert_eq!(
        transactions[0]["hash"].as_str().expect("tx hash string expected").to_lowercase(),
        expected_trigger_hash
    );
    assert_eq!(transactions[0]["index"], logs[0]["txIndex"]);

    Ok(())
}

#[tokio::test]
async fn test_eth_subscribe_new_fast_flashblock_logs_invalid_params() -> eyre::Result<()> {
    let setup = TestSetup::new().await?;
    let _provider = setup.harness.provider();
    let ws_url = setup.harness.ws_url();
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    ws_stream
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newFastFlashblockLogs", true]
            })
            .to_string()
            .into(),
        ))
        .await?;

    let response = ws_stream.next().await.unwrap()?;
    let error: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    assert_eq!(error["jsonrpc"], "2.0");
    assert_eq!(error["id"], 1);
    assert!(error.get("error").is_some(), "expected error response, got: {error:?}");
    let message = error["error"]["message"].as_str().expect("error message expected");
    assert!(message.contains("newFastFlashblockLogs"), "unexpected error message: {message}");

    Ok(())
}

#[tokio::test]
async fn test_eth_subscribe_new_fast_flashblock_logs_null_params() -> eyre::Result<()> {
    let setup = TestSetup::new().await?;
    let _provider = setup.harness.provider();
    let ws_url = setup.harness.ws_url();
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    ws_stream
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newFastFlashblockLogs", null]
            })
            .to_string()
            .into(),
        ))
        .await?;

    let response = ws_stream.next().await.unwrap()?;
    let sub: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    assert_eq!(sub["jsonrpc"], "2.0");
    assert_eq!(sub["id"], 1);
    let subscription_id = sub["result"].as_str().expect("subscription id expected");

    setup.send_flashblock(setup.create_first_payload()).await?;

    let notification = ws_stream.next().await.unwrap()?;
    let notif: serde_json::Value = serde_json::from_str(notification.to_text()?)?;
    assert_eq!(notif["method"], "eth_subscription");
    assert_eq!(notif["params"]["subscription"], subscription_id);

    let delta = &notif["params"]["result"];
    assert_eq!(delta["blockNumber"], "0x1");
    assert_eq!(delta["flashblockIndex"], "0x0");
    assert!(delta.get("batchHash").is_none(), "did not expect batchHash in fast delta: {delta:?}");
    assert_fast_flashblock_snapshot_id(&delta["snapshotId"]);
    assert!(delta["logs"].is_array(), "expected logs array, got: {delta:?}");
    assert!(delta["transactions"].is_array(), "expected transactions array, got: {delta:?}");

    Ok(())
}

#[tokio::test]
async fn test_get_block_transaction_count_by_number_pending() -> Result<()> {
    let setup = TestSetup::new().await?;
    let url = setup.harness.rpc_url();
    let client = RpcClient::new_http(url.parse()?);

    // Query pending block transaction count when no flashblocks exist
    // Should fall back to latest block (block 0 with 0 transactions)
    let count: Option<U256> =
        client.request("eth_getBlockTransactionCountByNumber", ("pending",)).await?;
    assert_eq!(count, Some(U256::from(0)));

    // Send first flashblock with 1 transaction (L1Info deposit)
    setup.send_flashblock(setup.create_first_payload()).await?;

    let count: Option<U256> =
        client.request("eth_getBlockTransactionCountByNumber", ("pending",)).await?;
    assert_eq!(count, Some(U256::from(1)));

    // Send second flashblock with 9 more transactions
    setup.send_flashblock(setup.create_second_payload()).await?;

    let count: Option<U256> =
        client.request("eth_getBlockTransactionCountByNumber", ("pending",)).await?;
    // Total: 1 (L1Info) + 9 (second payload) = 10 transactions
    assert_eq!(count, Some(U256::from(10)));

    // Query non-pending block (latest = block 0)
    let count: Option<U256> =
        client.request("eth_getBlockTransactionCountByNumber", ("latest",)).await?;
    assert_eq!(count, Some(U256::from(0)));

    Ok(())
}

#[tokio::test]
async fn test_pending_block_header_fields() -> Result<()> {
    let setup = TestSetup::new().await?;
    let provider = setup.harness.provider();

    // Send flashblocks to create pending state
    setup.send_test_payloads().await?;

    // Query pending block
    let pending_block = provider
        .get_block_by_number(BlockNumberOrTag::Pending)
        .await?
        .expect("pending block expected");

    // Verify withdrawals is empty array (not null)
    assert_eq!(
        pending_block.withdrawals,
        Some(vec![].into()),
        "withdrawals should be an empty array"
    );

    // Verify parent_beacon_block_root matches the test value
    assert_eq!(
        pending_block.header.parent_beacon_block_root,
        Some(TEST_PARENT_BEACON_BLOCK_ROOT),
        "parent_beacon_block_root should match test value"
    );

    // Verify withdrawals_root is the empty withdrawals hash
    assert_eq!(
        pending_block.header.withdrawals_root,
        Some(EMPTY_WITHDRAWALS),
        "withdrawals_root should be EMPTY_WITHDRAWALS"
    );

    // Verify requests_hash is EMPTY_REQUESTS_HASH
    assert_eq!(
        pending_block.header.requests_hash,
        Some(EMPTY_REQUESTS_HASH),
        "requests_hash should be EMPTY_REQUESTS_HASH"
    );

    Ok(())
}
