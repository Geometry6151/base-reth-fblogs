//! Metrics for flashblocks.

base_metrics::define_metrics! {
    reth_flashblocks
    #[describe("Count of times upstream receiver was closed/errored")]
    upstream_errors: counter,
    #[describe("Count of messages received from the upstream source")]
    upstream_messages: counter,
    #[describe("Time taken to decode upstream flashblock messages")]
    upstream_decode_duration: histogram,
    #[describe("Time taken to successfully apply a flashblock to pending state")]
    block_processing_duration: histogram,
    #[describe("Time taken to attempt applying a flashblock, including success, cache, and error paths")]
    flashblock_apply_duration: histogram,
    #[describe("Time spent waiting in the flashblock state queue before processing starts")]
    state_queue_delay_duration: histogram,
    #[describe("Time taken to build pending state from flashblocks")]
    pending_state_build_duration: histogram,
    #[describe("Time taken to build the newFlashblocks payload from pending state")]
    fast_delta_build_duration: histogram,
    #[describe("Time taken to build the newFlashblockLogsBatch payload")]
    logs_batch_build_duration: histogram,
    #[describe("Time taken to serialize newFlashblockLogsBatch subscription payloads")]
    logs_batch_pubsub_serialize_duration: histogram,
    #[describe("Time taken to serialize newFlashblocks subscription payloads")]
    fast_pubsub_serialize_duration: histogram,
    #[describe("Time taken to estimate gas against a state-pinned flashblock snapshot with best-effort block env")]
    pinned_estimate_gas_duration: histogram,
    #[describe("Time taken to execute a call against a pinned flashblock snapshot")]
    pinned_call_duration: histogram,
    #[describe("Time spent on parallel sender recovery")]
    sender_recovery_duration: histogram,
    #[describe("Number of Flashblocks that arrive in an unexpected order")]
    unexpected_block_order: counter,
    #[describe("Number of flashblocks in a block")]
    flashblocks_in_block: histogram,
    #[describe("Count of times flashblocks are unable to be converted to blocks")]
    block_processing_error: counter,
    #[describe("Number of times pending snapshot was cleared because canonical caught up")]
    pending_clear_catchup: counter,
    #[describe("Number of times pending snapshot was cleared because of reorg")]
    pending_clear_reorg: counter,
    #[describe("Pending snapshot flashblock index (current)")]
    pending_snapshot_fb_index: gauge,
    #[describe("Pending snapshot block number (current)")]
    pending_snapshot_height: gauge,
    #[describe("Total number of WebSocket reconnection attempts")]
    reconnect_attempts: counter,
    #[describe("Count of times flashblocks get_transaction_count is called")]
    rpc_get_transaction_count: counter,
    #[describe("Count of times flashblocks get_transaction_receipt is called")]
    rpc_get_transaction_receipt: counter,
    #[describe("Count of times flashblocks get_transaction_by_hash is called")]
    rpc_get_transaction_by_hash: counter,
    #[describe("Count of times flashblocks get_balance is called")]
    rpc_get_balance: counter,
    #[describe("Count of times flashblocks get_block_by_number is called")]
    rpc_get_block_by_number: counter,
    #[describe("Count of times flashblocks call is called")]
    rpc_call: counter,
    #[describe("Count of times flashblocks estimate_gas is called")]
    rpc_estimate_gas: counter,
    #[describe("Count of times flashblocks simulate_v1 is called")]
    rpc_simulate_v1: counter,
    #[describe("Count of times flashblocks get_logs is called")]
    rpc_get_logs: counter,
    #[describe("Count of times flashblocks get_block_transaction_count_by_number is called")]
    rpc_get_block_transaction_count_by_number: counter,
    #[describe("Time taken to clone bundle state")]
    bundle_state_clone_duration: histogram,
    #[describe("Size of bundle state being cloned (number of accounts)")]
    bundle_state_clone_size: histogram,
}
