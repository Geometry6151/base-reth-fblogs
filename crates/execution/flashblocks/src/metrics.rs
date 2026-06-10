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
    #[describe("Time taken to execute only the new hot suffix transactions")]
    hot_suffix_execute_duration: histogram,
    #[describe("Number of transactions executed in one hot suffix apply")]
    hot_suffix_tx_count: histogram,
    #[describe("Time taken to build a hot fast-log delta from executed suffix results")]
    hot_delta_build_duration: histogram,
    #[describe("Time taken to roll the hot window to the next pending block")]
    hot_window_rollover_duration: histogram,
    #[describe("Count of times the hot window was reset from flashblock sequencing or parent mismatch")]
    hot_window_reset_count: counter,
    #[describe("Count of hot window resets caused by receiving a non-zero flashblock index without an active window")]
    hot_window_reset_non_zero_first_index_count: counter,
    #[describe("Count of hot window resets caused by a non-sequential same-block flashblock gap")]
    hot_window_reset_sequence_gap_count: counter,
    #[describe("Count of hot window resets caused by receiving a new block without flashblock index zero")]
    hot_window_reset_invalid_new_block_index_count: counter,
    #[describe("Count of hot window resets caused by first-flashblock parent mismatch against canonical")]
    hot_window_reset_first_parent_mismatch_count: counter,
    #[describe("Count of hot window resets caused by missing active block state")]
    hot_window_reset_missing_active_block_count: counter,
    #[describe("Count of hot window resets caused by same-block parent mismatch")]
    hot_window_reset_same_block_parent_mismatch_count: counter,
    #[describe("Count of hot window resets caused by rollover parent mismatch")]
    hot_window_reset_rollover_parent_mismatch_count: counter,
    #[describe("Time taken to materialize an optional hot snapshot from carried execution state")]
    hot_snapshot_materialize_duration: histogram,
    #[describe("Count of times canonical processing forced a hot window reset")]
    hot_canonical_reset_count: counter,
    #[describe("Time taken to build the newFastFlashblockLogs delta from pending state")]
    fast_delta_build_duration: histogram,
    #[describe("Time taken to build the newFlashblocks block payload from pending state")]
    new_flashblocks_build_duration: histogram,
    #[describe("Time taken to build the newFlashblockLogsBatch payload")]
    logs_batch_build_duration: histogram,
    #[describe("Time taken to serialize newFlashblockLogsBatch subscription payloads")]
    logs_batch_pubsub_serialize_duration: histogram,
    #[describe("Time taken to serialize newFastFlashblockLogs subscription payloads")]
    fast_pubsub_serialize_duration: histogram,
    #[describe("Time taken to send newFastFlashblockLogs subscription payloads")]
    fast_pubsub_send_duration: histogram,
    #[describe("Time taken to send newFlashblockLogsBatch subscription payloads")]
    logs_batch_pubsub_send_duration: histogram,
    #[describe("Time taken to insert a pending snapshot into the snapshot cache")]
    snapshot_cache_insert_duration: histogram,
    #[describe("Time taken to look up a pending snapshot in the snapshot cache")]
    snapshot_cache_get_duration: histogram,
    #[describe("Time taken to clear the snapshot cache")]
    snapshot_cache_clear_duration: histogram,
    #[describe("Count of snapshot cache hits")]
    snapshot_cache_hits: counter,
    #[describe("Count of snapshot cache misses, including key-not-found and expired reads")]
    snapshot_cache_misses: counter,
    #[describe("Count of snapshot cache evictions from capacity pressure or explicit pruning")]
    snapshot_cache_evictions: counter,
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
