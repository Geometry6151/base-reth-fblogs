//! Cache of pending block snapshots keyed by flashblock snapshot id.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{FlashblockSnapshotId, Metrics, PendingBlocks};

/// Default number of pending snapshots retained when no explicit capacity is provided.
pub(crate) const DEFAULT_SNAPSHOT_CACHE_CAPACITY: usize = 16;

/// Default time-to-live for cached pending snapshots.
pub(crate) const DEFAULT_SNAPSHOT_CACHE_TTL: Duration = Duration::from_secs(5);

/// Bounded in-memory cache for pending block snapshots.
#[derive(Debug)]
pub struct SnapshotCache {
    max_entries: usize,
    ttl: Duration,
    entries: HashMap<FlashblockSnapshotId, (Instant, Arc<PendingBlocks>)>,
    order: VecDeque<FlashblockSnapshotId>,
}

impl Default for SnapshotCache {
    fn default() -> Self {
        Self::new(DEFAULT_SNAPSHOT_CACHE_CAPACITY, DEFAULT_SNAPSHOT_CACHE_TTL)
    }
}

impl SnapshotCache {
    /// Creates a new snapshot cache.
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self { max_entries, ttl, entries: HashMap::new(), order: VecDeque::new() }
    }

    /// Inserts or replaces a pending block snapshot.
    pub fn insert(
        &mut self,
        snapshot_id: FlashblockSnapshotId,
        pending_blocks: Arc<PendingBlocks>,
    ) {
        let _insert_timer = base_metrics::timed!(Metrics::snapshot_cache_insert_duration());
        let now = Instant::now();
        self.prune_expired(now);

        self.order.retain(|queued_snapshot_id| queued_snapshot_id != &snapshot_id);
        self.entries.insert(snapshot_id, (now, pending_blocks));
        self.order.push_back(snapshot_id);

        while self.entries.len() > self.max_entries {
            let Some(oldest_snapshot_id) = self.order.pop_front() else {
                break;
            };
            if self.entries.remove(&oldest_snapshot_id).is_some() {
                Metrics::snapshot_cache_evictions().increment(1);
            }
        }
    }

    /// Returns the cached pending blocks for the given snapshot id.
    pub fn get(&self, snapshot_id: &FlashblockSnapshotId) -> Option<Arc<PendingBlocks>> {
        let _get_timer = base_metrics::timed!(Metrics::snapshot_cache_get_duration());
        let now = Instant::now();

        let snapshot = self.entries.get(snapshot_id).and_then(|(inserted_at, pending_blocks)| {
            if self.is_expired(*inserted_at, now) { None } else { Some(Arc::clone(pending_blocks)) }
        });

        if snapshot.is_some() {
            Metrics::snapshot_cache_hits().increment(1);
        } else {
            Metrics::snapshot_cache_misses().increment(1);
        }

        snapshot
    }

    /// Removes all cached snapshots.
    pub fn clear(&mut self) {
        let _clear_timer = base_metrics::timed!(Metrics::snapshot_cache_clear_duration());
        self.entries.clear();
        self.order.clear();
    }

    /// Prunes snapshots that are expired as of the provided instant.
    pub fn prune_expired(&mut self, now: Instant) {
        loop {
            let Some(snapshot_id) = self.order.front().copied() else {
                break;
            };

            let Some((inserted_at, _)) = self.entries.get(&snapshot_id) else {
                self.order.pop_front();
                continue;
            };

            if !self.is_expired(*inserted_at, now) {
                break;
            }

            self.order.pop_front();
            if self.entries.remove(&snapshot_id).is_some() {
                Metrics::snapshot_cache_evictions().increment(1);
            }
        }
    }

    fn is_expired(&self, inserted_at: Instant, now: Instant) -> bool {
        matches!(now.checked_duration_since(inserted_at), Some(age) if age >= self.ttl)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "metrics")]
    use std::{collections::HashMap, sync::Mutex};
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use alloy_consensus::{Header, Sealed};
    use alloy_primitives::B256;
    use alloy_rpc_types_engine::PayloadId;
    use base_common_flashblocks::{ExecutionPayloadFlashblockDeltaV1, Flashblock, Metadata};
    #[cfg(feature = "metrics")]
    use metrics::{
        Counter, CounterFn, Gauge, Histogram, HistogramFn, Key, KeyName,
        Metadata as MetricMetadata, Recorder, SharedString, Unit,
    };

    use super::*;
    use crate::PendingBlocksBuilder;

    #[cfg(feature = "metrics")]
    #[derive(Clone, Default)]
    struct TestRecorder {
        counters: Arc<Mutex<HashMap<String, u64>>>,
        histograms: Arc<Mutex<HashMap<String, usize>>>,
    }

    #[cfg(feature = "metrics")]
    impl TestRecorder {
        fn counter_value(&self, name: &str) -> Option<u64> {
            self.counters.lock().expect("counter mutex poisoned").get(name).copied()
        }

        fn histogram_samples(&self, name: &str) -> Option<usize> {
            self.histograms.lock().expect("histogram mutex poisoned").get(name).copied()
        }
    }

    #[cfg(feature = "metrics")]
    #[derive(Debug)]
    struct TestCounter {
        name: String,
        counters: Arc<Mutex<HashMap<String, u64>>>,
    }

    #[cfg(feature = "metrics")]
    impl CounterFn for TestCounter {
        fn increment(&self, value: u64) {
            let mut counters = self.counters.lock().expect("counter mutex poisoned");
            *counters.entry(self.name.clone()).or_default() += value;
        }

        fn absolute(&self, value: u64) {
            let mut counters = self.counters.lock().expect("counter mutex poisoned");
            counters.insert(self.name.clone(), value);
        }
    }

    #[cfg(feature = "metrics")]
    #[derive(Debug)]
    struct TestHistogram {
        name: String,
        histograms: Arc<Mutex<HashMap<String, usize>>>,
    }

    #[cfg(feature = "metrics")]
    impl HistogramFn for TestHistogram {
        fn record(&self, _value: f64) {
            let mut histograms = self.histograms.lock().expect("histogram mutex poisoned");
            *histograms.entry(self.name.clone()).or_default() += 1;
        }
    }

    #[cfg(feature = "metrics")]
    impl Recorder for TestRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        }

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(
            &self,
            _key: KeyName,
            _unit: Option<Unit>,
            _description: SharedString,
        ) {
        }

        fn register_counter(&self, key: &Key, _metadata: &MetricMetadata<'_>) -> Counter {
            Counter::from_arc(Arc::new(TestCounter {
                name: key.name().to_owned(),
                counters: Arc::clone(&self.counters),
            }))
        }

        fn register_gauge(&self, _key: &Key, _metadata: &MetricMetadata<'_>) -> Gauge {
            Gauge::noop()
        }

        fn register_histogram(&self, key: &Key, _metadata: &MetricMetadata<'_>) -> Histogram {
            Histogram::from_arc(Arc::new(TestHistogram {
                name: key.name().to_owned(),
                histograms: Arc::clone(&self.histograms),
            }))
        }
    }

    #[cfg(feature = "metrics")]
    fn with_recorder(f: impl FnOnce(&TestRecorder)) {
        let recorder = TestRecorder::default();
        metrics::with_local_recorder(&recorder, || f(&recorder));
    }

    fn snapshot_id(nonce: u64) -> FlashblockSnapshotId {
        FlashblockSnapshotId::new(
            nonce,
            nonce,
            nonce,
            PayloadId::new([nonce as u8; 8]),
            B256::with_last_byte(nonce as u8),
        )
    }

    fn pending_blocks(block_number: u64, flashblock_index: u64) -> Arc<PendingBlocks> {
        let mut builder = PendingBlocksBuilder::new();
        builder.with_flashblocks([Flashblock {
            payload_id: PayloadId::default(),
            index: flashblock_index,
            base: None,
            diff: ExecutionPayloadFlashblockDeltaV1::default(),
            metadata: Metadata { block_number },
        }]);
        builder.with_header(Sealed::new_unchecked(
            Header { number: block_number, ..Default::default() },
            B256::ZERO,
        ));
        Arc::new(builder.build().expect("pending blocks should build"))
    }

    fn insert_snapshot_at(
        cache: &mut SnapshotCache,
        snapshot_id: FlashblockSnapshotId,
        inserted_at: Instant,
        pending_blocks: Arc<PendingBlocks>,
    ) {
        cache.order.push_back(snapshot_id);
        cache.entries.insert(snapshot_id, (inserted_at, pending_blocks));
    }

    #[test]
    fn insert_and_lookup_returns_same_arc() {
        let mut cache =
            SnapshotCache::new(DEFAULT_SNAPSHOT_CACHE_CAPACITY, DEFAULT_SNAPSHOT_CACHE_TTL);
        let id = snapshot_id(1);
        let pending_blocks = pending_blocks(1, 0);

        cache.insert(id, Arc::clone(&pending_blocks));

        let cached = cache.get(&id).expect("snapshot should be cached");
        assert!(Arc::ptr_eq(&pending_blocks, &cached));
    }

    #[test]
    fn count_eviction_removes_oldest() {
        let mut cache = SnapshotCache::new(2, Duration::from_secs(5));
        let first_id = snapshot_id(1);
        let second_id = snapshot_id(2);
        let third_id = snapshot_id(3);

        cache.insert(first_id, pending_blocks(1, 0));
        cache.insert(second_id, pending_blocks(2, 0));
        cache.insert(third_id, pending_blocks(3, 0));

        assert!(cache.get(&first_id).is_none());
        assert!(cache.get(&second_id).is_some());
        assert!(cache.get(&third_id).is_some());
    }

    #[test]
    fn get_does_not_return_expired_snapshots() {
        let ttl = Duration::from_secs(5);
        let mut cache = SnapshotCache::new(DEFAULT_SNAPSHOT_CACHE_CAPACITY, ttl);
        let id = snapshot_id(1);
        let inserted_at = Instant::now() - (ttl + Duration::from_secs(1));

        insert_snapshot_at(&mut cache, id, inserted_at, pending_blocks(1, 0));

        assert!(cache.get(&id).is_none());
    }

    #[test]
    fn prune_expired_removes_only_expired_snapshots() {
        let ttl = Duration::from_secs(5);
        let mut cache = SnapshotCache::new(DEFAULT_SNAPSHOT_CACHE_CAPACITY, ttl);
        let expired_id = snapshot_id(1);
        let fresh_id = snapshot_id(2);
        let now = Instant::now();

        insert_snapshot_at(
            &mut cache,
            expired_id,
            now - (ttl + Duration::from_secs(1)),
            pending_blocks(1, 0),
        );
        insert_snapshot_at(
            &mut cache,
            fresh_id,
            now - Duration::from_secs(1),
            pending_blocks(2, 0),
        );

        cache.prune_expired(now);

        assert!(cache.get(&expired_id).is_none());
        assert!(cache.get(&fresh_id).is_some());
    }

    #[test]
    fn clear_removes_all_snapshots() {
        let mut cache =
            SnapshotCache::new(DEFAULT_SNAPSHOT_CACHE_CAPACITY, DEFAULT_SNAPSHOT_CACHE_TTL);
        let first_id = snapshot_id(1);
        let second_id = snapshot_id(2);

        cache.insert(first_id, pending_blocks(1, 0));
        cache.insert(second_id, pending_blocks(2, 0));
        cache.clear();

        assert!(cache.get(&first_id).is_none());
        assert!(cache.get(&second_id).is_none());
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn records_snapshot_cache_metrics() {
        with_recorder(|recorder| {
            let ttl = Duration::from_secs(5);
            let mut cache = SnapshotCache::new(1, ttl);
            let first_id = snapshot_id(1);
            let second_id = snapshot_id(2);
            let missing_id = snapshot_id(9);

            cache.insert(first_id, pending_blocks(1, 0));
            assert!(cache.get(&first_id).is_some());
            assert!(cache.get(&missing_id).is_none());
            cache.insert(second_id, pending_blocks(2, 0));
            cache.clear();

            let mut expired_cache = SnapshotCache::new(DEFAULT_SNAPSHOT_CACHE_CAPACITY, ttl);
            let expired_id = snapshot_id(3);
            let now = Instant::now();
            insert_snapshot_at(
                &mut expired_cache,
                expired_id,
                now - (ttl + Duration::from_secs(1)),
                pending_blocks(3, 0),
            );

            assert!(expired_cache.get(&expired_id).is_none());
            expired_cache.prune_expired(now);

            assert_eq!(
                recorder.histogram_samples("reth_flashblocks.snapshot_cache_insert_duration"),
                Some(2),
            );
            assert_eq!(
                recorder.histogram_samples("reth_flashblocks.snapshot_cache_get_duration"),
                Some(3),
            );
            assert_eq!(
                recorder.histogram_samples("reth_flashblocks.snapshot_cache_clear_duration"),
                Some(1),
            );
            assert_eq!(recorder.counter_value("reth_flashblocks.snapshot_cache_hits"), Some(1));
            assert_eq!(recorder.counter_value("reth_flashblocks.snapshot_cache_misses"), Some(2));
            assert_eq!(
                recorder.counter_value("reth_flashblocks.snapshot_cache_evictions"),
                Some(2)
            );
        });
    }
}
