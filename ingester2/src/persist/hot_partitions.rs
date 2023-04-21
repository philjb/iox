use std::{fmt::Debug, sync::Arc};

use observability_deps::tracing::info;
use parking_lot::{Mutex, MutexGuard};

use crate::buffer_tree::{partition::PartitionData, post_write::PostWriteObserver};

use super::queue::PersistQueue;

/// A [`PostWriteObserver`] that triggers persistence of a partition when the
/// estimated persistence cost exceeds a pre-configured limit.
#[derive(Debug)]
pub(crate) struct HotPartitionPersister<P> {
    persist_handle: P,
    max_estimated_persist_cost: usize,

    /// A metric tracking the number of partitions persisted as "hot partitions".
    persist_count: metric::U64Counter,
}

impl<P> HotPartitionPersister<P>
where
    P: PersistQueue + Clone + Sync + 'static,
{
    pub fn new(
        persist_handle: P,
        max_estimated_persist_cost: usize,
        metrics: &metric::Registry,
    ) -> Self {
        let persist_count = metrics
            .register_metric::<metric::U64Counter>(
                "ingester_persist_hot_partition_enqueue_count",
                "number of times persistence of a partition has been triggered \
                because the persist cost exceeded the pre-configured limit",
            )
            .recorder(&[]);
        Self {
            persist_handle,
            max_estimated_persist_cost,
            persist_count,
        }
    }

    #[cold]
    fn persist(
        &self,
        cost_estimate: usize,
        partition: Arc<Mutex<PartitionData>>,
        mut guard: MutexGuard<'_, PartitionData>,
    ) {
        info!(
            partition_id = guard.partition_id().get(),
            cost_estimate, "marking hot partition for persistence"
        );

        let data = guard
            .mark_persisting()
            .expect("failed to transition buffer fsm to persisting state");

        // Perform the enqueue in a separate task, to avoid blocking this
        // writer if the persist system is saturated.
        let persist_handle = self.persist_handle.clone();
        tokio::spawn(async move {
            // There is no need to await on the completion handle.
            persist_handle.enqueue(partition, data).await;
        });
        // Update any exported metrics.
        self.persist_count.inc(1);
    }
}

impl<P> PostWriteObserver for HotPartitionPersister<P>
where
    P: PersistQueue + Clone + Sync + 'static,
{
    #[inline(always)]
    fn observe(&self, partition: Arc<Mutex<PartitionData>>, guard: MutexGuard<'_, PartitionData>) {
        // Without releasing the lock, obtain the new persist cost estimate.
        //
        // By holding the write lock, concurrent writes are blocked while the
        // cost is evaluated. This prevents "overrun" where parallel writes are
        // applied while the cost is evaluated concurrently in this thread.
        let cost_estimate = guard.persist_cost_estimate();

        // This observer is called after a successful write, therefore
        // persisting the partition MUST have a non-zero cost.
        assert!(cost_estimate > 0);

        // If the estimated persist cost is over the limit, mark the
        // partition as persisting.
        //
        // This SHOULD happen atomically with the above write, to ensure
        // accurate buffer costing - if the lock were to be released, more
        // writes could be added to the buffer in parallel, exceeding the
        // limit before it was marked as persisting.
        if cost_estimate >= self.max_estimated_persist_cost {
            self.persist(cost_estimate, partition, guard)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use assert_matches::assert_matches;
    use data_types::{NamespaceId, ObjectStorePathPartitionId, PartitionKey, SequenceNumber, ShardId, TableId};
    use lazy_static::lazy_static;
    use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
    use parking_lot::Mutex;

    use crate::{
        buffer_tree::{
            namespace::NamespaceName, partition::PartitionData, partition::SortKeyState,
            table::TableName,
        },
        deferred_load::DeferredLoad,
        persist::queue::mock::MockPersistQueue,
    };

    use super::*;

    const PARTITION_ID: ObjectStorePathPartitionId = ObjectStorePathPartitionId::new(1);
    const TRANSITION_SHARD_ID: ShardId = ShardId::new(84);

    lazy_static! {
        static ref PARTITION_KEY: PartitionKey = PartitionKey::from("pohtaytoes");
        static ref TABLE_NAME: TableName = TableName::from("potatoes");
        static ref NAMESPACE_NAME: NamespaceName = NamespaceName::from("namespace-potatoes");
    }

    #[tokio::test]
    async fn test_hot_partition_persist() {
        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            NamespaceId::new(3),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                NAMESPACE_NAME.clone()
            })),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            TRANSITION_SHARD_ID,
        );

        let mb = lp_to_mutable_batch(r#"potatoes,city=Hereford  people=1,crisps="good" 10"#).1;
        p.buffer_write(mb, SequenceNumber::new(1))
            .expect("write should succeed");
        let max_cost = p.persist_cost_estimate() + 1; // Require additional data to be buffered before enqueuing
        assert_eq!(p.completed_persistence_count(), 0);
        let p = Arc::new(Mutex::new(p));

        let metrics = metric::Registry::default();
        let persist_handle = Arc::new(MockPersistQueue::default());

        let hot_partition_persister =
            HotPartitionPersister::new(Arc::clone(&persist_handle), max_cost, &metrics);

        // Observe the partition after the first write
        hot_partition_persister.observe(Arc::clone(&p), p.lock());

        // Yield to allow the enqueue task to run
        tokio::task::yield_now().await;
        // Assert no persist calls were made
        assert_eq!(persist_handle.calls().len(), 0);

        metric::assert_counter!(
            metrics,
            metric::U64Counter,
            "ingester_persist_hot_partition_enqueue_count",
            value = 0,
        );

        // Write more data to the partition
        let want_query_data = {
            let mb = lp_to_mutable_batch(r#"potatoes,city=Worcester people=2,crisps="fine" 5"#).1;
            let mut guard = p.lock();
            guard
                .buffer_write(mb, SequenceNumber::new(2))
                .expect("write should succeed");
            guard.get_query_data().expect("should have query adaptor")
        };

        hot_partition_persister.observe(Arc::clone(&p), p.lock());

        tokio::task::yield_now().await;
        // Assert the partition was queued for persistence with the correct data.
        assert_matches!(persist_handle.calls().as_slice(), [got] => {
            let got_query_data = got.lock().get_query_data().expect("should have query adaptor");
            assert_eq!(got_query_data.record_batches(), want_query_data.record_batches());
        });

        metric::assert_counter!(
            metrics,
            metric::U64Counter,
            "ingester_persist_hot_partition_enqueue_count",
            value = 1,
        );

        // Check persist completion.
        drop(hot_partition_persister);
        Arc::try_unwrap(persist_handle)
            .expect("should be no more refs")
            .join()
            .await;
        assert_eq!(p.lock().completed_persistence_count(), 1);
    }
}
