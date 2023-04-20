//! The per-partition data nested in a query [`QueryResponse`].
//!
//! [`QueryResponse`]: super::response::QueryResponse

use data_types::ObjectStorePathPartitionId;
use datafusion::physical_plan::SendableRecordBatchStream;

/// Response data for a single partition.
pub(crate) struct PartitionResponse {
    /// Stream of snapshots.
    batches: Option<SendableRecordBatchStream>,

    /// Partition ID.
    id: ObjectStorePathPartitionId,

    /// Count of persisted Parquet files for this partition by this ingester instance.
    completed_persistence_count: u64,
}

impl std::fmt::Debug for PartitionResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionResponse")
            .field(
                "batches",
                &match self.batches {
                    Some(_) => "<SNAPSHOT STREAM>",
                    None => "<NO DATA>,",
                },
            )
            .field("partition_id", &self.id)
            .field(
                "completed_persistence_count",
                &self.completed_persistence_count,
            )
            .finish()
    }
}

impl PartitionResponse {
    pub(crate) fn new(
        data: Option<SendableRecordBatchStream>,
        id: ObjectStorePathPartitionId,
        completed_persistence_count: u64,
    ) -> Self {
        Self {
            batches: data,
            id,
            completed_persistence_count,
        }
    }

    pub(crate) fn id(&self) -> ObjectStorePathPartitionId {
        self.id
    }

    pub(crate) fn completed_persistence_count(&self) -> u64 {
        self.completed_persistence_count
    }

    pub(crate) fn into_record_batch_stream(self) -> Option<SendableRecordBatchStream> {
        self.batches
    }
}
