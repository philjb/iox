//! The response type returned from a query [`QueryExec::query_exec()`] call.
//!
//! [`QueryExec::query_exec()`]: super::QueryExec::query_exec()

use std::{pin::Pin, sync::Arc};

use arrow::{error::ArrowError, record_batch::RecordBatch};
use arrow_util::optimize::{optimize_record_batch, optimize_schema};
use futures::{Stream, StreamExt, TryStreamExt};

use super::partition_response::PartitionResponse;

/// Stream of partitions in this response.
pub(crate) type PartitionStream =
    Pin<Box<dyn Stream<Item = Result<PartitionResponse, ArrowError>> + Send>>;

/// A response stream wrapper for ingester query requests.
///
/// The data structure is constructed to allow lazy/streaming/pull-based data
/// sourcing..
pub(crate) struct QueryResponse {
    /// Stream of partitions.
    partitions: PartitionStream,
}

impl std::fmt::Debug for QueryResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Response")
            .field("partitions", &"<PARTITION STREAM>")
            .finish()
    }
}

impl QueryResponse {
    /// Make a response
    pub(crate) fn new(partitions: PartitionStream) -> Self {
        Self { partitions }
    }

    /// Return the stream of [`PartitionResponse`].
    pub(crate) fn into_partition_stream(self) -> PartitionStream {
        self.partitions
    }

    /// Reduce the [`QueryResponse`] to a set of [`RecordBatch`].
    pub(crate) async fn into_record_batches(self) -> Result<Vec<RecordBatch>, ArrowError> {
        self.into_partition_stream()
            .map_ok(|partition| {
                partition
                    .into_record_batch_stream()
                    .map_ok(|snapshot| {
                        let schema = Arc::new(optimize_schema(&snapshot.schema()));
                        snapshot
                            .map(move |batch| optimize_record_batch(&batch?, Arc::clone(&schema)))
                    })
                    .try_flatten()
            })
            .try_flatten()
            .try_collect()
            .await
    }
}