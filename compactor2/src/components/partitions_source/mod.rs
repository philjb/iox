use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::ObjectStorePathPartitionId;

pub mod catalog_all;
pub mod catalog_to_compact;
pub mod filter;
pub mod logging;
pub mod metrics;
pub mod mock;
pub mod not_empty;
pub mod randomize_order;

/// A source of [catalog partition identifiers](ObjectStorePathPartitionId) for partitions that may
/// potentially need compacting.
#[async_trait]
pub trait PartitionsSource: Debug + Display + Send + Sync {
    /// Get catalog partition identifiers.
    ///
    /// This method performs retries.
    ///
    /// This should only perform basic, efficient filtering. It MUST NOT inspect individual parquet
    /// files.
    async fn fetch(&self) -> Vec<ObjectStorePathPartitionId>;
}

#[async_trait]
impl<T> PartitionsSource for Arc<T>
where
    T: PartitionsSource + ?Sized,
{
    async fn fetch(&self) -> Vec<ObjectStorePathPartitionId> {
        self.as_ref().fetch().await
    }
}
