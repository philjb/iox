use std::{fmt::Display, sync::Mutex};

use async_trait::async_trait;
use data_types::ObjectStorePathPartitionId;

use super::PartitionsSource;

#[derive(Debug)]
pub struct MockPartitionsSource {
    partitions: Mutex<Vec<ObjectStorePathPartitionId>>,
}

impl MockPartitionsSource {
    pub fn new(partitions: Vec<ObjectStorePathPartitionId>) -> Self {
        Self {
            partitions: Mutex::new(partitions),
        }
    }

    #[allow(dead_code)] // not used anywhere
    pub fn set(&self, partitions: Vec<ObjectStorePathPartitionId>) {
        *self.partitions.lock().expect("not poisoned") = partitions;
    }
}

impl Display for MockPartitionsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}

#[async_trait]
impl PartitionsSource for MockPartitionsSource {
    async fn fetch(&self) -> Vec<ObjectStorePathPartitionId> {
        self.partitions.lock().expect("not poisoned").clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(MockPartitionsSource::new(vec![]).to_string(), "mock",);
    }

    #[tokio::test]
    async fn test_fetch() {
        let source = MockPartitionsSource::new(vec![]);
        assert_eq!(source.fetch().await, vec![],);

        let p_1 = ObjectStorePathPartitionId::new(5);
        let p_2 = ObjectStorePathPartitionId::new(1);
        let p_3 = ObjectStorePathPartitionId::new(12);
        let parts = vec![p_1, p_2, p_3];
        source.set(parts.clone());
        assert_eq!(source.fetch().await, parts,);
    }
}
