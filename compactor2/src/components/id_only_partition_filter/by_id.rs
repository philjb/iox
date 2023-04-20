use std::{collections::HashSet, fmt::Display};

use data_types::ObjectStorePathPartitionId;

use super::IdOnlyPartitionFilter;

#[derive(Debug)]
pub struct ByIdPartitionFilter {
    ids: HashSet<ObjectStorePathPartitionId>,
}

impl ByIdPartitionFilter {
    #[allow(dead_code)] // not used anywhere
    pub fn new(ids: HashSet<ObjectStorePathPartitionId>) -> Self {
        Self { ids }
    }
}

impl Display for ByIdPartitionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "by_id")
    }
}

impl IdOnlyPartitionFilter for ByIdPartitionFilter {
    fn apply(&self, partition_id: ObjectStorePathPartitionId) -> bool {
        self.ids.contains(&partition_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            ByIdPartitionFilter::new(HashSet::default()).to_string(),
            "by_id"
        );
    }

    #[test]
    fn test_apply() {
        let filter = ByIdPartitionFilter::new(HashSet::from([
            ObjectStorePathPartitionId::new(1),
            ObjectStorePathPartitionId::new(10),
        ]));

        assert!(filter.apply(ObjectStorePathPartitionId::new(1)));
        assert!(filter.apply(ObjectStorePathPartitionId::new(10)));
        assert!(!filter.apply(ObjectStorePathPartitionId::new(2)));
    }
}
