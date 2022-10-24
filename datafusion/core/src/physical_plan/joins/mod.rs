//! DataFusion Join implementations

mod cross_join;
mod hash_join;
mod sort_merge_join;
pub mod utils;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Partitioning mode to use for hash join
pub enum PartitionMode {
    /// Left/right children are partitioned using the left and right keys
    Partitioned,
    /// Left side will collected into one partition
    CollectLeft,
}

pub use cross_join::CrossJoinExec;
pub use hash_join::HashJoinExec;

// Note: SortMergeJoin is not used in plans yet
pub use sort_merge_join::SortMergeJoinExec;
