// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Metric [`Registry`] and [`Snapshot`] implementations.

use super::Metric;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock};

#[derive(Debug, Default)]
pub(super) struct Registry {
    metrics: Vec<Arc<Metric>>,
    // Readers index each registration once, avoiding repeated full scans when
    // reporting successive partitions. Registration itself only appends to metrics.
    index: PartitionIndex,
}

#[derive(Debug, Default)]
struct PartitionIndex {
    // Number of registry entries already examined, including global metrics.
    indexed: usize,
    // Partition ID -> positions in Registry::metrics, in registration order.
    // Positions also let older snapshots exclude later registrations. Storing
    // handles here would duplicate Arc ownership and still require these positions.
    positions: HashMap<usize, Vec<usize>>,
}

impl Registry {
    pub(super) fn new(metrics: Vec<Arc<Metric>>) -> Self {
        Self {
            metrics,
            index: PartitionIndex::default(),
        }
    }

    pub(super) fn register(&mut self, metric: Arc<Metric>) {
        self.metrics.push(metric);
    }

    /// Select metrics whose partition ID equals `partition`. `end` is an exclusive
    /// position in the full registration vector, including other partitions and
    /// global metrics. It fixes the membership of the requesting snapshot.
    fn select(&mut self, partition: usize, end: usize) -> Vec<Arc<Metric>> {
        let index = &mut self.index;
        for position in index.indexed..end {
            if let Some(partition) = self.metrics[position].partition() {
                index.positions.entry(partition).or_default().push(position);
            }
        }
        index.indexed = index.indexed.max(end);
        let Some(positions) = index.positions.get(&partition) else {
            return Vec::new();
        };
        // Another snapshot may already have indexed registrations after our end.
        let len = positions.partition_point(|&position| position < end);
        positions[..len]
            .iter()
            .map(|&i| Arc::clone(&self.metrics[i]))
            .collect()
    }
}

#[derive(Clone)]
pub(super) enum Snapshot {
    /// All metrics registered before a fixed boundary; partition reads use the index.
    All(Arc<AllMetrics>),
    /// A selected partition, independent of the source registry.
    Partition {
        partition: usize,
        metrics: Vec<Arc<Metric>>,
    },
    /// An arbitrary collection created directly or detached by mutation. It has
    /// no registry/index and may contain multiple partitions and global metrics.
    Standalone(Vec<Arc<Metric>>),
}

pub(super) struct AllMetrics {
    registry: Arc<Mutex<Registry>>,
    // Exclusive position in Registry::metrics, not a per-partition count.
    end: usize,
    flat: OnceLock<Vec<Arc<Metric>>>,
}

impl Default for Snapshot {
    fn default() -> Self {
        Self::Standalone(Vec::new())
    }
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl AllMetrics {
    fn materialize(&self) -> Vec<Arc<Metric>> {
        self.registry.lock().metrics[..self.end].to_vec()
    }
}

impl Snapshot {
    pub(super) fn new(registry: Arc<Mutex<Registry>>) -> Self {
        let end = registry.lock().metrics.len();
        Self::All(Arc::new(AllMetrics {
            registry,
            end,
            flat: OnceLock::new(),
        }))
    }

    pub(super) fn for_partition(&self, partition: usize) -> Self {
        let metrics = match self {
            Self::All(snapshot) => {
                snapshot.registry.lock().select(partition, snapshot.end)
            }
            Self::Partition {
                partition: selected,
                metrics,
            } => {
                if *selected == partition {
                    metrics.clone()
                } else {
                    Vec::new()
                }
            }
            // Standalone collections have no index to consult. The indexed
            // All path above is used for snapshots from execution-plan registries.
            Self::Standalone(metrics) => metrics
                .iter()
                .filter(|m| m.partition() == Some(partition))
                .cloned()
                .collect(),
        };
        Self::Partition { partition, metrics }
    }

    pub(super) fn push(&mut self, metric: Arc<Metric>) {
        match self {
            Self::Standalone(metrics) => metrics.push(metric),
            Self::Partition { partition, metrics }
                if metric.partition() == Some(*partition) =>
            {
                metrics.push(metric);
            }
            _ => {
                // Detach before mutation: the registry and its index are unchanged.
                // A selected partition can become mixed when another ID is appended.
                let mut metrics: Vec<_> = std::mem::take(self).into_iter().collect();
                metrics.push(metric);
                *self = Self::Standalone(metrics);
            }
        }
    }

    pub(super) fn iter(&self) -> std::slice::Iter<'_, Arc<Metric>> {
        match self {
            Self::Standalone(metrics) | Self::Partition { metrics, .. } => metrics.iter(),
            Self::All(snapshot) => {
                snapshot.flat.get_or_init(|| snapshot.materialize()).iter()
            }
        }
    }
}

impl IntoIterator for Snapshot {
    type Item = Arc<Metric>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        let metrics = match self {
            Self::Standalone(metrics) | Self::Partition { metrics, .. } => metrics,
            Self::All(snapshot) => match Arc::try_unwrap(snapshot) {
                Ok(mut snapshot) => snapshot
                    .flat
                    .take()
                    .unwrap_or_else(|| snapshot.materialize()),
                Err(snapshot) => {
                    snapshot.flat.get_or_init(|| snapshot.materialize()).clone()
                }
            },
        };
        metrics.into_iter()
    }
}

impl<'a> IntoIterator for &'a Snapshot {
    type Item = &'a Arc<Metric>;
    type IntoIter = std::slice::Iter<'a, Arc<Metric>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Extend<Arc<Metric>> for Snapshot {
    fn extend<I: IntoIterator<Item = Arc<Metric>>>(&mut self, iter: I) {
        for metric in iter {
            self.push(metric);
        }
    }
}

impl FromIterator<Arc<Metric>> for Snapshot {
    fn from_iter<I: IntoIterator<Item = Arc<Metric>>>(iter: I) -> Self {
        Self::Standalone(iter.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{Count, ExecutionPlanMetricsSet, MetricValue};

    fn metric(partition: Option<usize>) -> Arc<Metric> {
        Arc::new(Metric::new(
            MetricValue::OutputRows(Count::new()),
            partition,
        ))
    }

    #[test]
    fn out_of_order_snapshots_preserve_membership_and_registration_order() {
        let registry = ExecutionPlanMetricsSet::new();
        let mut expected = Vec::new();
        let mut snapshots = Vec::new();
        for i in 0..1024 {
            let partition = match i % 5 {
                0 => None,
                1 => Some(usize::MAX),
                2 => Some(0),
                _ => Some(i * 17),
            };
            let metric = metric(partition);
            expected.push(Arc::clone(&metric));
            registry.register(metric);
            if i % 31 == 0 {
                snapshots.push((registry.clone_inner(), expected.len()));
            }
        }
        // Reading full snapshots must not build the partition index.
        assert_eq!(registry.clone_inner().iter().count(), 1024);
        assert_eq!(registry.inner.lock().index.indexed, 0);
        assert_eq!(registry.inner.lock().index.positions.capacity(), 0);
        // Advance the shared index before reading older, unmaterialized snapshots.
        registry.clone_inner().for_partition(0);
        for (snapshot, len) in snapshots.into_iter().rev() {
            for partition in [0, 17, 51, 999999, usize::MAX] {
                let selected = snapshot.for_partition(partition);
                let expected: Vec<_> = expected[..len]
                    .iter()
                    .filter(|m| m.partition() == Some(partition))
                    .collect();
                assert_eq!(selected.iter().count(), expected.len());
                for (actual, expected) in selected.iter().zip(expected) {
                    assert!(Arc::ptr_eq(actual, expected));
                }
            }
            assert_eq!(snapshot.iter().count(), len);
            for (actual, expected) in snapshot.iter().zip(&expected[..len]) {
                assert!(Arc::ptr_eq(actual, expected));
            }
        }
    }

    #[test]
    fn registration_leaves_index_and_retained_snapshots_unchanged() {
        let registry = ExecutionPlanMetricsSet::new();
        registry.register(metric(Some(0)));
        let empty_partition = registry.clone_inner();
        let old = registry.clone_inner();
        assert_eq!(old.for_partition(0).iter().count(), 1);
        registry.register(metric(Some(0)));
        registry.register(metric(Some(1)));
        {
            let registry = registry.inner.lock();
            let index = &registry.index;
            assert_eq!(index.indexed, 1);
            assert_eq!(index.positions[&0], vec![0]);
            assert!(!index.positions.contains_key(&1));
        }
        assert_eq!(registry.clone_inner().for_partition(0).iter().count(), 2);
        assert_eq!(registry.clone_inner().for_partition(1).iter().count(), 1);
        assert_eq!(empty_partition.for_partition(1).iter().count(), 0);
        assert_eq!(old.for_partition(0).iter().count(), 1);
        assert_eq!(old.iter().count(), 1);
        let registry = registry.inner.lock();
        let index = &registry.index;
        assert_eq!(index.indexed, 3);
        assert_eq!(index.positions[&0], vec![0, 1]);
    }

    #[test]
    fn selected_result_does_not_retain_registry() {
        let registry = ExecutionPlanMetricsSet::new();
        registry.register(metric(Some(0)));
        registry.register(metric(Some(1)));
        let weak = Arc::downgrade(&registry.inner);
        let snapshot = registry.clone_inner();
        let selected = snapshot.for_partition(0);
        drop(registry);
        assert!(weak.upgrade().is_some());
        drop(snapshot);
        assert!(weak.upgrade().is_none());
        assert_eq!(selected.iter().count(), 1);
    }

    #[test]
    fn selected_snapshot_handles_same_other_and_global_additions() {
        let registry = ExecutionPlanMetricsSet::new();
        registry.register(metric(Some(7)));
        let original = registry.clone_inner().for_partition(7);
        let mut same = original.clone();
        same.push(metric(Some(7)));
        assert_eq!(same.for_partition(7).iter().count(), 2);
        assert_eq!(same.for_partition(9).iter().count(), 0);
        assert_eq!(original.iter().count(), 1);
        same.push(metric(None));
        assert_eq!(same.iter().count(), 3);
        assert_eq!(same.for_partition(7).iter().count(), 2);
        same.push(metric(Some(9)));
        assert_eq!(same.for_partition(9).iter().count(), 1);
        assert_eq!(registry.clone_inner().iter().count(), 1);
        let empty = original.for_partition(9);
        assert_eq!(empty.for_partition(7).iter().count(), 0);
    }

    #[test]
    fn standalone_selection_preserves_duplicates_order_and_globals() {
        let first = metric(Some(usize::MAX));
        let global = metric(None);
        let other = metric(Some(3));
        let metrics: Snapshot = [Arc::clone(&first), global, other, Arc::clone(&first)]
            .into_iter()
            .collect();
        let selected = metrics.for_partition(usize::MAX);
        assert_eq!(selected.iter().count(), 2);
        assert!(selected.iter().all(|m| Arc::ptr_eq(m, &first)));
        assert_eq!(metrics.iter().count(), 4);
        assert_eq!(metrics.for_partition(3).iter().count(), 1);
        assert_eq!(metrics.for_partition(0).iter().count(), 0);
    }

    #[test]
    fn mutating_full_snapshot_detaches_from_registry() {
        let registry = ExecutionPlanMetricsSet::new();
        registry.register(metric(Some(0)));
        let original = registry.clone_inner();
        assert_eq!(original.for_partition(0).iter().count(), 1);
        let mut modified = original.clone();
        modified.push(metric(Some(1)));
        registry.register(metric(Some(2)));
        assert_eq!(original.iter().count(), 1);
        assert_eq!(modified.iter().count(), 2);
        assert_eq!(modified.for_partition(1).iter().count(), 1);
        assert_eq!(modified.for_partition(2).iter().count(), 0);
        assert_eq!(registry.clone_inner().for_partition(1).iter().count(), 0);
        assert_eq!(registry.clone_inner().for_partition(2).iter().count(), 1);
    }
}
