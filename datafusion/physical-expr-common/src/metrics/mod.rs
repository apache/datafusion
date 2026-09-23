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

//! Metrics for recording information about execution

mod baseline;
mod builder;
mod custom;
mod elapsed_compute;
mod expression;
mod snapshot;
mod value;

use datafusion_common::HashMap;
pub use datafusion_common::format::{MetricCategory, MetricType};
use datafusion_common::human_readable_size;
use parking_lot::Mutex;
use snapshot::{Registry, Snapshot};
use std::{
    borrow::Cow,
    fmt::{self, Debug, Display},
    hash::{Hash, Hasher},
    sync::Arc,
    vec::IntoIter,
};

// public exports

pub use baseline::{BaselineMetrics, RecordOutput, SpillMetrics, SplitMetrics};
pub use builder::MetricBuilder;
pub use custom::CustomMetricValue;
pub use elapsed_compute::{ElapsedComputeFuture, ElapsedComputeFutureExt};
pub use expression::ExpressionEvaluatorMetrics;
pub use value::{
    Count, Gauge, MetricValue, PruningMetrics, RatioMergeStrategy, RatioMetrics,
    ScopedTimerGuard, Time, Timestamp,
};

/// Something that tracks a value of interest (metric) during execution.
///
/// Typically [`Metric`]s are not created directly, but instead
/// are created using [`MetricBuilder`] or methods on
/// [`ExecutionPlanMetricsSet`].
///
/// ```
/// use datafusion_physical_expr_common::metrics::*;
///
/// let metrics = ExecutionPlanMetricsSet::new();
/// assert!(metrics.clone_inner().output_rows().is_none());
///
/// // Create a counter to increment using the MetricBuilder
/// let partition = 1;
/// let output_rows = MetricBuilder::new(&metrics).output_rows(partition);
///
/// // Counter can be incremented
/// output_rows.add(13);
///
/// // The value can be retrieved directly:
/// assert_eq!(output_rows.value(), 13);
///
/// // As well as from the metrics set
/// assert_eq!(metrics.clone_inner().output_rows(), Some(13));
/// ```

#[derive(Debug)]
pub struct Metric {
    /// The value of the metric
    value: MetricValue,

    /// arbitrary name=value pairs identifying this metric
    labels: Vec<Label>,

    /// To which partition of an operators output did this metric
    /// apply? If `None` then means all partitions.
    partition: Option<usize>,

    metric_type: MetricType,

    /// Optional semantic category (rows / bytes / timing).
    ///
    /// When `None` (the default for custom metrics), the metric is
    /// **always included** unless the user sets
    /// `analyze_categories = 'none'`.
    metric_category: Option<MetricCategory>,
}

impl Display for Metric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value.name())?;

        let mut iter = self
            .partition
            .iter()
            .map(|partition| Label::new("partition", partition.to_string()))
            .chain(self.labels().iter().cloned())
            .peekable();

        // print out the labels specially
        if iter.peek().is_some() {
            write!(f, "{{")?;

            let mut is_first = true;
            for i in iter {
                if !is_first {
                    write!(f, ", ")?;
                } else {
                    is_first = false;
                }

                write!(f, "{i}")?;
            }

            write!(f, "}}")?;
        }

        // and now the value
        write!(f, "=")?;

        if self.metric_category == Some(MetricCategory::Bytes) {
            match &self.value {
                MetricValue::Count { count, .. } => {
                    return write!(f, "{}", human_readable_size(count.value()));
                }
                MetricValue::Gauge { gauge, .. } => {
                    return write!(f, "{}", human_readable_size(gauge.value()));
                }
                _ => {}
            }
        }

        write!(f, "{}", self.value)
    }
}

impl Metric {
    /// Create a new [`Metric`]. Consider using [`MetricBuilder`]
    /// rather than this function directly.
    pub fn new(value: MetricValue, partition: Option<usize>) -> Self {
        Self {
            value,
            labels: vec![],
            partition,
            metric_type: MetricType::Dev,
            metric_category: None,
        }
    }

    /// Create a new [`Metric`]. Consider using [`MetricBuilder`]
    /// rather than this function directly.
    pub fn new_with_labels(
        value: MetricValue,
        partition: Option<usize>,
        labels: Vec<Label>,
    ) -> Self {
        Self {
            value,
            labels,
            partition,
            metric_type: MetricType::Dev,
            metric_category: None,
        }
    }

    /// Set the type for this metric. Defaults to [`MetricType::Dev`]
    pub fn with_type(mut self, metric_type: MetricType) -> Self {
        self.metric_type = metric_type;
        self
    }

    /// Set the semantic category for this metric.
    ///
    /// See [`MetricCategory`] for details on the determinism properties
    /// of each category.
    pub fn with_category(mut self, category: MetricCategory) -> Self {
        self.metric_category = Some(category);
        self
    }

    /// Add a new label to this metric
    pub fn with_label(mut self, label: Label) -> Self {
        self.labels.push(label);
        self
    }

    /// What labels are present for this metric?
    pub fn labels(&self) -> &[Label] {
        &self.labels
    }

    /// Return a reference to the value of this metric
    pub fn value(&self) -> &MetricValue {
        &self.value
    }

    /// Return a mutable reference to the value of this metric
    pub fn value_mut(&mut self) -> &mut MetricValue {
        &mut self.value
    }

    /// Return a reference to the partition
    pub fn partition(&self) -> Option<usize> {
        self.partition
    }

    /// Return the metric type (verbosity level) associated with this metric
    pub fn metric_type(&self) -> MetricType {
        self.metric_type
    }

    /// Return the metric category, if one was declared.
    ///
    /// `None` means the metric is always included (except in `none` mode).
    pub fn metric_category(&self) -> Option<MetricCategory> {
        self.metric_category
    }
}

/// A snapshot of the metrics for a particular execution plan.
///
/// Snapshots returned by [`ExecutionPlanMetricsSet::clone_inner`] record a fixed
/// registration boundary and defer copying metric handles until iteration. Their
/// membership is fixed while metric values remain live. Cloning these snapshots
/// is O(1); cloning an owned set copies its handles.
///
/// A registry-backed snapshot retains its source registry, including later
/// registrations. Full iteration caches its original members in O(n) time;
/// [`Self::for_partition`] can instead use the registry's incremental index.
#[derive(Default, Debug, Clone)]
pub struct MetricsSet {
    metrics: Snapshot,
}

impl MetricsSet {
    /// Create a new container of metrics
    pub fn new() -> Self {
        Default::default()
    }

    /// Add the specified metric.
    ///
    /// Mutating a deferred snapshot first copies its members into an independent
    /// vector. Subsequent additions append to that vector.
    pub fn push(&mut self, metric: Arc<Metric>) {
        self.metrics.push(metric)
    }

    /// Return a snapshot containing only metrics with `partition == Some(partition)`.
    ///
    /// For registry-backed snapshots, this incrementally indexes registrations
    /// not processed by earlier partition reads, then clones only matching handles.
    /// Each registration is indexed at most once across snapshots of the registry.
    /// Owned sets instead filter their handles in O(n) time.
    ///
    /// Unpartitioned metrics are excluded; an unknown partition yields an empty set.
    /// Registration order, duplicates and shared metric values are preserved. The
    /// result owns its handles and does not retain the source registry. It never
    /// acquires metrics registered after this snapshot was created.
    ///
    /// Index updates and selection hold the registry lock and can delay concurrent
    /// registration. This does not avoid work already performed by the provider,
    /// such as full metrics transport over FFI.
    pub fn for_partition(&self, partition: usize) -> Self {
        Self {
            metrics: self.metrics.for_partition(partition),
        }
    }

    /// Returns an iterator across all metrics
    pub fn iter(&self) -> impl Iterator<Item = &Arc<Metric>> {
        self.metrics.iter()
    }

    /// Convenience: return the number of rows produced, aggregated
    /// across partitions or `None` if no metric is present
    pub fn output_rows(&self) -> Option<usize> {
        self.sum(|metric| matches!(metric.value(), MetricValue::OutputRows(_)))
            .map(|v| v.as_usize())
    }

    /// Convenience: return the count of spills, aggregated
    /// across partitions or `None` if no metric is present
    pub fn spill_count(&self) -> Option<usize> {
        self.sum(|metric| matches!(metric.value(), MetricValue::SpillCount(_)))
            .map(|v| v.as_usize())
    }

    /// Convenience: return the total byte size of spills, aggregated
    /// across partitions or `None` if no metric is present
    pub fn spilled_bytes(&self) -> Option<usize> {
        self.sum(|metric| matches!(metric.value(), MetricValue::SpilledBytes(_)))
            .map(|v| v.as_usize())
    }

    /// Convenience: return the total rows of spills, aggregated
    /// across partitions or `None` if no metric is present
    pub fn spilled_rows(&self) -> Option<usize> {
        self.sum(|metric| matches!(metric.value(), MetricValue::SpilledRows(_)))
            .map(|v| v.as_usize())
    }

    /// Convenience: return the amount of elapsed CPU time spent,
    /// aggregated across partitions or `None` if no metric is present
    pub fn elapsed_compute(&self) -> Option<usize> {
        self.sum(|metric| matches!(metric.value(), MetricValue::ElapsedCompute(_)))
            .map(|v| v.as_usize())
    }

    /// Sums the values for metrics for which `f(metric)` returns
    /// `true`, and returns the value. Returns `None` if no metrics match
    /// the predicate.
    pub fn sum<F>(&self, mut f: F) -> Option<MetricValue>
    where
        F: FnMut(&Metric) -> bool,
    {
        let mut iter = self
            .metrics
            .iter()
            .filter(|metric| f(metric.as_ref()))
            .peekable();

        let mut accum = match iter.peek() {
            None => {
                return None;
            }
            Some(metric) => metric.value().new_empty(),
        };

        iter.for_each(|metric| accum.aggregate(metric.value()));

        Some(accum)
    }

    /// Returns the sum of all the metrics with the specified name
    /// in the returned set.
    pub fn sum_by_name(&self, metric_name: &str) -> Option<MetricValue> {
        self.sum(|m| match m.value() {
            MetricValue::Count { name, .. } => name == metric_name,
            MetricValue::Time { name, .. } => name == metric_name,
            MetricValue::OutputRows(_) => false,
            MetricValue::ElapsedCompute(_) => false,
            MetricValue::SpillCount(_) => false,
            MetricValue::SpilledBytes(_) => false,
            MetricValue::OutputBytes(_) => false,
            MetricValue::OutputBatches(_) => false,
            MetricValue::SpilledRows(_) => false,
            MetricValue::CurrentMemoryUsage(_) => false,
            MetricValue::Gauge { name, .. } => name == metric_name,
            MetricValue::PeakMemoryUsage { name, .. } => name == metric_name,
            MetricValue::StartTimestamp(_) => false,
            MetricValue::EndTimestamp(_) => false,
            MetricValue::PruningMetrics { name, .. } => name == metric_name,
            MetricValue::Ratio { name, .. } => name == metric_name,
            MetricValue::Custom { name, .. } => name == metric_name,
        })
    }

    /// Returns a new derived `MetricsSet` where all metrics
    /// that had the same name have been
    /// aggregated together. The resulting `MetricsSet` has all
    /// metrics with `Partition=None`
    pub fn aggregate_by_name(&self) -> Self {
        let mut map = HashMap::new();

        // There are all sorts of ways to make this more efficient
        for metric in &self.metrics {
            let key = metric.value.name();
            map.entry(key)
                .and_modify(|accum: &mut Metric| {
                    accum.value_mut().aggregate(metric.value());
                })
                .or_insert_with(|| {
                    // accumulate with no partition
                    let partition = None;
                    let mut accum = Metric::new(metric.value().new_empty(), partition)
                        .with_type(metric.metric_type());
                    if let Some(cat) = metric.metric_category() {
                        accum = accum.with_category(cat);
                    }
                    accum.value_mut().aggregate(metric.value());
                    accum
                });
        }

        let new_metrics = map.into_iter().map(|(_k, v)| Arc::new(v)).collect();

        Self {
            metrics: new_metrics,
        }
    }

    /// Sort the order of metrics so the "most useful" show up first
    pub fn sorted_for_display(self) -> Self {
        let mut metrics: Vec<_> = self.into_iter().collect();
        metrics.sort_unstable_by_key(|metric| {
            (
                metric.value().display_sort_key(),
                metric.value().name().to_owned(),
            )
        });
        metrics.into_iter().collect()
    }

    /// Remove all timestamp metrics (for more compact display)
    pub fn timestamps_removed(self) -> Self {
        let Self { metrics } = self;

        let metrics = metrics
            .into_iter()
            .filter(|m| !m.value.is_timestamp())
            .collect();

        Self { metrics }
    }

    /// Returns a new derived `MetricsSet` containing only metrics whose
    /// [`MetricType`] appears in `allowed`.
    pub fn filter_by_metric_types(self, allowed: &[MetricType]) -> Self {
        if allowed.is_empty() {
            return Self::new();
        }

        let metrics = self
            .metrics
            .into_iter()
            .filter(|metric| allowed.contains(&metric.metric_type()))
            .collect();
        Self { metrics }
    }

    /// Returns a new `MetricsSet` filtered by [`MetricCategory`].
    ///
    /// - Metrics that declared a category are kept only when that
    ///   category appears in `allowed`.
    /// - Metrics with **no** declared category are treated as
    ///   [`Uncategorized`](MetricCategory::Uncategorized) for filtering.
    /// - An **empty** `allowed` slice means "plan only": all metrics are
    ///   removed.
    pub fn filter_by_categories(self, allowed: &[MetricCategory]) -> Self {
        if allowed.is_empty() {
            return Self::new();
        }

        let metrics = self
            .metrics
            .into_iter()
            .filter(|metric| {
                let cat = metric
                    .metric_category()
                    .unwrap_or(MetricCategory::Uncategorized);
                allowed.contains(&cat)
            })
            .collect();
        Self { metrics }
    }

    /// Returns a new `MetricsSet` filtered by metric name.
    /// Only metrics with the names appearing the list will be kept.
    pub fn filter_by_names(self, names: &[String]) -> Self {
        if names.is_empty() {
            return Self::new();
        }

        let metrics = self
            .metrics
            .into_iter()
            .filter(|metric| names.iter().any(|name| name == metric.value().name()))
            .collect();
        Self { metrics }
    }
}

impl Display for MetricsSet {
    /// Format the [`MetricsSet`] as a single string
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut is_first = true;
        for i in self.metrics.iter() {
            if !is_first {
                write!(f, ", ")?;
            } else {
                is_first = false;
            }

            write!(f, "{i}")?;
        }
        Ok(())
    }
}

impl IntoIterator for MetricsSet {
    type Item = Arc<Metric>;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.metrics.into_iter()
    }
}

impl<'a> IntoIterator for &'a MetricsSet {
    type Item = &'a Arc<Metric>;
    type IntoIter = std::slice::Iter<'a, Arc<Metric>>;

    fn into_iter(self) -> Self::IntoIter {
        self.metrics.iter()
    }
}

impl Extend<Arc<Metric>> for MetricsSet {
    fn extend<I: IntoIterator<Item = Arc<Metric>>>(&mut self, iter: I) {
        self.metrics.extend(iter);
    }
}

impl FromIterator<Arc<Metric>> for MetricsSet {
    fn from_iter<T: IntoIterator<Item = Arc<Metric>>>(iter: T) -> Self {
        Self {
            metrics: iter.into_iter().collect(),
        }
    }
}

/// A set of [`Metric`]s for an individual operator.
///
/// This structure is intended as a convenience for execution plan
/// implementations so they can generate different streams for multiple
/// partitions but easily report them together.
///
/// Each `clone()` of this structure will add metrics to the same
/// underlying metrics set
#[derive(Default, Debug, Clone)]
pub struct ExecutionPlanMetricsSet {
    inner: Arc<Mutex<Registry>>,
}

impl ExecutionPlanMetricsSet {
    /// Create a new empty shared metrics set
    pub fn new() -> Self {
        Self::default()
    }

    /// Add the specified metric to the underlying metric set
    pub fn register(&self, metric: Arc<Metric>) {
        self.inner.lock().metrics.push(metric)
    }

    /// Return a snapshot with the current registration boundary in O(1).
    ///
    /// Metric handles are copied on iteration or partition selection. Later
    /// registrations leave this snapshot's membership unchanged, while values
    /// remain shared. Retaining a snapshot also retains the source registry.
    pub fn clone_inner(&self) -> MetricsSet {
        let end = self.inner.lock().metrics.len();
        MetricsSet {
            metrics: Snapshot::new(Arc::clone(&self.inner), end),
        }
    }
}

impl From<MetricsSet> for ExecutionPlanMetricsSet {
    fn from(metrics: MetricsSet) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Registry::new(metrics.into_iter().collect()))),
        }
    }
}

/// `name=value` pairs identifying a metric. This concept is called various things
/// in various different systems:
///
/// "labels" in
/// [prometheus](https://prometheus.io/docs/concepts/data_model/) and
/// "tags" in
/// [InfluxDB](https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/)
/// , "attributes" in [open
/// telemetry]<https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md>,
/// etc.
///
/// As the name and value are expected to often be constant strings, borrowed
/// static strings avoid allocations in that common case. Dynamic strings are
/// stored behind [`Arc<str>`] so cloning labels does not copy the underlying
/// string data.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Label {
    name: LabelValue,
    value: LabelValue,
}

impl Label {
    /// Create a new [`Label`]
    pub fn new(name: impl Into<LabelValue>, value: impl Into<LabelValue>) -> Self {
        let name = name.into();
        let value = value.into();
        Self { name, value }
    }

    /// Returns the name of this label
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Returns the value of this label
    pub fn value(&self) -> &str {
        self.value.as_str()
    }
}

impl Display for Label {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}={}", self.name, self.value)
    }
}

/// A label name or value.
///
/// String literals preserve the existing allocation-free path. Dynamic strings
/// can be stored behind [`Arc<str>`], so cloning a [`Label`] only increments an
/// atomic reference count and does not allocate or copy the underlying string
/// data.
#[derive(Clone)]
pub struct LabelValue(LabelValueInner);

/// Internal representation for label names and values.
///
/// `LabelValue` is public because `Label::new` accepts it, but these storage
/// variants are implementation details. Keeping them private prevents external
/// code from constructing or matching on `Static` and `Shared` directly.
#[derive(Clone)]
enum LabelValueInner {
    Static(&'static str),
    Shared(Arc<str>),
}

impl LabelValue {
    /// Return this label value as a string slice.
    pub fn as_str(&self) -> &str {
        match &self.0 {
            LabelValueInner::Static(value) => value,
            LabelValueInner::Shared(value) => value.as_ref(),
        }
    }
}

impl From<&'static str> for LabelValue {
    fn from(value: &'static str) -> Self {
        Self(LabelValueInner::Static(value))
    }
}

impl From<String> for LabelValue {
    fn from(value: String) -> Self {
        Self(LabelValueInner::Shared(Arc::from(value)))
    }
}

impl From<Arc<str>> for LabelValue {
    fn from(value: Arc<str>) -> Self {
        Self(LabelValueInner::Shared(value))
    }
}

impl From<Cow<'static, str>> for LabelValue {
    fn from(value: Cow<'static, str>) -> Self {
        match value {
            Cow::Borrowed(value) => value.into(),
            Cow::Owned(value) => value.into(),
        }
    }
}

impl PartialEq for LabelValue {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for LabelValue {}

impl Hash for LabelValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl Debug for LabelValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self.as_str(), f)
    }
}

impl Display for LabelValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self.as_str(), f)
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use chrono::{TimeZone, Utc};

    use super::*;

    #[test]
    fn test_display_no_labels_no_partition() {
        let count = Count::new();
        count.add(33);
        let value = MetricValue::OutputRows(count);
        let partition = None;
        let metric = Metric::new(value, partition);

        assert_eq!("output_rows=33", metric.to_string())
    }

    #[test]
    fn test_display_no_labels_with_partition() {
        let count = Count::new();
        count.add(44);
        let value = MetricValue::OutputRows(count);
        let partition = Some(1);
        let metric = Metric::new(value, partition);

        assert_eq!("output_rows{partition=1}=44", metric.to_string())
    }

    #[test]
    fn test_display_labels_no_partition() {
        let count = Count::new();
        count.add(55);
        let value = MetricValue::OutputRows(count);
        let partition = None;
        let label = Label::new("foo", "bar");
        let metric = Metric::new_with_labels(value, partition, vec![label]);

        assert_eq!("output_rows{foo=bar}=55", metric.to_string())
    }

    #[test]
    fn test_display_labels_and_partition() {
        let count = Count::new();
        count.add(66);
        let value = MetricValue::OutputRows(count);
        let partition = Some(2);
        let label = Label::new("foo", "bar");
        let metric = Metric::new_with_labels(value, partition, vec![label]);

        assert_eq!("output_rows{partition=2, foo=bar}=66", metric.to_string())
    }

    #[test]
    fn test_label_owned_and_borrowed_values_are_equal() {
        let borrowed = Label::new("foo", "bar");
        let owned = Label::new("foo".to_string(), "bar".to_string());
        let shared = Label::new("foo", Arc::<str>::from("bar"));

        assert_eq!(borrowed, owned);
        assert_eq!(borrowed, shared);
        assert_eq!(borrowed.to_string(), owned.to_string());
        assert_eq!(borrowed.to_string(), shared.to_string());
    }

    #[test]
    fn selected_snapshot_mutations_are_independent() {
        let registry = ExecutionPlanMetricsSet::new();
        MetricBuilder::new(&registry).output_rows(0).add(3);
        MetricBuilder::new(&registry).output_rows(1).add(7);
        MetricBuilder::new(&registry)
            .global_counter("global")
            .add(11);
        let full = registry.clone_inner();
        let selected = full.for_partition(0);
        assert_eq!(selected.for_partition(0).output_rows(), Some(3));
        assert_eq!(selected.for_partition(1).iter().count(), 0);
        let mut modified = selected.clone();
        let count = Count::new();
        count.add(13);
        modified.push(Arc::new(Metric::new(
            MetricValue::OutputRows(count),
            Some(1),
        )));
        assert_eq!(modified.output_rows(), Some(16));
        assert_eq!(modified.for_partition(1).output_rows(), Some(13));
        assert_eq!(selected.output_rows(), Some(3));
        assert_eq!(full.output_rows(), Some(10));
        assert_eq!(registry.clone_inner().iter().count(), 3);
        assert_eq!(modified.clone().into_iter().count(), 2);
        assert_eq!(
            modified.sorted_for_display().for_partition(0).output_rows(),
            Some(3)
        );
    }

    #[test]
    fn partition_snapshots_preserve_registration_and_shared_values() {
        let metrics = ExecutionPlanMetricsSet::new();
        assert_eq!(metrics.clone_inner().for_partition(0).iter().count(), 0);
        let first = MetricBuilder::new(&metrics).output_rows(0);
        first.add(11);
        MetricBuilder::new(&metrics).global_counter("global").add(7);
        MetricBuilder::new(&metrics).output_rows(usize::MAX).add(99);
        // Same name and partition must not overwrite the earlier metric.
        MetricBuilder::new(&metrics).output_rows(0).add(13);
        let snapshot = metrics.clone_inner().for_partition(0);
        assert_eq!(snapshot.output_rows(), Some(24));
        assert_eq!(snapshot.iter().count(), 2);
        assert_eq!(snapshot.aggregate_by_name().output_rows(), Some(24));
        assert_eq!(
            metrics
                .clone_inner()
                .for_partition(usize::MAX)
                .output_rows(),
            Some(99)
        );
        assert_eq!(metrics.clone_inner().for_partition(1).iter().count(), 0);

        let shared = metrics.clone();
        first.add(1);
        MetricBuilder::new(&shared).output_rows(0).add(17);
        assert_eq!(snapshot.output_rows(), Some(25));
        assert_eq!(
            shared.clone_inner().for_partition(0).output_rows(),
            Some(42)
        );
        assert_eq!(
            metrics.clone_inner().for_partition(0).output_rows(),
            Some(42)
        );

        let full = metrics.clone_inner();
        let imported = ExecutionPlanMetricsSet::from(full.clone());
        for (original, copied) in full.iter().zip(imported.clone_inner().iter()) {
            assert!(Arc::ptr_eq(original, copied));
        }
        for partition in [0, 1, usize::MAX] {
            let expected: Vec<_> = full
                .iter()
                .filter(|metric| metric.partition() == Some(partition))
                .collect();
            let selected = imported.clone_inner().for_partition(partition);
            assert_eq!(expected.len(), selected.iter().count());
            for (original, copied) in expected.into_iter().zip(selected.iter()) {
                assert!(Arc::ptr_eq(original, copied));
            }
        }
        // From shares metric values, but creates an independent registration set.
        MetricBuilder::new(&imported).output_rows(0).add(3);
        assert_eq!(
            imported.clone_inner().for_partition(0).output_rows(),
            Some(45)
        );
        assert_eq!(
            metrics.clone_inner().for_partition(0).output_rows(),
            Some(42)
        );
        assert_eq!(full.iter().filter(|m| m.partition().is_none()).count(), 1);
    }

    #[test]
    fn partition_snapshots_during_registration() {
        let metrics = ExecutionPlanMetricsSet::new();
        let barrier = std::sync::Barrier::new(5);
        std::thread::scope(|scope| {
            for partition in 0..4 {
                let metrics = &metrics;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    for _ in 0..1000 {
                        MetricBuilder::new(metrics).output_rows(partition).add(1);
                    }
                });
            }
            barrier.wait();
            for _ in 0..1000 {
                for partition in 0..4 {
                    let selected = metrics.clone_inner().for_partition(partition);
                    assert!(selected.iter().all(|m| m.partition() == Some(partition)));
                    assert!(selected.iter().count() <= 1000);
                }
            }
        });
        for partition in 0..4 {
            let selected = metrics.clone_inner().for_partition(partition);
            assert_eq!(selected.iter().count(), 1000);
            assert_eq!(selected.output_rows(), Some(1000));
        }
        assert_eq!(metrics.clone_inner().output_rows(), Some(4000));
    }

    #[test]
    fn test_output_rows() {
        let metrics = ExecutionPlanMetricsSet::new();
        assert!(metrics.clone_inner().output_rows().is_none());

        let partition = 1;
        let output_rows = MetricBuilder::new(&metrics).output_rows(partition);
        output_rows.add(13);

        let output_rows = MetricBuilder::new(&metrics).output_rows(partition + 1);
        output_rows.add(7);
        assert_eq!(metrics.clone_inner().output_rows().unwrap(), 20);
    }

    #[test]
    fn test_elapsed_compute() {
        let metrics = ExecutionPlanMetricsSet::new();
        assert!(metrics.clone_inner().elapsed_compute().is_none());

        let partition = 1;
        let elapsed_compute = MetricBuilder::new(&metrics).elapsed_compute(partition);
        elapsed_compute.add_duration(Duration::from_nanos(1234));

        let elapsed_compute = MetricBuilder::new(&metrics).elapsed_compute(partition + 1);
        elapsed_compute.add_duration(Duration::from_nanos(6));
        assert_eq!(metrics.clone_inner().elapsed_compute().unwrap(), 1240);
    }

    #[test]
    fn test_sum() {
        let metrics = ExecutionPlanMetricsSet::new();

        let count1 = MetricBuilder::new(&metrics)
            .with_new_label("foo", "bar")
            .counter("my_counter", 1);
        count1.add(1);

        let count2 = MetricBuilder::new(&metrics).counter("my_counter", 2);
        count2.add(2);

        let metrics = metrics.clone_inner();
        assert!(metrics.sum(|_| false).is_none());

        let expected_count = Count::new();
        expected_count.add(3);
        let expected_sum = MetricValue::Count {
            name: "my_counter".into(),
            count: expected_count,
        };

        assert_eq!(metrics.sum(|_| true), Some(expected_sum));
    }

    #[test]
    fn test_bytes_counter_and_gauge_use_byte_units() {
        let metrics = ExecutionPlanMetricsSet::new();

        // A dedicated byte counter/gauge (like `bytes_scanned` or
        // `stream_memory_usage`) must render with human_readable_size's
        // 1024-based units (KB/MB/GB), not human_readable_count's 1000-based
        // units (K/M/B) - see #24203. 3 GiB, chosen to clear
        // human_readable_size's >= 2x-tier threshold for GB (below that it
        // falls back to a large MB value).
        let three_gib = 3 * 1024 * 1024 * 1024;
        let bytes_scanned =
            MetricBuilder::new(&metrics).bytes_counter("bytes_scanned", 0);
        bytes_scanned.add(three_gib);

        let stream_memory_usage =
            MetricBuilder::new(&metrics).bytes_gauge("stream_memory_usage", 0);
        stream_memory_usage.add(three_gib);

        // ParquetSink uses the global (non-partitioned) builder for
        // `bytes_written`, distinct from `bytes_scanned`'s partitioned
        // `bytes_counter` above - cover that path too.
        let bytes_written =
            MetricBuilder::new(&metrics).global_bytes_counter("bytes_written");
        bytes_written.add(three_gib);

        // A generic Count/Gauge explicitly tagged Bytes must ALSO be
        // byte-formatted at Display time - see #24203. `MetricValue` is a
        // public, already-released exhaustive enum, so dedicated
        // BytesCount/BytesGauge variants would be a SemVer break; instead
        // `Display for Metric` reinterprets any Bytes-category Count/Gauge.
        let generic_bytes_gauge = MetricBuilder::new(&metrics)
            .with_category(MetricCategory::Bytes)
            .gauge("right_input_bytes", 0);
        generic_bytes_gauge.add(three_gib);

        // A generic Count/Gauge with NO Bytes category must keep
        // count-formatting (human_readable_count), not byte-formatting.
        let generic_rows_gauge =
            MetricBuilder::new(&metrics).gauge("right_input_rows", 0);
        generic_rows_gauge.add(three_gib);

        let rendered: Vec<String> = metrics
            .clone_inner()
            .iter()
            .map(|m| m.to_string())
            .collect();

        assert!(
            rendered
                .iter()
                .any(|s| s == "bytes_scanned{partition=0}=3.0 GB"),
            "bytes_scanned should be byte-formatted, got: {rendered:?}"
        );
        assert!(
            rendered
                .iter()
                .any(|s| s == "stream_memory_usage{partition=0}=3.0 GB"),
            "stream_memory_usage should be byte-formatted, got: {rendered:?}"
        );
        assert!(
            rendered.iter().any(|s| s == "bytes_written=3.0 GB"),
            "bytes_written (global_bytes_counter, no partition) should be byte-formatted, got: {rendered:?}"
        );
        assert!(
            rendered
                .iter()
                .any(|s| s == "right_input_bytes{partition=0}=3.0 GB"),
            "a generic Gauge tagged Bytes should be byte-formatted, got: {rendered:?}"
        );
        assert!(
            rendered
                .iter()
                .any(|s| s == "right_input_rows{partition=0}=3.22 B"),
            "a generic Gauge with no Bytes category must keep count-formatting, got: {rendered:?}"
        );
    }

    #[test]
    fn test_sum_by_name_custom_metric() {
        #[derive(Debug)]
        struct CustomCount(AtomicUsize);

        impl Display for CustomCount {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0.load(Ordering::Relaxed))
            }
        }

        impl CustomMetricValue for CustomCount {
            fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
                Arc::new(Self(AtomicUsize::new(0)))
            }

            fn aggregate(&self, other: Arc<dyn CustomMetricValue + 'static>) {
                let other = other.as_any().downcast_ref::<Self>().unwrap();
                self.0
                    .fetch_add(other.0.load(Ordering::Relaxed), Ordering::Relaxed);
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn as_usize(&self) -> usize {
                self.0.load(Ordering::Relaxed)
            }

            fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
                other.as_any().downcast_ref::<Self>().is_some_and(|other| {
                    self.0.load(Ordering::Relaxed) == other.0.load(Ordering::Relaxed)
                })
            }
        }

        let metrics = ExecutionPlanMetricsSet::new();
        for (name, value) in [("custom_count", 1), ("custom_count", 2), ("other", 4)] {
            MetricBuilder::new(&metrics).build(MetricValue::Custom {
                name: name.into(),
                value: Arc::new(CustomCount(AtomicUsize::new(value))),
            });
        }

        assert_eq!(
            metrics
                .clone_inner()
                .sum_by_name("custom_count")
                .map(|metric| metric.as_usize()),
            Some(3)
        );
    }

    #[test]
    #[should_panic(expected = "Mismatched metric types. Can not aggregate Count")]
    fn test_bad_sum() {
        // can not add different kinds of metrics
        let metrics = ExecutionPlanMetricsSet::new();

        let count = MetricBuilder::new(&metrics).counter("my_metric", 1);
        count.add(1);

        let time = MetricBuilder::new(&metrics).subset_time("my_metric", 1);
        time.add_duration(Duration::from_nanos(10));

        // expect that this will error out
        metrics.clone_inner().sum(|_| true);
    }

    #[test]
    fn test_aggregate_by_name() {
        let metrics = ExecutionPlanMetricsSet::new();

        // Note cpu_time1 has labels but it is still aggregated with metrics 2 and 3
        let elapsed_compute1 = MetricBuilder::new(&metrics)
            .with_new_label("foo", "bar")
            .elapsed_compute(1);
        elapsed_compute1.add_duration(Duration::from_nanos(12));

        let elapsed_compute2 = MetricBuilder::new(&metrics).elapsed_compute(2);
        elapsed_compute2.add_duration(Duration::from_nanos(34));

        let elapsed_compute3 = MetricBuilder::new(&metrics).elapsed_compute(4);
        elapsed_compute3.add_duration(Duration::from_nanos(56));

        let output_rows = MetricBuilder::new(&metrics).output_rows(1); // output rows
        output_rows.add(56);

        let aggregated = metrics.clone_inner().aggregate_by_name();

        // cpu time should be aggregated:
        let elapsed_computes = aggregated
            .iter()
            .filter(|metric| matches!(metric.value(), MetricValue::ElapsedCompute(_)))
            .collect::<Vec<_>>();
        assert_eq!(elapsed_computes.len(), 1);
        assert_eq!(elapsed_computes[0].value().as_usize(), 12 + 34 + 56);
        assert!(elapsed_computes[0].partition().is_none());

        // output rows should
        let output_rows = aggregated
            .iter()
            .filter(|metric| matches!(metric.value(), MetricValue::OutputRows(_)))
            .collect::<Vec<_>>();
        assert_eq!(output_rows.len(), 1);
        assert_eq!(output_rows[0].value().as_usize(), 56);
        assert!(output_rows[0].partition.is_none())
    }

    #[test]
    #[should_panic(expected = "Mismatched metric types. Can not aggregate Count")]
    fn test_aggregate_partition_bad_sum() {
        let metrics = ExecutionPlanMetricsSet::new();

        let count = MetricBuilder::new(&metrics).counter("my_metric", 1);
        count.add(1);

        let time = MetricBuilder::new(&metrics).subset_time("my_metric", 1);
        time.add_duration(Duration::from_nanos(10));

        // can't aggregate time and count -- expect a panic
        metrics.clone_inner().aggregate_by_name();
    }

    #[test]
    fn test_aggregate_partition_timestamps() {
        let metrics = ExecutionPlanMetricsSet::new();

        // 1431648000000000 == 1970-01-17 13:40:48 UTC
        let t1 = Utc.timestamp_nanos(1431648000000000);
        // 1531648000000000 == 1970-01-18 17:27:28 UTC
        let t2 = Utc.timestamp_nanos(1531648000000000);
        // 1631648000000000 == 1970-01-19 21:14:08 UTC
        let t3 = Utc.timestamp_nanos(1631648000000000);
        // 1731648000000000 == 1970-01-21 01:00:48 UTC
        let t4 = Utc.timestamp_nanos(1731648000000000);

        let start_timestamp0 = MetricBuilder::new(&metrics).start_timestamp(0);
        start_timestamp0.set(t1);
        let end_timestamp0 = MetricBuilder::new(&metrics).end_timestamp(0);
        end_timestamp0.set(t2);
        let start_timestamp1 = MetricBuilder::new(&metrics).start_timestamp(0);
        start_timestamp1.set(t3);
        let end_timestamp1 = MetricBuilder::new(&metrics).end_timestamp(0);
        end_timestamp1.set(t4);

        // aggregate
        let aggregated = metrics.clone_inner().aggregate_by_name();

        let mut ts = aggregated
            .iter()
            .filter(|metric| {
                matches!(metric.value(), MetricValue::StartTimestamp(_))
                    && metric.labels().is_empty()
            })
            .collect::<Vec<_>>();
        assert_eq!(ts.len(), 1);
        match ts.remove(0).value() {
            MetricValue::StartTimestamp(ts) => {
                // expect earliest of t1, t2
                assert_eq!(ts.value(), Some(t1));
            }
            _ => {
                panic!("Not a timestamp");
            }
        }

        let mut ts = aggregated
            .iter()
            .filter(|metric| {
                matches!(metric.value(), MetricValue::EndTimestamp(_))
                    && metric.labels().is_empty()
            })
            .collect::<Vec<_>>();
        assert_eq!(ts.len(), 1);
        match ts.remove(0).value() {
            MetricValue::EndTimestamp(ts) => {
                // expect latest of t3, t4
                assert_eq!(ts.value(), Some(t4));
            }
            _ => {
                panic!("Not a timestamp");
            }
        }
    }

    #[test]
    fn test_extend() {
        let mut metrics = MetricsSet::new();
        let m1 = Arc::new(Metric::new(MetricValue::OutputRows(Count::new()), None));
        let m2 = Arc::new(Metric::new(MetricValue::SpillCount(Count::new()), None));

        metrics.extend([Arc::clone(&m1), Arc::clone(&m2)]);
        assert_eq!(metrics.iter().count(), 2);

        let m3 = Arc::new(Metric::new(MetricValue::SpilledBytes(Count::new()), None));
        metrics.extend(std::iter::once(Arc::clone(&m3)));
        assert_eq!(metrics.iter().count(), 3);
    }

    #[test]
    fn test_collect() {
        let m1 = Arc::new(Metric::new(MetricValue::OutputRows(Count::new()), None));
        let m2 = Arc::new(Metric::new(MetricValue::SpillCount(Count::new()), None));

        let metrics: MetricsSet =
            vec![Arc::clone(&m1), Arc::clone(&m2)].into_iter().collect();
        assert_eq!(metrics.iter().count(), 2);

        let empty: MetricsSet = std::iter::empty().collect();
        assert_eq!(empty.iter().count(), 0);
    }

    #[test]
    fn test_into_iterator_by_ref() {
        let mut metrics = MetricsSet::new();
        metrics.push(Arc::new(Metric::new(
            MetricValue::OutputRows(Count::new()),
            None,
        )));
        metrics.push(Arc::new(Metric::new(
            MetricValue::SpillCount(Count::new()),
            None,
        )));

        let mut count = 0;
        for _m in &metrics {
            count += 1;
        }
        assert_eq!(count, 2);
    }

    #[test]
    fn test_sorted_for_display() {
        let metrics = ExecutionPlanMetricsSet::new();
        MetricBuilder::new(&metrics).end_timestamp(0);
        MetricBuilder::new(&metrics).start_timestamp(0);
        MetricBuilder::new(&metrics).elapsed_compute(0);
        MetricBuilder::new(&metrics).counter("the_second_counter", 0);
        MetricBuilder::new(&metrics).counter("the_counter", 0);
        MetricBuilder::new(&metrics).counter("the_third_counter", 0);
        MetricBuilder::new(&metrics).subset_time("the_time", 0);
        MetricBuilder::new(&metrics).output_rows(0);
        let metrics = metrics.clone_inner();

        fn metric_names(metrics: &MetricsSet) -> String {
            let n = metrics.iter().map(|m| m.value().name()).collect::<Vec<_>>();
            n.join(", ")
        }

        assert_eq!(
            "end_timestamp, start_timestamp, elapsed_compute, the_second_counter, the_counter, the_third_counter, the_time, output_rows",
            metric_names(&metrics)
        );

        let metrics = metrics.sorted_for_display();
        assert_eq!(
            "output_rows, elapsed_compute, the_counter, the_second_counter, the_third_counter, the_time, start_timestamp, end_timestamp",
            metric_names(&metrics)
        );
    }

    #[test]
    fn test_filter_by_names() {
        let metrics = ExecutionPlanMetricsSet::new();
        MetricBuilder::new(&metrics).output_rows(0);
        MetricBuilder::new(&metrics).counter("custom_counter", 0);

        assert!(
            metrics
                .clone_inner()
                .filter_by_names(&[])
                .iter()
                .next()
                .is_none()
        );

        let names = vec!["output_rows".to_string()];
        let filtered = metrics.clone_inner().filter_by_names(&names);

        assert_eq!(filtered.iter().count(), 1);
        assert_eq!(
            filtered.iter().next().unwrap().value().name(),
            "output_rows"
        );
    }
}
