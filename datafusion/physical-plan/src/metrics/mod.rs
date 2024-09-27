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
mod value;

use parking_lot::Mutex;
use std::{
    borrow::Cow,
    fmt::{Debug, Display},
    sync::Arc,
};

use hashbrown::HashMap;

// public exports
pub use baseline::{BaselineMetrics, RecordOutput};
pub use builder::MetricBuilder;
pub use value::{Count, Gauge, MetricValue, ScopedTimerGuard, Time, Timestamp};

/// Something that tracks a value of interest (metric) of a DataFusion
/// [`ExecutionPlan`] execution.
///
/// Typically [`Metric`]s are not created directly, but instead
/// are created using [`MetricBuilder`] or methods on
/// [`ExecutionPlanMetricsSet`].
///
/// ```
///  use datafusion_physical_plan::metrics::*;
///
///  let metrics = ExecutionPlanMetricsSet::new();
///  assert!(metrics.clone_inner().output_rows().is_none());
///
///  // Create a counter to increment using the MetricBuilder
///  let partition = 1;
///  let output_rows = MetricBuilder::new(&metrics)
///      .output_rows(partition);
///
///  // Counter can be incremented
///  output_rows.add(13);
///
///  // The value can be retrieved directly:
///  assert_eq!(output_rows.value(), 13);
///
///  // As well as from the metrics set
///  assert_eq!(metrics.clone_inner().output_rows(), Some(13));
/// ```
///
/// [`ExecutionPlan`]: super::ExecutionPlan

#[derive(Debug)]
pub struct Metric {
    /// The value of the metric
    value: MetricValue,

    /// arbitrary name=value pairs identifying this metric
    labels: Vec<Label>,

    /// To which partition of an operators output did this metric
    /// apply? If `None` then means all partitions.
    partition: Option<usize>,
}

impl Display for Metric {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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
        write!(f, "={}", self.value)
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
        }
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
}

/// A snapshot of the metrics for a particular ([`ExecutionPlan`]).
///
/// [`ExecutionPlan`]: super::ExecutionPlan
#[derive(Default, Debug, Clone)]
pub struct MetricsSet {
    metrics: Vec<Arc<Metric>>,
}

impl MetricsSet {
    /// Create a new container of metrics
    pub fn new() -> Self {
        Default::default()
    }

    /// Add the specified metric
    pub fn push(&mut self, metric: Arc<Metric>) {
        self.metrics.push(metric)
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
            MetricValue::SpilledRows(_) => false,
            MetricValue::CurrentMemoryUsage(_) => false,
            MetricValue::Gauge { name, .. } => name == metric_name,
            MetricValue::StartTimestamp(_) => false,
            MetricValue::EndTimestamp(_) => false,
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
                    let mut accum = Metric::new(metric.value().new_empty(), partition);
                    accum.value_mut().aggregate(metric.value());
                    accum
                });
        }

        let new_metrics = map
            .into_iter()
            .map(|(_k, v)| Arc::new(v))
            .collect::<Vec<_>>();

        Self {
            metrics: new_metrics,
        }
    }

    /// Sort the order of metrics so the "most useful" show up first
    pub fn sorted_for_display(mut self) -> Self {
        self.metrics.sort_unstable_by_key(|metric| {
            (
                metric.value().display_sort_key(),
                metric.value().name().to_owned(),
            )
        });
        self
    }

    /// Remove all timestamp metrics (for more compact display)
    pub fn timestamps_removed(self) -> Self {
        let Self { metrics } = self;

        let metrics = metrics
            .into_iter()
            .filter(|m| !m.value.is_timestamp())
            .collect::<Vec<_>>();

        Self { metrics }
    }
}

impl Display for MetricsSet {
    /// Format the [`MetricsSet`] as a single string
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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

/// A set of [`Metric`]s for an individual "operator" (e.g. `&dyn
/// ExecutionPlan`).
///
/// This structure is intended as a convenience for [`ExecutionPlan`]
/// implementations so they can generate different streams for multiple
/// partitions but easily report them together.
///
/// Each `clone()` of this structure will add metrics to the same
/// underlying metrics set
///
/// [`ExecutionPlan`]: super::ExecutionPlan
#[derive(Default, Debug, Clone)]
pub struct ExecutionPlanMetricsSet {
    inner: Arc<Mutex<MetricsSet>>,
}

impl ExecutionPlanMetricsSet {
    /// Create a new empty shared metrics set
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MetricsSet::new())),
        }
    }

    /// Add the specified metric to the underlying metric set
    pub fn register(&self, metric: Arc<Metric>) {
        self.inner.lock().push(metric)
    }

    /// Return a clone of the inner [`MetricsSet`]
    pub fn clone_inner(&self) -> MetricsSet {
        let guard = self.inner.lock();
        (*guard).clone()
    }
}

/// `name=value` pairs identifiying a metric. This concept is called various things
/// in various different systems:
///
/// "labels" in
/// [prometheus](https://prometheus.io/docs/concepts/data_model/) and
/// "tags" in
/// [InfluxDB](https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/)
/// , "attributes" in [open
/// telemetry]<https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/datamodel.md>,
/// etc.
///
/// As the name and value are expected to mostly be constant strings,
/// use a [`Cow`] to avoid copying / allocations in this common case.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Label {
    name: Cow<'static, str>,
    value: Cow<'static, str>,
}

impl Label {
    /// Create a new [`Label`]
    pub fn new(
        name: impl Into<Cow<'static, str>>,
        value: impl Into<Cow<'static, str>>,
    ) -> Self {
        let name = name.into();
        let value = value.into();
        Self { name, value }
    }

    /// Returns the name of this label
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    /// Returns the value of this label
    pub fn value(&self) -> &str {
        self.value.as_ref()
    }
}

impl Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}={}", self.name, self.value)
    }
}

#[cfg(test)]
mod tests {
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
        };

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
        };
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

        assert_eq!("end_timestamp, start_timestamp, elapsed_compute, the_second_counter, the_counter, the_third_counter, the_time, output_rows", metric_names(&metrics));

        let metrics = metrics.sorted_for_display();
        assert_eq!("output_rows, elapsed_compute, the_counter, the_second_counter, the_third_counter, the_time, start_timestamp, end_timestamp", metric_names(&metrics));
    }
}
