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

use std::{
    borrow::Cow,
    fmt::{Debug, Display},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};

use hashbrown::HashMap;

/// Structure for constructing metrics, counters, timers, etc.
///
/// Note the use of `Cow<..>` is to avoid allocations in the common
/// case of constant strings
///
/// ```rust
/// TODO doc example
/// ```
pub struct MetricBuilder<'a> {
    /// Location that the metric created by this builder will be added do
    metrics: &'a ExecutionPlanMetricsSet,

    /// optional partition number
    partition: Option<usize>,

    /// arbitrary name=value pairs identifiying this metric
    labels: Vec<Label>,
}

impl<'a> MetricBuilder<'a> {
    /// Create a new `MetricBuilder` that will register the result of `build()` with the `metrics`
    pub fn new(metrics: &'a ExecutionPlanMetricsSet) -> Self {
        Self {
            metrics,
            partition: None,
            labels: vec![],
        }
    }

    /// Add a label to the metric being constructed
    pub fn with_label(mut self, label: Label) -> Self {
        self.labels.push(label);
        self
    }

    /// Add a label to the metric being constructed
    pub fn with_new_label(
        self,
        name: impl Into<Cow<'static, str>>,
        value: impl Into<Cow<'static, str>>,
    ) -> Self {
        self.with_label(Label::new(name.into(), value.into()))
    }

    /// Set the partition of the metric being constructed
    pub fn with_partition(mut self, partition: usize) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Consume self and create a metric of the specified value
    /// registered with the MetricsSet
    pub fn build(self, value: MetricValue) {
        let Self {
            labels,
            partition,
            metrics,
        } = self;
        let metric = Arc::new(SQLMetric::new_with_labels(value, partition, labels));
        metrics.register(metric.clone());
    }

    /// Consume self and create a new counter for recording output rows
    pub fn output_rows(self, partition: usize) -> Count {
        let count = Count::new();
        self.with_partition(partition)
            .build(MetricValue::OutputRows(count.clone()));
        count
    }

    /// Consumes self and creates a new [`Count`] for recording some
    /// arbitrary metric of an operator.
    pub fn counter(
        self,
        counter_name: impl Into<Cow<'static, str>>,
        partition: usize,
    ) -> Count {
        self.with_partition(partition).global_counter(counter_name)
    }

    /// Consumes self and creates a new [`Count`] for recording a
    /// metric of an overall operator (not per partition)
    pub fn global_counter(self, counter_name: impl Into<Cow<'static, str>>) -> Count {
        let count = Count::new();
        self.build(MetricValue::Count {
            name: counter_name.into(),
            count: count.clone(),
        });
        count
    }

    /// Consume self and create a new Timer for recording the overall cpu time
    /// spent by an operator
    pub fn cpu_time(self, partition: usize) -> Time {
        let time = Time::new();
        self.with_partition(partition)
            .build(MetricValue::CPUTime(time.clone()));
        time
    }

    /// Consumes self and creates a new Timer for recording some
    /// subset of of an operators execution time.
    pub fn subset_time(
        self,
        subset_name: impl Into<Cow<'static, str>>,
        partition: usize,
    ) -> Time {
        let time = Time::new();
        self.with_partition(partition).build(MetricValue::Time {
            name: subset_name.into(),
            time: time.clone(),
        });
        time
    }
}

/// A counter to record things such as number of input or output rows
///
/// Note `clone`ing counters update the same underlying metrics
#[derive(Debug, Clone)]
pub struct Count {
    /// value of the metric counter
    value: Arc<AtomicUsize>,
}

impl Count {
    /// create a new counter
    pub fn new() -> Self {
        Self {
            value: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Add `n` to the metric's value
    pub fn add(&self, n: usize) {
        // relaxed ordering for operations on `value` poses no issues
        // we're purely using atomic ops with no associated memory ops
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Get the current value
    pub fn value(&self) -> usize {
        self.value.load(Ordering::Relaxed)
    }
}

/// a SQLMetric for CPU timing information
#[derive(Debug, Clone)]
pub struct Time {
    /// elapsed time, in nanoseconds
    nanos: Arc<AtomicUsize>,
}

impl Time {
    /// Create a new [`Time`] wrapper suitable for recording elapsed
    /// times for operations.
    pub fn new() -> Self {
        Self {
            nanos: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Add elapsed nanoseconds since `start`to self
    pub fn add_elapsed(&self, start: Instant) {
        let more_nanos = start.elapsed().as_nanos() as usize;
        self.nanos.fetch_add(more_nanos, Ordering::Relaxed);
    }

    /// return a scoped guard that adds the amount of time elapsed
    /// between its creation and its drop or call to `stop` to the
    /// underlying metric.
    pub fn timer(&self) -> ScopedTimerGuard<'_> {
        ScopedTimerGuard {
            inner: self,
            start: Some(Instant::now()),
        }
    }

    /// Get the number of nanoseconds record by this Time metric
    pub fn value(&self) -> usize {
        self.nanos.load(Ordering::Relaxed)
    }
}

/// RAAI structure that adds all time between its construction and
/// destruction to the CPU time or the first call to `stop` whichever
/// comes first
pub struct ScopedTimerGuard<'a> {
    inner: &'a Time,
    start: Option<Instant>,
}

impl<'a> ScopedTimerGuard<'a> {
    /// Stop the timer timing and record the time taken
    pub fn stop(&mut self) {
        if let Some(start) = self.start.take() {
            self.inner.add_elapsed(start)
        }
    }

    /// Stop the timer, record the time taken and consume self
    pub fn done(mut self) {
        self.stop()
    }
}

impl<'a> Drop for ScopedTimerGuard<'a> {
    fn drop(&mut self) {
        self.stop()
    }
}

/// Possible values for each metric. Among other differences, the
/// metric types have various ways to display their values and some
/// metrics are so common they are given special treatment.
#[derive(Debug, Clone)]
pub enum MetricValue {
    /// Number of output rows produced
    OutputRows(Count),
    /// CPU time
    CPUTime(Time),
    /// Operator defined count
    Count {
        name: Cow<'static, str>,
        count: Count,
    },
    /// Operator defined time
    Time { name: Cow<'static, str>, time: Time }, // TODO timestamp, etc
                                                  // https://github.com/apache/arrow-datafusion/issues/866
}

impl MetricValue {
    pub fn name(&self) -> &str {
        // match self {
        //     MetricKind::OutputRows => "outputRows",
        //     MetricKind::Custom(name) => name,
        //     MetricKind::CPUTime => "cpuTime",
        // }
        todo!();
    }

    /// Return the value of the metric as a usize value
    pub fn as_usize(&self) -> usize {
        match self {
            MetricValue::OutputRows(count) => count.value(),
            MetricValue::CPUTime(time) => time.value(),
            MetricValue::Count { count, .. } => count.value(),
            MetricValue::Time { time, .. } => time.value(),
        }
    }

    /// create a new MetricValue with the same type as `self` suitable
    /// for accumulating
    pub fn new_empty(&self) -> Self {
        todo!()
    }

    /// Add the value of other to this. panic's if the type is mismatched or
    /// aggregating does not make sense for this value
    ///
    /// Note this is purposely marked `mut` (even though atomics are
    /// used) so Rust's type system can be used to ensure the
    /// appropriate API access. `MetricValues` should be modified
    /// using the original [`Count`] or [`Time`] they were created
    /// from.
    pub fn add(&mut self, other: &Self) {
        todo!()
    }
}

impl Display for MetricValue {
    /// Prints the value of this metric
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
        // // and now the value
        // let format_as_duration = match &self.kind {
        //     MetricKind::OutputRows => false,
        //     MetricKind::CPUTime => true,
        //     MetricKind::Custom(name) => name.contains("Time") || name.contains("time"),
        // };

        // if format_as_duration {
        //     let duration = std::time::Duration::from_nanos(self.value() as u64);
        //     write!(f, "={:?}", duration)
        // } else {
        //     write!(f, "={}", self.value())
        // }
    }
}

/// Something that tracks the metrics of an execution using an atomic
/// usize
#[derive(Debug)]
pub struct SQLMetric {
    /// value of the metric
    value: MetricValue,

    /// arbitrary name=value pairs identifiying this metric
    labels: Vec<Label>,

    /// To which partition of an operators output did this metric
    /// apply? If None means all partitions.
    partition: Option<usize>,
}

impl Display for SQLMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

                write!(f, "{}", i)?;
            }

            write!(f, "}}")?;
        }

        // and now the value
        write!(f, "{}", self.value)
    }
}

impl SQLMetric {
    /// Create a new SQLMetric. Consider using [`MetricBuilder`]
    /// rather than this function directly.
    pub fn new(value: MetricValue, partition: Option<usize>) -> Self {
        Self {
            value,
            labels: vec![],
            partition,
        }
    }

    /// Create a new SQLMetric. Consider using [`MetricBuilder`]
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
    pub fn with(mut self, label: Label) -> Self {
        self.labels.push(label);
        self
    }

    /// What labels are present for this metric?
    fn labels(&self) -> &[Label] {
        &self.labels
    }

    /// return a reference to the value of this metric
    pub fn value(&self) -> &MetricValue {
        &self.value
    }

    /// return a mutable reference to the value of this metric
    pub fn value_mut(&mut self) -> &mut MetricValue {
        &mut self.value
    }
}

/// A set of SQLMetrics for a particular operator
#[derive(Default, Debug, Clone)]
pub struct MetricsSet {
    metrics: Vec<Arc<SQLMetric>>,
}

impl MetricsSet {
    /// Create a new container of metrics
    pub fn new() -> Self {
        Default::default()
    }

    /// Add the specified metric
    pub fn push(&mut self, metric: Arc<SQLMetric>) {
        self.metrics.push(metric)
    }

    /// Add all [`SQLMetric`]s in this set to the specified array.
    fn extend_other(&mut self, metrics: &mut Vec<Arc<SQLMetric>>) {
        metrics.extend(self.metrics.iter().cloned())
    }

    /// convenience: return the number of rows produced, aggregated
    /// across partitions or None if no metric is present
    pub fn output_rows(&self) -> Option<usize> {
        self.sum(|metric| matches!(metric.value(), MetricValue::OutputRows(_)))
            .map(|v| v.as_usize())
    }

    /// convenience: return the amount of CPU time spent, aggregated
    /// across partitions or None if no metric is present
    pub fn cpu_time(&self) -> Option<usize> {
        self.sum(|metric| matches!(metric.value(), MetricValue::CPUTime(_)))
            .map(|v| v.as_usize())
    }

    /// Sums the values for metrics for which `f(metric)` returns
    /// true, and returns the value. Returns None if no metrics match
    /// the predicate.
    pub fn sum<F>(&self, mut f: F) -> Option<MetricValue>
    where
        F: FnMut(&SQLMetric) -> bool,
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

        iter.for_each(|metric| accum.add(metric.value()));

        Some(accum)
    }

    /// Returns returns a new derived `MetricsSet` where all metrics
    /// that had the same name and partition=`Some(..)` have been
    /// aggregated together. The resulting `MetricsSet` has all
    /// metrics with `Partition=None`
    pub fn aggregate_by_partition(&self) -> Self {
        let mut map = HashMap::new();

        // There are all sorts of ways to make this more efficient
        for metric in &self.metrics {
            let key = (metric.value.name(), metric.labels.clone());
            map.entry(key)
                .and_modify(|accum: &mut SQLMetric| {
                    accum.value_mut().add(metric.value());
                })
                .or_insert_with(|| {
                    // accumulate with no partition
                    let partition = None;
                    let mut accum = SQLMetric::new_with_labels(
                        metric.value().new_empty(),
                        partition,
                        metric.labels().to_vec(),
                    );
                    accum.value_mut().add(metric.value());
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
}

impl Display for MetricsSet {
    /// format the MetricsSet as a single string
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut is_first = true;
        for i in self.metrics.iter() {
            if !is_first {
                write!(f, ", ")?;
            } else {
                is_first = false;
            }

            write!(f, "{}", i)?;
        }
        Ok(())
    }
}

/// A set of SQLMetrics for an individual "operator" (e.g. `&dyn
/// ExecutionPlan`).
///
/// This structure is intended as a convenience for [`ExecutionPlan`]
/// implementations so they can generate different streams for multiple
/// partitions but easily report them together.
///
/// Each `clone()` of this structure will add metrics to the same
/// underlying metrics set
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

    /// Add the specified metric
    pub fn register(&self, metric: Arc<SQLMetric>) {
        self.inner.lock().expect("not poisoned").push(metric)
    }

    /// Add all [`SQLMetric`]s for this `ExecutionPlan` to the
    /// specified array.
    pub fn extend_other(&self, metrics: &mut Vec<Arc<SQLMetric>>) {
        self.inner
            .lock()
            .expect("not poisoned")
            .extend_other(metrics)
    }

    /// Return a clone of the inner MetricsSet
    pub fn clone_inner(&self) -> MetricsSet {
        let guard = self.inner.lock().expect("not poisoned");
        (*guard).clone()
    }
}

/// name=value pairs identifiying a metric. This concept is called various things
/// in various different systems:
///
/// "labels" in
/// [prometheus](https://prometheus.io/docs/concepts/data_model/) and
/// "tags" in
/// [InfluxDB](https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/)
/// , "attributes" in [open
/// telemetry](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/datamodel.md],
/// etc.
///
/// As the name and value are expected to mostly be constant strings,
/// use a `Cow` to avoid copying / allocations in this common case.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Label {
    name: Cow<'static, str>,
    value: Cow<'static, str>,
}

impl Label {
    /// Create a new Label
    pub fn new(
        name: impl Into<Cow<'static, str>>,
        value: impl Into<Cow<'static, str>>,
    ) -> Self {
        let name = name.into();
        let value = value.into();
        Self { name, value }
    }
}

impl Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}={}", self.name, self.value)
    }
}
