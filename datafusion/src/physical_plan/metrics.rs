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

pub mod wrappers;

use std::{
    fmt::{Debug, Display},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use hashbrown::HashMap;

use self::wrappers::{Count, Time};

/// Structure for constructing metrics, counters, timers, etc
pub struct MetricBuilder<'a> {
    /// Location that the metric created by this builder will be added do
    metrics: &'a SharedMetricsSet,

    /// optional partition number
    partition: Option<usize>,

    /// arbitrary name=value pairs identifiying this metric
    labels: Vec<Label>,
}

impl<'a> MetricBuilder<'a> {
    /// Create a new `MetricBuilder` that will register the result of `build()` with the `metrics`
    pub fn new(metrics: &'a SharedMetricsSet) -> Self {
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
        name: impl Into<Arc<str>>,
        value: impl Into<Arc<str>>,
    ) -> Self {
        self.with_label(Label::new(name.into(), value.into()))
    }

    /// Set the partition of the metric being constructed
    pub fn with_partition(mut self, partition: usize) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Consume self and create a metric of the specified kind
    /// registered with the MetricsSet
    pub fn build(self, kind: MetricKind) -> Arc<SQLMetric> {
        let Self {
            labels,
            partition,
            metrics,
        } = self;
        let metric = Arc::new(SQLMetric::new_with_labels(kind, partition, labels));
        metrics.register(metric.clone());
        metric
    }

    /// Consume self and create a new counter for recording output rows
    pub fn output_rows(self, partition: usize) -> Count {
        let metric = self.with_partition(partition).build(MetricKind::OutputRows);
        Count::new(metric)
    }

    /// Consumes self and creates a new Countr for recording
    /// some metric of an operators
    pub fn counter(self, counter_name: &'static str, partition: usize) -> Count {
        let metric = self
            .with_partition(partition)
            .build(MetricKind::Custom(counter_name));
        Count::new(metric)
    }

    /// Consumes self and creates a new Counter for recording
    /// some metric of an overall operator (not per partition
    pub fn global_counter(self, counter_name: &'static str) -> Count {
        let metric = self.build(MetricKind::Custom(counter_name));
        Count::new(metric)
    }

    /// Consume self and create a new Timer for recording the overall cpu time
    /// spent by an operator
    pub fn cpu_time(self, partition: usize) -> Time {
        let metric = self.with_partition(partition).build(MetricKind::CPUTime);
        Time::new(metric)
    }

    /// Consumes self and creates a new Timer for recording some
    /// subset of of an operators execution time
    pub fn subset_time(self, subset_name: &'static str, partition: usize) -> Time {
        let metric = self
            .with_partition(partition)
            .build(MetricKind::Custom(subset_name));
        Time::new(metric)
    }
}

/// Something that tracks the metrics of an execution using an atomic
/// usize
#[derive(Debug)]
pub struct SQLMetric {
    /// value of the metric
    value: AtomicUsize,

    /// arbitrary name=value pairs identifiying this metric
    labels: Vec<Label>,

    /// To which partition of an operators output did this metric
    /// apply? If None means all partitions.
    partition: Option<usize>,

    /// The kind of metric (how to logically interpret the value)
    kind: MetricKind,
}

impl Display for SQLMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)?;

        let mut iter = self
            .partition
            .iter()
            .map(|partition| {
                Label::new(
                    Arc::from("partition"),
                    Arc::from(partition.to_string().as_str()),
                )
            })
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
        let format_as_duration = match &self.kind {
            MetricKind::OutputRows => false,
            MetricKind::CPUTime => true,
            MetricKind::Custom(name) => name.contains("Time") || name.contains("time"),
        };

        if format_as_duration {
            let duration = std::time::Duration::from_nanos(self.value() as u64);
            write!(f, "={:?}", duration)
        } else {
            write!(f, "={}", self.value())
        }
    }
}

impl SQLMetric {
    /// Create a new SQLMetric
    pub fn new(kind: MetricKind, partition: Option<usize>) -> Self {
        Self {
            value: 0.into(),
            labels: vec![],
            partition,
            kind,
        }
    }

    /// Add a new label to this metric
    pub fn new_with_labels(
        kind: MetricKind,
        partition: Option<usize>,
        labels: Vec<Label>,
    ) -> Self {
        Self {
            value: 0.into(),
            labels,
            partition,
            kind,
        }
    }

    /// Add a new label to this metric
    pub fn with(mut self, label: Label) -> Self {
        self.labels.push(label);
        self
    }

    /// Add the standard name for output rows

    /// Get the current value
    pub fn value(&self) -> usize {
        self.value.load(Ordering::Relaxed)
    }

    /// get the kind of this metric
    pub fn kind(&self) -> &MetricKind {
        &self.kind
    }

    /// Add `n` to the metric's value
    pub fn add(&self, n: usize) {
        // relaxed ordering for operations on `value` poses no issues
        // we're purely using atomic ops with no associated memory ops
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Set the metric's value to `n`
    pub fn set(&self, n: usize) {
        self.value.store(n, Ordering::Relaxed);
    }

    /// What labels are present for this metric?
    fn labels(&self) -> &[Label] {
        &self.labels
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// How should the value of the metric be interpreted?
pub enum MetricKind {
    /// Number of output rows produced
    OutputRows,
    /// CPU time
    CPUTime,
    // TODO timestamp, etc
    // https://github.com/apache/arrow-datafusion/issues/866
    /// Arbitarary user defined type
    Custom(&'static str),
}

impl Display for MetricKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl MetricKind {
    /// return a user displayable name of this kind of metric
    pub fn name(&self) -> &str {
        match self {
            MetricKind::OutputRows => "outputRows",
            MetricKind::Custom(name) => name,
            MetricKind::CPUTime => "cpuTime",
        }
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
        self.sum(|metric| matches!(metric.kind(), MetricKind::OutputRows))
    }

    /// convenience: return the amount of CPU time spent, aggregated
    /// across partitions or None if no metric is present
    pub fn cpu_time(&self) -> Option<usize> {
        self.sum(|metric| matches!(metric.kind(), MetricKind::CPUTime))
    }

    /// Sums the values for metrics for which `f(metric)` returns
    /// true, and returns the value. Returns None if no metrics match
    /// the predicate.
    pub fn sum<F>(&self, mut f: F) -> Option<usize>
    where
        F: FnMut(&SQLMetric) -> bool,
    {
        let mut iter = self
            .metrics
            .iter()
            .filter(|metric| f(metric.as_ref()))
            .peekable();

        if iter.peek().is_none() {
            None
        } else {
            Some(iter.map(|metric| metric.value()).sum())
        }
    }

    /// Returns returns a new derived `MetricsSet` where all metrics
    /// that had partition=`Some(..)` have been aggregated
    /// together. The resulting `MetricsSet` has all metrics with `Partition=None`
    pub fn aggregate_by_partition(&self) -> Self {
        let mut map = HashMap::new();

        // There are all sorts of ways to make this more efficient
        for metric in &self.metrics {
            let key = (metric.kind.clone(), metric.labels.clone());
            map.entry(key)
                .and_modify(|accum: &mut SQLMetric| {
                    accum.set(accum.value() + metric.value())
                })
                .or_insert_with(|| {
                    // accumulate with no partition
                    let partition = None;
                    let accum = SQLMetric::new_with_labels(
                        metric.kind().clone(),
                        partition,
                        metric.labels().to_vec(),
                    );
                    accum.set(metric.value());
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

/// A set of SQLMetrics that can be added to as partitions
/// execute. Designed to be a convenience for operator implementation
#[derive(Default, Debug, Clone)]
pub struct SharedMetricsSet {
    inner: Arc<Mutex<MetricsSet>>,
}

impl SharedMetricsSet {
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Label {
    name: Arc<str>,
    value: Arc<str>,
}

impl Label {
    /// Create a new Label
    pub fn new(name: impl Into<Arc<str>>, value: impl Into<Arc<str>>) -> Self {
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
