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

mod builder;
mod value;

use std::{
    borrow::Cow,
    fmt::{Debug, Display},
    sync::{Arc, Mutex},
};

use hashbrown::HashMap;

// public exports
pub use builder::MetricBuilder;
pub use value::{Count, MetricValue, ScopedTimerGuard, Time};

/// Something that tracks a value of interest (metric) of a DataFusion
/// [`ExecutionPlan`] execution.
///
/// Typically [`Metric`]s are not created directly, but instead
/// are created using [`MetricBuilder`] or methods on
/// [`ExecutionPlanMetricsSet`].
#[derive(Debug)]
pub struct Metric {
    /// The value the metric
    value: MetricValue,

    /// arbitrary name=value pairs identifiying this metric
    labels: Vec<Label>,

    /// To which partition of an operators output did this metric
    /// apply? If None means all partitions.
    partition: Option<usize>,
}

impl Display for Metric {
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

/// A snapshot of the metrics for a particular operator (`dyn
/// ExecutionPlan`).
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
                .and_modify(|accum: &mut Metric| {
                    accum.value_mut().add(metric.value());
                })
                .or_insert_with(|| {
                    // accumulate with no partition
                    let partition = None;
                    let mut accum = Metric::new_with_labels(
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

/// A set of [`Metric`] for an individual "operator" (e.g. `&dyn
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

    /// Add the specified metric to the underlying metric set
    pub fn register(&self, metric: Arc<Metric>) {
        self.inner.lock().expect("not poisoned").push(metric)
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
