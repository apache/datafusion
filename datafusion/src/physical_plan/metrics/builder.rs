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

//! Builder for creating arbitrary metrics

use std::{borrow::Cow, sync::Arc};

use super::{
    Count, ExecutionPlanMetricsSet, Label, Metric, MetricValue, Time, Timestamp,
};

/// Structure for constructing metrics, counters, timers, etc.
///
/// Note the use of `Cow<..>` is to avoid allocations in the common
/// case of constant strings
///
/// ```rust
///  use datafusion::physical_plan::metrics::*;
///
///  let metrics = ExecutionPlanMetricsSet::new();
///  let partition = 1;
///
///  // Create the standard output_rows metric
///  let output_rows = MetricBuilder::new(&metrics).output_rows(partition);
///
///  // Create a operator specific counter with some labels
///  let num_bytes = MetricBuilder::new(&metrics)
///    .with_new_label("filename", "my_awesome_file.parquet")
///    .counter("num_bytes", partition);
///
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
        let metric = Arc::new(Metric::new_with_labels(value, partition, labels));
        metrics.register(metric);
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

    /// Consume self and create a new Timer for recording the elapsed
    /// CPU time spent by an operator
    pub fn elapsed_compute(self, partition: usize) -> Time {
        let time = Time::new();
        self.with_partition(partition)
            .build(MetricValue::ElapsedCompute(time.clone()));
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

    /// Consumes self and creates a new Timestamp for recording the
    /// starting time of execution for a partition
    pub fn start_timestamp(self, partition: usize) -> Timestamp {
        let timestamp = Timestamp::new();
        self.with_partition(partition)
            .build(MetricValue::StartTimestamp(timestamp.clone()));
        timestamp
    }

    /// Consumes self and creates a new Timestamp for recording the
    /// ending time of execution for a partition
    pub fn end_timestamp(self, partition: usize) -> Timestamp {
        let timestamp = Timestamp::new();
        self.with_partition(partition)
            .build(MetricValue::EndTimestamp(timestamp.clone()));
        timestamp
    }
}
