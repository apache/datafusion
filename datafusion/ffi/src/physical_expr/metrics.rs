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

//! FFI-stable mirrors of [`MetricsSet`] and related metric types.
//!
//! Metrics are passed across the FFI boundary as a **snapshot**: all
//! atomic-backed counters/gauges/timers are read into plain integer fields
//! at conversion time. Callers re-invoke [`ExecutionPlan::metrics()`] across
//! the boundary to observe newer values. This matches the documented contract
//! ("Once `self.execute()` has returned... metrics should be complete") and
//! all in-tree consumers (`AnalyzeExec`, `DisplayableExecutionPlan`).
//!
//! The variant *order* of [`FFI_MetricValue`] is part of the stable ABI and
//! must not be reordered. New variants must be appended at the end.
//!
//! [`ExecutionPlan::metrics()`]: datafusion_physical_plan::ExecutionPlan::metrics

use std::any::Any;
use std::borrow::Cow;
use std::fmt::{self, Debug, Display};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion_common::format::{MetricCategory, MetricType};
use datafusion_physical_expr_common::metrics::{
    Count, CustomMetricValue, Gauge, MetricValue, MetricsSet, PruningMetrics,
    RatioMergeStrategy, RatioMetrics, Time, Timestamp,
};
use datafusion_physical_expr_common::metrics::{Label, Metric};
use stabby::string::String as SString;
use stabby::vec::Vec as SVec;

use crate::ffi_option::FFI_Option;

/// FFI-stable mirror of [`MetricsSet`].
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_MetricsSet {
    pub metrics: SVec<FFI_Metric>,
}

/// FFI-stable mirror of [`Metric`].
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_Metric {
    pub value: FFI_MetricValue,
    pub labels: SVec<FFI_Label>,
    pub partition: FFI_Option<u64>,
    pub metric_type: FFI_MetricType,
    pub metric_category: FFI_Option<FFI_MetricCategory>,
}

/// FFI-stable mirror of [`Label`].
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_Label {
    pub name: SString,
    pub value: SString,
}

/// FFI-stable mirror of [`MetricType`].
#[expect(non_camel_case_types)]
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FFI_MetricType {
    Summary,
    Dev,
}

/// FFI-stable mirror of [`MetricCategory`].
#[expect(non_camel_case_types)]
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FFI_MetricCategory {
    Rows,
    Bytes,
    Timing,
    Uncategorized,
}

/// FFI-stable mirror of [`PruningMetrics`]. All counts are snapshotted at
/// conversion time.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FFI_PruningMetrics {
    pub pruned: u64,
    pub matched: u64,
    pub fully_matched: u64,
}

/// FFI-stable mirror of [`RatioMergeStrategy`].
#[expect(non_camel_case_types)]
#[expect(
    clippy::enum_variant_names,
    reason = "match RatioMergeStrategy variants"
)]
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FFI_RatioMergeStrategy {
    AddPartAddTotal,
    AddPartSetTotal,
    SetPartAddTotal,
}

/// FFI-stable mirror of [`RatioMetrics`]. Numerator/denominator are
/// snapshotted at conversion time.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FFI_RatioMetrics {
    pub part: u64,
    pub total: u64,
    pub merge_strategy: FFI_RatioMergeStrategy,
    pub display_raw_values: bool,
}

/// FFI-stable mirror of [`MetricValue`].
#[repr(C, u8)]
#[derive(Debug, Clone)]
pub enum FFI_MetricValue {
    OutputRows(u64),
    ElapsedComputeNs(u64),
    SpillCount(u64),
    SpilledBytes(u64),
    OutputBytes(u64),
    OutputBatches(u64),
    SpilledRows(u64),
    CurrentMemoryUsage(u64),
    Count {
        name: SString,
        count: u64,
    },
    Gauge {
        name: SString,
        gauge: u64,
    },
    Time {
        name: SString,
        time_ns: u64,
    },
    StartTimestampNsUTC(FFI_Option<i64>),
    EndTimestampNsUTC(FFI_Option<i64>),
    PruningMetrics {
        name: SString,
        pruning_metrics: FFI_PruningMetrics,
    },
    Ratio {
        name: SString,
        ratio_metrics: FFI_RatioMetrics,
    },
    /// Custom metrics are marshalled as their `Display` output plus the
    /// `as_usize()` fallback. The underlying `dyn CustomMetricValue` type is
    /// not preserved across the boundary, so `aggregate`/`as_any` downcasting
    /// are lost; the reconstructed value uses [`FfiCustomMetricValue`].
    Custom {
        name: SString,
        display: SString,
        as_usize_value: u64,
    },
}

// -----------------------------------------------------------------------------
// MetricsSet <-> FFI_MetricsSet
// -----------------------------------------------------------------------------

impl From<&MetricsSet> for FFI_MetricsSet {
    fn from(set: &MetricsSet) -> Self {
        Self {
            metrics: set.iter().map(|m| FFI_Metric::from(m.as_ref())).collect(),
        }
    }
}

impl From<FFI_MetricsSet> for MetricsSet {
    fn from(set: FFI_MetricsSet) -> Self {
        let mut out = MetricsSet::new();
        for ffi_metric in set.metrics {
            out.push(Arc::new(Metric::from(ffi_metric)));
        }
        out
    }
}

// -----------------------------------------------------------------------------
// Metric <-> FFI_Metric
// -----------------------------------------------------------------------------

impl From<&Metric> for FFI_Metric {
    fn from(m: &Metric) -> Self {
        Self {
            value: FFI_MetricValue::from(m.value()),
            labels: m.labels().iter().map(FFI_Label::from).collect(),
            partition: m.partition().map(|p| p as u64).into(),
            metric_type: m.metric_type().into(),
            metric_category: m.metric_category().map(FFI_MetricCategory::from).into(),
        }
    }
}

impl From<FFI_Metric> for Metric {
    fn from(m: FFI_Metric) -> Self {
        let labels: Vec<Label> = m.labels.into_iter().map(Label::from).collect();
        let partition: Option<u64> = m.partition.into();
        let category: Option<FFI_MetricCategory> = m.metric_category.into();
        let mut metric = Metric::new_with_labels(
            m.value.into(),
            partition.map(|p| p as usize),
            labels,
        )
        .with_type(m.metric_type.into());
        if let Some(c) = category {
            metric = metric.with_category(c.into());
        }
        metric
    }
}

// -----------------------------------------------------------------------------
// Label <-> FFI_Label
// -----------------------------------------------------------------------------

impl From<&Label> for FFI_Label {
    fn from(l: &Label) -> Self {
        Self {
            name: SString::from(l.name()),
            value: SString::from(l.value()),
        }
    }
}

impl From<FFI_Label> for Label {
    fn from(l: FFI_Label) -> Self {
        let name: String = l.name.into();
        let value: String = l.value.into();
        Label::new(name, value)
    }
}

// -----------------------------------------------------------------------------
// MetricType <-> FFI_MetricType
// -----------------------------------------------------------------------------

impl From<MetricType> for FFI_MetricType {
    fn from(t: MetricType) -> Self {
        match t {
            MetricType::Summary => Self::Summary,
            MetricType::Dev => Self::Dev,
        }
    }
}

impl From<FFI_MetricType> for MetricType {
    fn from(t: FFI_MetricType) -> Self {
        match t {
            FFI_MetricType::Summary => Self::Summary,
            FFI_MetricType::Dev => Self::Dev,
        }
    }
}

// -----------------------------------------------------------------------------
// MetricCategory <-> FFI_MetricCategory
// -----------------------------------------------------------------------------

impl From<MetricCategory> for FFI_MetricCategory {
    fn from(c: MetricCategory) -> Self {
        match c {
            MetricCategory::Rows => Self::Rows,
            MetricCategory::Bytes => Self::Bytes,
            MetricCategory::Timing => Self::Timing,
            MetricCategory::Uncategorized => Self::Uncategorized,
        }
    }
}

impl From<FFI_MetricCategory> for MetricCategory {
    fn from(c: FFI_MetricCategory) -> Self {
        match c {
            FFI_MetricCategory::Rows => Self::Rows,
            FFI_MetricCategory::Bytes => Self::Bytes,
            FFI_MetricCategory::Timing => Self::Timing,
            FFI_MetricCategory::Uncategorized => Self::Uncategorized,
        }
    }
}

// -----------------------------------------------------------------------------
// RatioMergeStrategy <-> FFI_RatioMergeStrategy
// -----------------------------------------------------------------------------

impl From<&RatioMergeStrategy> for FFI_RatioMergeStrategy {
    fn from(s: &RatioMergeStrategy) -> Self {
        match s {
            RatioMergeStrategy::AddPartAddTotal => Self::AddPartAddTotal,
            RatioMergeStrategy::AddPartSetTotal => Self::AddPartSetTotal,
            RatioMergeStrategy::SetPartAddTotal => Self::SetPartAddTotal,
        }
    }
}

impl From<FFI_RatioMergeStrategy> for RatioMergeStrategy {
    fn from(s: FFI_RatioMergeStrategy) -> Self {
        match s {
            FFI_RatioMergeStrategy::AddPartAddTotal => Self::AddPartAddTotal,
            FFI_RatioMergeStrategy::AddPartSetTotal => Self::AddPartSetTotal,
            FFI_RatioMergeStrategy::SetPartAddTotal => Self::SetPartAddTotal,
        }
    }
}

// -----------------------------------------------------------------------------
// PruningMetrics <-> FFI_PruningMetrics
// -----------------------------------------------------------------------------

impl From<&PruningMetrics> for FFI_PruningMetrics {
    fn from(p: &PruningMetrics) -> Self {
        Self {
            pruned: p.pruned() as u64,
            matched: p.matched() as u64,
            fully_matched: p.fully_matched() as u64,
        }
    }
}

impl From<FFI_PruningMetrics> for PruningMetrics {
    fn from(p: FFI_PruningMetrics) -> Self {
        let out = PruningMetrics::new();
        out.add_pruned(p.pruned as usize);
        out.add_matched(p.matched as usize);
        out.add_fully_matched(p.fully_matched as usize);
        out
    }
}

// -----------------------------------------------------------------------------
// RatioMetrics <-> FFI_RatioMetrics
// -----------------------------------------------------------------------------

impl From<&RatioMetrics> for FFI_RatioMetrics {
    fn from(r: &RatioMetrics) -> Self {
        Self {
            part: r.part() as u64,
            total: r.total() as u64,
            merge_strategy: r.merge_strategy().into(),
            display_raw_values: r.display_raw_values(),
        }
    }
}

impl From<FFI_RatioMetrics> for RatioMetrics {
    fn from(r: FFI_RatioMetrics) -> Self {
        let out = RatioMetrics::new()
            .with_merge_strategy(r.merge_strategy.into())
            .with_display_raw_values(r.display_raw_values);
        out.set_part(r.part as usize);
        out.set_total(r.total as usize);
        out
    }
}

// -----------------------------------------------------------------------------
// MetricValue <-> FFI_MetricValue
// -----------------------------------------------------------------------------

fn timestamp_to_ffi(ts: &Timestamp) -> FFI_Option<i64> {
    ts.value().and_then(|dt| dt.timestamp_nanos_opt()).into()
}

fn timestamp_from_ffi(nanos: FFI_Option<i64>) -> Timestamp {
    let ts = Timestamp::new();
    if let Some(n) = nanos.into_option() {
        ts.set(DateTime::<Utc>::from_timestamp_nanos(n));
    }
    ts
}

fn count_from_value(v: u64) -> Count {
    let c = Count::new();
    c.add(v as usize);
    c
}

fn gauge_from_value(v: u64) -> Gauge {
    let g = Gauge::new();
    g.add(v as usize);
    g
}

fn time_from_nanos(nanos: u64) -> Time {
    let t = Time::new();
    if nanos != 0 {
        // add_duration always adds at least one
        t.add_duration(std::time::Duration::from_nanos(nanos));
    }
    t
}

impl From<&MetricValue> for FFI_MetricValue {
    fn from(v: &MetricValue) -> Self {
        match v {
            MetricValue::OutputRows(c) => Self::OutputRows(c.value() as u64),
            MetricValue::ElapsedCompute(t) => Self::ElapsedComputeNs(t.value() as u64),
            MetricValue::SpillCount(c) => Self::SpillCount(c.value() as u64),
            MetricValue::SpilledBytes(c) => Self::SpilledBytes(c.value() as u64),
            MetricValue::OutputBytes(c) => Self::OutputBytes(c.value() as u64),
            MetricValue::OutputBatches(c) => Self::OutputBatches(c.value() as u64),
            MetricValue::SpilledRows(c) => Self::SpilledRows(c.value() as u64),
            MetricValue::CurrentMemoryUsage(g) => {
                Self::CurrentMemoryUsage(g.value() as u64)
            }
            MetricValue::Count { name, count } => Self::Count {
                name: SString::from(name.as_ref()),
                count: count.value() as u64,
            },
            MetricValue::Gauge { name, gauge } => Self::Gauge {
                name: SString::from(name.as_ref()),
                gauge: gauge.value() as u64,
            },
            MetricValue::Time { name, time } => Self::Time {
                name: SString::from(name.as_ref()),
                time_ns: time.value() as u64,
            },
            MetricValue::StartTimestamp(ts) => {
                Self::StartTimestampNsUTC(timestamp_to_ffi(ts))
            }
            MetricValue::EndTimestamp(ts) => {
                Self::EndTimestampNsUTC(timestamp_to_ffi(ts))
            }
            MetricValue::PruningMetrics {
                name,
                pruning_metrics,
            } => Self::PruningMetrics {
                name: SString::from(name.as_ref()),
                pruning_metrics: pruning_metrics.into(),
            },
            MetricValue::Ratio {
                name,
                ratio_metrics,
            } => Self::Ratio {
                name: SString::from(name.as_ref()),
                ratio_metrics: ratio_metrics.into(),
            },
            MetricValue::Custom { name, value } => Self::Custom {
                name: SString::from(name.as_ref()),
                display: SString::from(value.to_string().as_str()),
                as_usize_value: value.as_usize() as u64,
            },
        }
    }
}

impl From<FFI_MetricValue> for MetricValue {
    fn from(v: FFI_MetricValue) -> Self {
        match v {
            FFI_MetricValue::OutputRows(n) => Self::OutputRows(count_from_value(n)),
            FFI_MetricValue::ElapsedComputeNs(n) => {
                Self::ElapsedCompute(time_from_nanos(n))
            }
            FFI_MetricValue::SpillCount(n) => Self::SpillCount(count_from_value(n)),
            FFI_MetricValue::SpilledBytes(n) => Self::SpilledBytes(count_from_value(n)),
            FFI_MetricValue::OutputBytes(n) => Self::OutputBytes(count_from_value(n)),
            FFI_MetricValue::OutputBatches(n) => Self::OutputBatches(count_from_value(n)),
            FFI_MetricValue::SpilledRows(n) => Self::SpilledRows(count_from_value(n)),
            FFI_MetricValue::CurrentMemoryUsage(n) => {
                Self::CurrentMemoryUsage(gauge_from_value(n))
            }
            FFI_MetricValue::Count { name, count } => Self::Count {
                name: Cow::Owned(name.into()),
                count: count_from_value(count),
            },
            FFI_MetricValue::Gauge { name, gauge } => Self::Gauge {
                name: Cow::Owned(name.into()),
                gauge: gauge_from_value(gauge),
            },
            FFI_MetricValue::Time { name, time_ns } => Self::Time {
                name: Cow::Owned(name.into()),
                time: time_from_nanos(time_ns),
            },
            FFI_MetricValue::StartTimestampNsUTC(nanos) => {
                Self::StartTimestamp(timestamp_from_ffi(nanos))
            }
            FFI_MetricValue::EndTimestampNsUTC(nanos) => {
                Self::EndTimestamp(timestamp_from_ffi(nanos))
            }
            FFI_MetricValue::PruningMetrics {
                name,
                pruning_metrics,
            } => Self::PruningMetrics {
                name: Cow::Owned(name.into()),
                pruning_metrics: pruning_metrics.into(),
            },
            FFI_MetricValue::Ratio {
                name,
                ratio_metrics,
            } => Self::Ratio {
                name: Cow::Owned(name.into()),
                ratio_metrics: ratio_metrics.into(),
            },
            FFI_MetricValue::Custom {
                name,
                display,
                as_usize_value,
            } => Self::Custom {
                name: Cow::Owned(name.into()),
                value: Arc::new(FfiCustomMetricValue {
                    display: display.into(),
                    as_usize_value: as_usize_value as usize,
                }),
            },
        }
    }
}

/// [`CustomMetricValue`] shim used when reconstructing a `MetricValue::Custom`
/// on the consumer side of the FFI boundary.
///
/// The original `dyn CustomMetricValue` is not preserved across FFI — only its
/// `Display` output and `as_usize()` fallback. As a result:
/// - `Display` returns the producer-side rendered string.
/// - `as_usize` returns the snapshot value captured at marshal time.
/// - `aggregate` is a no-op: snapshots from FFI are not mergeable.
/// - `as_any` only downcasts to `FfiCustomMetricValue` itself.
#[derive(Debug)]
pub struct FfiCustomMetricValue {
    display: String,
    as_usize_value: usize,
}

impl Display for FfiCustomMetricValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display)
    }
}

impl CustomMetricValue for FfiCustomMetricValue {
    fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
        Arc::new(FfiCustomMetricValue {
            display: String::new(),
            as_usize_value: 0,
        })
    }

    fn aggregate(&self, _other: Arc<dyn CustomMetricValue>) {
        // FFI snapshots are immutable and not mergeable; aggregation across
        // the boundary is intentionally a no-op.
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_usize(&self) -> usize {
        self.as_usize_value
    }

    fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .map(|o| o.display == self.display && o.as_usize_value == self.as_usize_value)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_physical_expr_common::metrics::Label;

    /// Round-trips `value` through the FFI representation, asserts the
    /// `PartialEq`-based equality, and returns the reconstructed value so
    /// callers can additionally assert fields that `MetricValue`'s
    /// `PartialEq` impl does not compare (e.g. `PruningMetrics::fully_matched`,
    /// `RatioMetrics::merge_strategy`).
    fn assert_value_roundtrip(value: MetricValue) -> MetricValue {
        let ffi: FFI_MetricValue = (&value).into();
        let back: MetricValue = ffi.into();
        assert_eq!(value, back, "round-trip mismatch for {value:?}");
        back
    }

    #[test]
    fn roundtrip_named_variants() {
        let c = Count::new();
        c.add(7);
        assert_value_roundtrip(MetricValue::OutputRows(c.clone()));
        assert_value_roundtrip(MetricValue::SpillCount(c.clone()));
        assert_value_roundtrip(MetricValue::SpilledBytes(c.clone()));
        assert_value_roundtrip(MetricValue::OutputBytes(c.clone()));
        assert_value_roundtrip(MetricValue::OutputBatches(c.clone()));
        assert_value_roundtrip(MetricValue::SpilledRows(c.clone()));

        let g = Gauge::new();
        g.add(123);
        assert_value_roundtrip(MetricValue::CurrentMemoryUsage(g));

        let t = Time::new();
        t.add_duration(std::time::Duration::from_nanos(456));
        assert_value_roundtrip(MetricValue::ElapsedCompute(t));
    }

    #[test]
    fn roundtrip_keyed_count_gauge_time() {
        let count = Count::new();
        count.add(11);
        assert_value_roundtrip(MetricValue::Count {
            name: Cow::Borrowed("custom_count"),
            count,
        });

        let gauge = Gauge::new();
        gauge.add(22);
        assert_value_roundtrip(MetricValue::Gauge {
            name: Cow::Borrowed("custom_gauge"),
            gauge,
        });

        let time = Time::new();
        time.add_duration(std::time::Duration::from_nanos(33));
        assert_value_roundtrip(MetricValue::Time {
            name: Cow::Borrowed("custom_time"),
            time,
        });
    }

    #[test]
    fn roundtrip_timestamps() {
        let unset = Timestamp::new();
        assert_value_roundtrip(MetricValue::StartTimestamp(unset.clone()));
        assert_value_roundtrip(MetricValue::EndTimestamp(unset));

        let set = Timestamp::new();
        set.set(DateTime::<Utc>::from_timestamp_nanos(
            1_700_000_000_000_000_000,
        ));
        assert_value_roundtrip(MetricValue::StartTimestamp(set.clone()));
        assert_value_roundtrip(MetricValue::EndTimestamp(set));
    }

    #[test]
    fn roundtrip_pruning_and_ratio() {
        let pruning = PruningMetrics::new();
        pruning.add_pruned(3);
        pruning.add_matched(5);
        pruning.add_fully_matched(2);
        let back = assert_value_roundtrip(MetricValue::PruningMetrics {
            name: Cow::Borrowed("file_prune"),
            pruning_metrics: pruning,
        });
        match back {
            MetricValue::PruningMetrics {
                name,
                pruning_metrics,
            } => {
                assert_eq!(name.as_ref(), "file_prune");
                assert_eq!(pruning_metrics.pruned(), 3);
                assert_eq!(pruning_metrics.matched(), 5);
                assert_eq!(pruning_metrics.fully_matched(), 2);
            }
            other => panic!("expected PruningMetrics, got {other:?}"),
        }

        let ratio = RatioMetrics::new()
            .with_merge_strategy(RatioMergeStrategy::SetPartAddTotal)
            .with_display_raw_values(false);
        ratio.set_part(20);
        ratio.set_total(100);
        // `RatioMetrics`'s `PartialEq` does not compare `merge_strategy`,
        // so assert it explicitly after the round-trip.
        let back = assert_value_roundtrip(MetricValue::Ratio {
            name: Cow::Borrowed("selectivity"),
            ratio_metrics: ratio,
        });
        match back {
            MetricValue::Ratio {
                name,
                ratio_metrics,
            } => {
                assert_eq!(name.as_ref(), "selectivity");
                assert_eq!(ratio_metrics.part(), 20);
                assert_eq!(ratio_metrics.total(), 100);
                assert!(matches!(
                    ratio_metrics.merge_strategy(),
                    RatioMergeStrategy::SetPartAddTotal
                ));
                assert!(!ratio_metrics.display_raw_values());
            }
            other => panic!("expected Ratio, got {other:?}"),
        }
    }

    #[test]
    fn custom_metric_value_stringifies() {
        #[derive(Debug)]
        struct DummyCustom;
        impl Display for DummyCustom {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "dummy:42")
            }
        }
        impl CustomMetricValue for DummyCustom {
            fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
                Arc::new(DummyCustom)
            }
            fn aggregate(&self, _other: Arc<dyn CustomMetricValue>) {}
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn as_usize(&self) -> usize {
                42
            }
            fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
                other.as_any().downcast_ref::<Self>().is_some()
            }
        }

        let original = MetricValue::Custom {
            name: Cow::Borrowed("dummy"),
            value: Arc::new(DummyCustom),
        };
        let ffi: FFI_MetricValue = (&original).into();
        let back: MetricValue = ffi.into();

        match back {
            MetricValue::Custom { name, value } => {
                assert_eq!(name.as_ref(), "dummy");
                assert_eq!(value.to_string(), "dummy:42");
                assert_eq!(value.as_usize(), 42);
            }
            other => panic!("expected Custom, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_full_metric_with_labels_partition_type_category() {
        let count = Count::new();
        count.add(99);
        let metric = Metric::new_with_labels(
            MetricValue::OutputRows(count),
            Some(3),
            vec![Label::new("file", "a.parquet")],
        )
        .with_type(MetricType::Summary)
        .with_category(MetricCategory::Rows);

        let ffi: FFI_Metric = (&metric).into();
        let back: Metric = ffi.into();

        assert_eq!(back.value(), metric.value());
        assert_eq!(back.partition(), Some(3));
        assert_eq!(back.labels().len(), 1);
        assert_eq!(back.labels()[0].name(), "file");
        assert_eq!(back.labels()[0].value(), "a.parquet");
        assert_eq!(back.metric_type(), MetricType::Summary);
        assert_eq!(back.metric_category(), Some(MetricCategory::Rows));
    }

    #[test]
    fn roundtrip_metrics_set() {
        let mut set = MetricsSet::new();
        let c = Count::new();
        c.add(1);
        set.push(Arc::new(Metric::new(MetricValue::OutputRows(c), Some(0))));
        let c2 = Count::new();
        c2.add(2);
        set.push(Arc::new(Metric::new(MetricValue::OutputRows(c2), Some(1))));

        let ffi: FFI_MetricsSet = (&set).into();
        let back: MetricsSet = ffi.into();

        // Aggregate check.
        assert_eq!(back.output_rows(), Some(3));

        // Per-metric check: ordering, partition, and per-partition value must
        // all survive the round-trip (the aggregate above can't catch a lost
        // partition or a metric count mismatch).
        let metrics: Vec<_> = back.iter().collect();
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].partition(), Some(0));
        assert_eq!(
            metrics[0].value(),
            &MetricValue::OutputRows(count_from_value(1))
        );
        assert_eq!(metrics[1].partition(), Some(1));
        assert_eq!(
            metrics[1].value(),
            &MetricValue::OutputRows(count_from_value(2))
        );
    }
}
