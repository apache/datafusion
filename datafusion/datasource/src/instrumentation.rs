// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Application-defined instrumentation for [`DataSourceExec`](crate::source::DataSourceExec).

use std::fmt::Debug;
use std::sync::Arc;

use datafusion_common::Result;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;

use crate::source::DataSource;

/// Opens one partition and applies `DataSourceExec`'s output batch splitting.
///
/// Opening is synchronous; the returned stream performs the asynchronous reads.
pub type DataSourceOpen<'a> =
    Box<dyn FnOnce() -> Result<SendableRecordBatchStream> + Send + 'a>;

/// Instruments the opening and output stream of a [`DataSourceExec`](crate::source::DataSourceExec).
///
/// Implementations must call `open` exactly once and preserve the returned stream's
/// schema, batches, errors, and polling behavior. They can establish context while
/// opening, observe opening errors, and wrap the stream to record batches, stream
/// errors, completion, or cancellation. Stream wrappers should establish any
/// thread-local context separately on each poll rather than retain a context guard.
///
/// Register an implementation using [`DataSourceInstrumentationExtension`].
/// This hook covers sources executed through `DataSourceExec`, not arbitrary
/// execution plans or direct calls to `DataSource::open`.
pub trait DataSourceInstrumentation: Debug + Send + Sync {
    /// Instruments one partition using the executing task's context.
    ///
    /// `source` allows applications to select which sources to instrument.
    /// `metrics` is owned by the executor, independently of the source's metrics,
    /// and is included in `ExecutionPlan::metrics()`. Use distinct metric names to
    /// avoid ambiguity with source metrics. Clones of the plan share these metrics;
    /// resetting execution state or replacing the source creates a fresh set.
    ///
    /// The callback returns batches after source-level limits and output batch
    /// splitting, before downstream operators. Stream lifetime includes consumer
    /// backpressure; it is not individual storage-request latency.
    fn instrument(
        &self,
        source: &dyn DataSource,
        partition: usize,
        context: &TaskContext,
        metrics: &ExecutionPlanMetricsSet,
        open: DataSourceOpen<'_>,
    ) -> Result<SendableRecordBatchStream>;
}

/// Session extension holding an application-defined data source instrumentation hook.
///
/// Install with [`SessionConfig::with_extension`](datafusion_execution::config::SessionConfig::with_extension).
/// The hook is resolved at execution time and is not part of the serialized plan.
/// Applications executing plans in other processes must install it in those sessions too.
/// Without this extension, `DataSourceExec` opens its source directly.
///
/// # Example
///
/// ```
/// use std::sync::Arc;
/// use datafusion_common::Result;
/// use datafusion_execution::{SendableRecordBatchStream, TaskContext};
/// use datafusion_execution::config::SessionConfig;
/// use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
/// use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
/// use datafusion_datasource::source::DataSource;
/// use datafusion_datasource::instrumentation::{
///     DataSourceInstrumentation, DataSourceInstrumentationExtension, DataSourceOpen,
/// };
/// use futures::TryStreamExt;
///
/// #[derive(Debug)]
/// struct ScanMetrics;
///
/// impl DataSourceInstrumentation for ScanMetrics {
///     fn instrument(
///         &self,
///         _source: &dyn DataSource,
///         partition: usize,
///         _context: &TaskContext,
///         metrics: &ExecutionPlanMetricsSet,
///         open: DataSourceOpen<'_>,
///     ) -> Result<SendableRecordBatchStream> {
///         let stream = open()?;
///         let schema = stream.schema();
///         let rows = MetricBuilder::new(metrics)
///             .with_new_label("application", "example")
///             .counter("observed_rows", partition);
///         let stream = stream.inspect_ok(move |batch| rows.add(batch.num_rows()));
///         Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
///     }
/// }
///
/// let config = SessionConfig::new().with_extension(Arc::new(
///     DataSourceInstrumentationExtension(Arc::new(ScanMetrics)),
/// ));
/// ```
#[derive(Debug, Clone)]
pub struct DataSourceInstrumentationExtension(pub Arc<dyn DataSourceInstrumentation>);
