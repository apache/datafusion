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

//! Extension hooks for refining a [`ParquetAccessPlan`] during file open.
//!
//! The Parquet opener narrows down which row groups (and which rows within
//! them) it will read through a fixed sequence of built-in passes:
//!
//! - file-range pruning,
//! - row-group statistics pruning,
//! - bloom-filter pruning,
//! - limit-based pruning,
//! - page-index pruning.
//!
//! Each pass operates on a [`ParquetAccessPlan`]. This module exposes the
//! pipeline as two stage-specific hooks so external crates can contribute
//! additional passes — sampling, custom statistics, user-defined Parquet
//! indexes — without forking the opener.
//!
//! # Hook stages
//!
//! - [`PostMetadataAccessPlanHook`] runs after the Parquet footer and
//!   page index (when enabled) are loaded, and after the built-in
//!   file-range and row-group-statistics passes have refined the plan.
//!   The opener also pre-registers the built-in bloom-filter pruning
//!   pass as a hook at this stage when `enable_bloom_filter` is set.
//! - [`PreBuildStreamAccessPlanHook`] runs after the built-in limit and
//!   page-index passes, just before the reader stream is constructed.
//!
//! # Hook is a state machine
//!
//! A hook may do multiple steps of CPU and I/O work — for instance,
//! "fetch an external index" (I/O) followed by "apply pruning using the
//! fetched data" (CPU). To preserve the opener's CPU/I/O routing, each
//! hook is itself a small state machine driven by the opener:
//!
//! 1. The opener calls [`PostMetadataAccessPlanHook::begin`] to obtain a
//!    stateful [`PostMetadataHookInstance`].
//! 2. The opener calls [`PostMetadataHookInstance::step`] on the
//!    **CPU pool**. Any CPU work the hook needs to do happens
//!    synchronously inside `step`. The hook returns either
//!    [`PostMetadataHookStep::Done`] (final context) or
//!    [`PostMetadataHookStep::Yield`] (a future to await on the
//!    **I/O pool**).
//! 3. On [`Yield`](PostMetadataHookStep::Yield), the opener awaits the
//!    returned future on its I/O pool. The future returns
//!    `(ctx, next_instance)`; the opener then re-enters `step` on the
//!    CPU pool with the next instance.
//! 4. On [`Done`](PostMetadataHookStep::Done), the opener moves on to
//!    the next registered hook (if any), or to the next built-in stage.
//!
//! Each "state" of a hook can be a different type — `MyHookStart`,
//! `MyHookLoaded { index_data }`, etc. — that implements
//! `PostMetadataHookInstance`. The instance morphs by returning the
//! next type. State that the I/O future produces (e.g., a loaded
//! index) becomes a field on the next instance type.
//!
//! # Doing I/O against the parquet file
//!
//! Both contexts expose a [`SharedAsyncFileReader`] that's cheaply
//! cloneable. Hook I/O futures can use it directly — they see the same
//! warm state the opener's primary reader has, with no need to call the
//! [`ParquetFileReaderFactory`] for a fresh reader.

use std::fmt::Debug;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion_common::Result;
use datafusion_datasource::{FileRange, PartitionedFile};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_pruning::PruningPredicate;
use futures::future::BoxFuture;
use parquet::file::metadata::ParquetMetaData;

use crate::ParquetAccessPlan;
use crate::ParquetFileMetrics;
use crate::ParquetFileReaderFactory;
use crate::SharedAsyncFileReader;
use crate::page_filter::PagePruningAccessPlanFilter;

// =====================================================================
// PostMetadata stage
// =====================================================================

/// Context for a [`PostMetadataAccessPlanHook`].
///
/// Available after the Parquet footer and page index (when enabled) have
/// been loaded, and after the built-in file-range and
/// row-group-statistics passes have refined the plan. Bloom filters have
/// **not** been loaded yet — the built-in bloom-filter pruning pass is
/// itself a hook at this stage.
#[derive(Debug)]
pub struct PostMetadataContext {
    /// Access plan refined so far. Hooks mutate this in place.
    pub plan: ParquetAccessPlan,
    /// Execution partition index for the scan.
    pub partition_index: usize,
    /// The file being opened.
    pub partitioned_file: PartitionedFile,
    /// Optional byte range restricting which part of the file to read.
    pub file_range: Option<FileRange>,
    /// Schema of the file after type coercions.
    pub physical_file_schema: SchemaRef,
    /// Loaded Parquet metadata, including page index when enabled.
    pub file_metadata: Arc<ParquetMetaData>,
    /// Raw predicate applied to this scan, if any.
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Row-group-level pruning predicate derived from `predicate`.
    pub pruning_predicate: Option<Arc<PruningPredicate>>,
    /// Page-index pruning predicate derived from `predicate`. The
    /// built-in page-index pass has **not** run yet at this stage.
    pub page_pruning_predicate: Option<Arc<PagePruningAccessPlanFilter>>,
    /// Outer query limit, if any. The built-in limit pass has **not**
    /// run yet at this stage.
    pub limit: Option<usize>,
    /// Whether the query requires the original row order to be preserved.
    pub preserve_order: bool,
    /// Per-file metrics. Hooks may emit to these counters.
    pub file_metrics: ParquetFileMetrics,
    /// Cheaply cloneable reader for the opened file. Hook I/O futures
    /// should clone this rather than asking
    /// [`Self::parquet_file_reader_factory`] for a fresh reader, so they
    /// share warm state with the opener's primary reader.
    pub async_file_reader: SharedAsyncFileReader,
    /// Factory used by the opener to create the primary reader. Hooks
    /// that need an *additional, independent* reader can obtain one here.
    pub parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    /// Hint forwarded to
    /// [`Self::parquet_file_reader_factory`]`::create_reader`.
    pub metadata_size_hint: Option<usize>,
    /// Plan-wide metrics set, for `MetricBuilder` use by hooks.
    pub metrics: ExecutionPlanMetricsSet,
}

/// Future returned by [`PostMetadataHookStep::Yield`].
pub type PostMetadataHookYieldFuture = BoxFuture<
    'static,
    Result<(Box<PostMetadataContext>, Box<dyn PostMetadataHookInstance>)>,
>;

/// One step of a [`PostMetadataHookInstance`].
pub enum PostMetadataHookStep {
    /// Hook needs I/O. The opener awaits the future on its I/O pool;
    /// the future returns the updated context and the next instance,
    /// which the opener then drives on the CPU pool.
    Yield(PostMetadataHookYieldFuture),
    /// Hook is finished. `ctx` contains the final mutations.
    Done(Box<PostMetadataContext>),
}

/// A stateful instance of a [`PostMetadataAccessPlanHook`], produced by
/// [`PostMetadataAccessPlanHook::begin`].
///
/// The opener calls [`step`](Self::step) on the CPU pool. Any CPU work
/// happens inside `step`. To do I/O the hook returns
/// [`PostMetadataHookStep::Yield`]; the opener awaits the future on the
/// I/O pool and re-enters `step` on the next instance.
pub trait PostMetadataHookInstance: Debug + Send {
    /// Drive one step. Consumes the current instance.
    fn step(
        self: Box<Self>,
        ctx: Box<PostMetadataContext>,
    ) -> Result<PostMetadataHookStep>;
}

/// Factory trait for [`PostMetadataHookInstance`]s. One factory is
/// registered on [`ParquetSource`](crate::source::ParquetSource); the
/// opener calls [`begin`](Self::begin) once per file open.
pub trait PostMetadataAccessPlanHook: Debug + Send + Sync {
    fn begin(&self) -> Box<dyn PostMetadataHookInstance>;
}

// =====================================================================
// PreBuildStream stage
// =====================================================================

/// Context for a [`PreBuildStreamAccessPlanHook`].
///
/// Available after **all** built-in pruning passes have run, just
/// before the reader stream is constructed.
#[derive(Debug)]
pub struct PreBuildStreamContext {
    /// Access plan refined so far. Hooks mutate this in place.
    pub plan: ParquetAccessPlan,
    /// Execution partition index for the scan.
    pub partition_index: usize,
    /// The file being opened.
    pub partitioned_file: PartitionedFile,
    /// Optional byte range restricting which part of the file to read.
    pub file_range: Option<FileRange>,
    /// Schema of the file after type coercions.
    pub physical_file_schema: SchemaRef,
    /// Loaded Parquet metadata.
    pub file_metadata: Arc<ParquetMetaData>,
    /// Raw predicate applied to this scan, if any.
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Row-group-level pruning predicate derived from `predicate`.
    pub pruning_predicate: Option<Arc<PruningPredicate>>,
    /// Outer query limit, if any. Already applied by the built-in
    /// limit pass.
    pub limit: Option<usize>,
    /// Whether the query requires the original row order to be preserved.
    pub preserve_order: bool,
    /// Per-file metrics.
    pub file_metrics: ParquetFileMetrics,
    /// Cheaply cloneable reader for the opened file.
    pub async_file_reader: SharedAsyncFileReader,
    /// Factory used by the opener to create the primary reader.
    pub parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    /// Hint forwarded to
    /// [`Self::parquet_file_reader_factory`]`::create_reader`.
    pub metadata_size_hint: Option<usize>,
    /// Plan-wide metrics set.
    pub metrics: ExecutionPlanMetricsSet,
}

/// Future returned by [`PreBuildStreamHookStep::Yield`].
pub type PreBuildStreamHookYieldFuture = BoxFuture<
    'static,
    Result<(
        Box<PreBuildStreamContext>,
        Box<dyn PreBuildStreamHookInstance>,
    )>,
>;

/// One step of a [`PreBuildStreamHookInstance`]. See [`PostMetadataHookStep`].
pub enum PreBuildStreamHookStep {
    Yield(PreBuildStreamHookYieldFuture),
    Done(Box<PreBuildStreamContext>),
}

/// A stateful instance of a [`PreBuildStreamAccessPlanHook`]. See
/// [`PostMetadataHookInstance`] for the protocol.
pub trait PreBuildStreamHookInstance: Debug + Send {
    fn step(
        self: Box<Self>,
        ctx: Box<PreBuildStreamContext>,
    ) -> Result<PreBuildStreamHookStep>;
}

/// Factory trait for [`PreBuildStreamHookInstance`]s.
pub trait PreBuildStreamAccessPlanHook: Debug + Send + Sync {
    fn begin(&self) -> Box<dyn PreBuildStreamHookInstance>;
}
