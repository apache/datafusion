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

//! Common behaviors that every file format needs to implement

use std::any::Any;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::file_groups::FileGroupPartitioner;
use crate::file_scan_config::FileScanConfig;
use crate::file_stream::FileOpener;
#[expect(deprecated)]
use crate::schema_adapter::SchemaAdapterFactory;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, not_impl_err};
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr::{EquivalenceProperties, LexOrdering, PhysicalExpr};
use datafusion_physical_plan::DisplayFormatType;
use datafusion_physical_plan::SortOrderPushdownResult;
use datafusion_physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;

use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
use object_store::ObjectStore;

/// Helper function to convert any type implementing [`FileSource`] to `Arc<dyn FileSource>`
pub fn as_file_source<T: FileSource + 'static>(source: T) -> Arc<dyn FileSource> {
    Arc::new(source)
}

/// File format specific behaviors for [`DataSource`]
///
/// # Schema information
/// There are two important schemas for a [`FileSource`]:
/// 1. [`Self::table_schema`] -- the schema for the overall table
///    (file data plus partition columns)
/// 2. The logical output schema, comprised of [`Self::table_schema`] with
///    [`Self::projection`] applied
///
/// See more details on specific implementations:
/// * [`ArrowSource`](https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.ArrowSource.html)
/// * [`AvroSource`](https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.AvroSource.html)
/// * [`CsvSource`](https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.CsvSource.html)
/// * [`JsonSource`](https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.JsonSource.html)
/// * [`ParquetSource`](https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.ParquetSource.html)
///
/// [`DataSource`]: crate::source::DataSource
pub trait FileSource: Send + Sync {
    /// Creates a `dyn FileOpener` based on given parameters
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>>;
    /// Any
    fn as_any(&self) -> &dyn Any;

    /// Returns the table schema for the overall table (including partition columns, if any)
    ///
    /// This method returns the unprojected schema: the full schema of the data
    /// without [`Self::projection`] applied.
    ///
    /// The output schema of this `FileSource` is this TableSchema
    /// with [`Self::projection`] applied.
    ///
    /// Use [`ProjectionExprs::project_schema`] to get the projected schema
    /// after applying the projection.
    fn table_schema(&self) -> &crate::table_schema::TableSchema;

    /// Initialize new type with batch size configuration
    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource>;

    /// Returns the filter expression that will be applied *during* the file scan.
    ///
    /// These expressions are in terms of the unprojected [`Self::table_schema`].
    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        None
    }

    /// Return the projection that will be applied to the output stream on top
    /// of [`Self::table_schema`].
    ///
    /// Note you can use [`ProjectionExprs::project_schema`] on the table
    /// schema to get the effective output schema of this source.
    fn projection(&self) -> Option<&ProjectionExprs> {
        None
    }

    /// Return execution plan metrics
    fn metrics(&self) -> &ExecutionPlanMetricsSet;

    /// String representation of file source such as "csv", "json", "parquet"
    fn file_type(&self) -> &str;

    /// Format FileType specific information
    fn fmt_extra(&self, _t: DisplayFormatType, _f: &mut Formatter) -> fmt::Result {
        Ok(())
    }

    /// Returns whether this file source supports repartitioning files by byte ranges.
    ///
    /// When this returns `true`, files can be split into multiple partitions
    /// based on byte offsets for parallel reading.
    ///
    /// When this returns `false`, files cannot be repartitioned (e.g., CSV files
    /// with `newlines_in_values` enabled cannot be split because record boundaries
    /// cannot be determined by byte offset alone).
    ///
    /// The default implementation returns `true`. File sources that cannot support
    /// repartitioning should override this method.
    fn supports_repartitioning(&self) -> bool {
        true
    }

    /// If supported by the [`FileSource`], redistribute files across partitions
    /// according to their size. Allows custom file formats to implement their
    /// own repartitioning logic.
    ///
    /// The default implementation uses [`FileGroupPartitioner`]. See that
    /// struct for more details.
    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
        config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        if config.file_compression_type.is_compressed() || !self.supports_repartitioning()
        {
            return Ok(None);
        }

        let repartitioned_file_groups_option = FileGroupPartitioner::new()
            .with_target_partitions(target_partitions)
            .with_repartition_file_min_size(repartition_file_min_size)
            .with_preserve_order_within_groups(output_ordering.is_some())
            .repartition_file_groups(&config.file_groups);

        if let Some(repartitioned_file_groups) = repartitioned_file_groups_option {
            let mut source = config.clone();
            source.file_groups = repartitioned_file_groups;
            return Ok(Some(source));
        }
        Ok(None)
    }

    /// Try to push down filters into this FileSource.
    ///
    /// `filters` must be in terms of the unprojected table schema (file schema
    /// plus partition columns), before any projection is applied.
    ///
    /// Any filters that this FileSource chooses to evaluate itself should be
    /// returned as `PushedDown::Yes` in the result, along with a FileSource
    /// instance that incorporates those filters. Such filters are logically
    /// applied "during" the file scan, meaning they may refer to columns not
    /// included in the final output projection.
    ///
    /// Filters that cannot be pushed down should be marked as `PushedDown::No`,
    /// and will be evaluated by an execution plan after the file source.
    ///
    /// See [`ExecutionPlan::handle_child_pushdown_result`] for more details.
    ///
    /// [`ExecutionPlan::handle_child_pushdown_result`]: datafusion_physical_plan::ExecutionPlan::handle_child_pushdown_result
    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        Ok(FilterPushdownPropagation::with_parent_pushdown_result(
            vec![PushedDown::No; filters.len()],
        ))
    }

    /// Try to create a new FileSource that can produce data in the specified sort order.
    ///
    /// This method attempts to optimize data retrieval to match the requested ordering.
    /// It receives both the requested ordering and equivalence properties that describe
    /// the output data from this file source.
    ///
    /// # Parameters
    /// * `order` - The requested sort ordering from the query
    /// * `eq_properties` - Equivalence properties of the data that will be produced by this
    ///   file source. These properties describe the ordering, constant columns, and other
    ///   relationships in the output data, allowing the implementation to determine if
    ///   optimizations like reversed scanning can help satisfy the requested ordering.
    ///   This includes information about:
    ///   - The file's natural ordering (from output_ordering in FileScanConfig)
    ///   - Constant columns (e.g., from filters like `ticker = 'AAPL'`)
    ///   - Monotonic functions (e.g., `extract_year_month(timestamp)`)
    ///   - Other equivalence relationships
    ///
    /// # Examples
    ///
    /// ## Example 1: Simple reverse
    /// ```text
    /// File ordering: [a ASC, b DESC]
    /// Requested:     [a DESC]
    /// Reversed file: [a DESC, b ASC]
    /// Result: Satisfies request (prefix match) → Inexact
    /// ```
    ///
    /// ## Example 2: Monotonic function
    /// ```text
    /// File ordering: [extract_year_month(ts) ASC, ts ASC]
    /// Requested:     [ts DESC]
    /// Reversed file: [extract_year_month(ts) DESC, ts DESC]
    /// Result: Through monotonicity, satisfies [ts DESC] → Inexact
    /// ```
    ///
    /// # Returns
    /// * `Exact` - Created a source that guarantees perfect ordering
    /// * `Inexact` - Created a source optimized for ordering (e.g., reversed row groups) but not perfectly sorted
    /// * `Unsupported` - Cannot optimize for this ordering
    ///
    /// # Deprecation / migration notes
    /// - [`Self::try_reverse_output`] was renamed to this method and deprecated since `53.0.0`.
    ///   Per DataFusion's deprecation guidelines, it will be removed in `59.0.0` or later
    ///   (6 major versions or 6 months, whichever is longer).
    /// - New implementations should override [`Self::try_pushdown_sort`] directly.
    /// - For backwards compatibility, the default implementation of
    ///   [`Self::try_pushdown_sort`] delegates to the deprecated
    ///   [`Self::try_reverse_output`] until it is removed. After that point, the
    ///   default implementation will return [`SortOrderPushdownResult::Unsupported`].
    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
        eq_properties: &EquivalenceProperties,
    ) -> Result<SortOrderPushdownResult<Arc<dyn FileSource>>> {
        #[expect(deprecated)]
        self.try_reverse_output(order, eq_properties)
    }

    /// Deprecated: Renamed to [`Self::try_pushdown_sort`].
    #[deprecated(
        since = "53.0.0",
        note = "Renamed to try_pushdown_sort. This method was never limited to reversing output. It will be removed in 59.0.0 or later."
    )]
    fn try_reverse_output(
        &self,
        _order: &[PhysicalSortExpr],
        _eq_properties: &EquivalenceProperties,
    ) -> Result<SortOrderPushdownResult<Arc<dyn FileSource>>> {
        Ok(SortOrderPushdownResult::Unsupported)
    }

    /// Try to push down a projection into this FileSource.
    ///
    /// `FileSource` implementations that support projection pushdown should
    /// override this method and return a new `FileSource` instance with the
    /// projection incorporated.
    ///
    /// If a `FileSource` does accept a projection it is expected to handle
    /// the projection in it's entirety, including partition columns.
    /// For example, the `FileSource` may translate that projection into a
    /// file format specific projection (e.g. Parquet can push down struct field access,
    /// some other file formats like Vortex can push down computed expressions into un-decoded data)
    /// and also need to handle partition column projection (generally done by replacing partition column
    /// references with literal values derived from each files partition values).
    ///
    /// Not all FileSource's can handle complex expression pushdowns. For example,
    /// a CSV file source may only support simple column selections. In such cases,
    /// the `FileSource` can use [`SplitProjection`] and [`ProjectionOpener`]
    /// to split the projection into a pushdownable part and a non-pushdownable part.
    /// These helpers also handle partition column projection.
    ///
    /// [`SplitProjection`]: crate::projection::SplitProjection
    /// [`ProjectionOpener`]: crate::projection::ProjectionOpener
    fn try_pushdown_projection(
        &self,
        _projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        Ok(None)
    }

    /// Deprecated: Set optional schema adapter factory.
    ///
    /// `SchemaAdapterFactory` has been removed. Use `PhysicalExprAdapterFactory` instead.
    /// See `upgrading.md` for more details.
    #[deprecated(
        since = "53.0.0",
        note = "SchemaAdapterFactory has been removed. Use PhysicalExprAdapterFactory instead. See upgrading.md for more details."
    )]
    #[expect(deprecated)]
    fn with_schema_adapter_factory(
        &self,
        _factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        not_impl_err!(
            "SchemaAdapterFactory has been removed. Use PhysicalExprAdapterFactory instead. See upgrading.md for more details."
        )
    }

    /// Deprecated: Returns the current schema adapter factory if set.
    ///
    /// `SchemaAdapterFactory` has been removed. Use `PhysicalExprAdapterFactory` instead.
    /// See `upgrading.md` for more details.
    #[deprecated(
        since = "53.0.0",
        note = "SchemaAdapterFactory has been removed. Use PhysicalExprAdapterFactory instead. See upgrading.md for more details."
    )]
    #[expect(deprecated)]
    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        None
    }
}
