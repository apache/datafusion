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

//! Utilities to push down of DataFusion filter predicates (any DataFusion
//! `PhysicalExpr` that evaluates to a [`BooleanArray`]) to the parquet decoder
//! level in `arrow-rs`.
//!
//! DataFusion will use a `ParquetRecordBatchStream` to read data from parquet
//! into [`RecordBatch`]es.
//!
//! The `ParquetRecordBatchStream` takes an optional `RowFilter` which is itself
//! a Vec of `Box<dyn ArrowPredicate>`. During decoding, the predicates are
//! evaluated in order, to generate a mask which is used to avoid decoding rows
//! in projected columns which do not pass the filter which can significantly
//! reduce the amount of compute required for decoding and thus improve query
//! performance.
//!
//! Since the predicates are applied serially in the order defined in the
//! `RowFilter`, the optimal ordering depends on the exact filters. The best
//! filters to execute first have two properties:
//!
//! 1. They are relatively inexpensive to evaluate (e.g. they read
//!    column chunks which are relatively small)
//!
//! 2. They filter many (contiguous) rows, reducing the amount of decoding
//!    required for subsequent filters and projected columns
//!
//! If requested, this code will reorder the filters based on heuristics try and
//! reduce the evaluation cost.
//!
//! The basic algorithm for constructing the `RowFilter` is as follows
//!
//! 1. Break conjunctions into separate predicates. An expression
//!    like `a = 1 AND (b = 2 AND c = 3)` would be
//!    separated into the expressions `a = 1`, `b = 2`, and `c = 3`.
//! 2. Determine whether each predicate can be evaluated as an `ArrowPredicate`.
//! 3. Determine, for each predicate, the total compressed size of all
//!    columns required to evaluate the predicate.
//! 4. Determine, for each predicate, whether all columns required to
//!    evaluate the expression are sorted.
//! 5. Re-order the predicate by total size (from step 3).
//! 6. Partition the predicates according to whether they are sorted (from step 4)
//! 7. "Compile" each predicate `Expr` to a `DatafusionArrowPredicate`.
//! 8. Build the `RowFilter` with the sorted predicates followed by
//!    the unsorted predicates. Within each partition, predicates are
//!    still be sorted by size.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ArrowPredicate, RowFilter};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;

use datafusion_common::cast::as_boolean_array;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::Result;
use datafusion_datasource::schema_adapter::{SchemaAdapterFactory, SchemaMapper};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr::{split_conjunction, PhysicalExpr};

use datafusion_physical_plan::metrics;

use super::ParquetFileMetrics;

/// A "compiled" predicate passed to `ParquetRecordBatchStream` to perform
/// row-level filtering during parquet decoding.
///
/// See the module level documentation for more information.
///
/// Implements the `ArrowPredicate` trait used by the parquet decoder
///
/// An expression can be evaluated as a `DatafusionArrowPredicate` if it:
/// * Does not reference any projected columns
/// * Does not reference columns with non-primitive types (e.g. structs / lists)
#[derive(Debug)]
pub(crate) struct DatafusionArrowPredicate {
    /// the filter expression
    physical_expr: Arc<dyn PhysicalExpr>,
    /// Path to the columns in the parquet schema required to evaluate the
    /// expression
    projection_mask: ProjectionMask,
    /// how many rows were filtered out by this predicate
    rows_pruned: metrics::Count,
    /// how many rows passed this predicate
    rows_matched: metrics::Count,
    /// how long was spent evaluating this predicate
    time: metrics::Time,
    /// used to perform type coercion while filtering rows
    schema_mapper: Arc<dyn SchemaMapper>,
}

impl DatafusionArrowPredicate {
    /// Create a new `DatafusionArrowPredicate` from a `FilterCandidate`
    pub fn try_new(
        candidate: FilterCandidate,
        metadata: &ParquetMetaData,
        rows_pruned: metrics::Count,
        rows_matched: metrics::Count,
        time: metrics::Time,
    ) -> Result<Self> {
        let physical_expr =
            reassign_expr_columns(candidate.expr, &candidate.filter_schema)?;

        Ok(Self {
            physical_expr,
            projection_mask: ProjectionMask::roots(
                metadata.file_metadata().schema_descr(),
                candidate.projection,
            ),
            rows_pruned,
            rows_matched,
            time,
            schema_mapper: candidate.schema_mapper,
        })
    }
}

impl ArrowPredicate for DatafusionArrowPredicate {
    fn projection(&self) -> &ProjectionMask {
        &self.projection_mask
    }

    fn evaluate(&mut self, batch: RecordBatch) -> ArrowResult<BooleanArray> {
        let batch = self.schema_mapper.map_batch(batch)?;

        // scoped timer updates on drop
        let mut timer = self.time.timer();

        self.physical_expr
            .evaluate(&batch)
            .and_then(|v| v.into_array(batch.num_rows()))
            .and_then(|array| {
                let bool_arr = as_boolean_array(&array)?.clone();
                let num_matched = bool_arr.true_count();
                let num_pruned = bool_arr.len() - num_matched;
                self.rows_pruned.add(num_pruned);
                self.rows_matched.add(num_matched);
                timer.stop();
                Ok(bool_arr)
            })
            .map_err(|e| {
                ArrowError::ComputeError(format!(
                    "Error evaluating filter predicate: {e:?}"
                ))
            })
    }
}

/// A candidate expression for creating a `RowFilter`.
///
/// Each candidate contains the expression as well as data to estimate the cost
/// of evaluating the resulting expression.
///
/// See the module level documentation for more information.
pub(crate) struct FilterCandidate {
    expr: Arc<dyn PhysicalExpr>,
    /// Estimate for the total number of bytes that will need to be processed
    /// to evaluate this filter. This is used to estimate the cost of evaluating
    /// the filter and to order the filters when `reorder_predicates` is true.
    /// This is generated by summing the compressed size of all columns that the filter references.
    required_bytes: usize,
    /// Can this filter use an index (e.g. a page index) to prune rows?
    can_use_index: bool,
    /// The projection to read from the file schema to get the columns
    /// required to pass through a `SchemaMapper` to the table schema
    /// upon which we then evaluate the filter expression.
    projection: Vec<usize>,
    ///  A `SchemaMapper` used to map batches read from the file schema to
    /// the filter's projection of the table schema.
    schema_mapper: Arc<dyn SchemaMapper>,
    /// The projected table schema that this filter references
    filter_schema: SchemaRef,
}

/// Helper to build a `FilterCandidate`.
///
/// This will do several things
/// 1. Determine the columns required to evaluate the expression
/// 2. Calculate data required to estimate the cost of evaluating the filter
/// 3. Rewrite column expressions in the predicate which reference columns not
///    in the particular file schema.
///
/// # Schema Rewrite
///
/// When parquet files are read in the context of "schema evolution" there are
/// potentially wo schemas:
///
/// 1. The table schema (the columns of the table that the parquet file is part of)
/// 2. The file schema (the columns actually in the parquet file)
///
/// There are times when the table schema contains columns that are not in the
/// file schema, such as when new columns have been added in new parquet files
/// but old files do not have the columns.
///
/// When a file is missing a column from the table schema, the value of the
/// missing column is filled in by a `SchemaAdapter` (by default as `NULL`).
///
/// When a predicate is pushed down to the parquet reader, the predicate is
/// evaluated in the context of the file schema.
/// For each predicate we build a filter schema which is the projection of the table
/// schema that contains only the columns that this filter references.
/// If any columns from the file schema are missing from a particular file they are
/// added by the `SchemaAdapter`, by default as `NULL`.
struct FilterCandidateBuilder {
    expr: Arc<dyn PhysicalExpr>,
    /// The schema of this parquet file.
    /// Columns may have different types from the table schema and there may be
    /// columns in the file schema that are not in the table schema or columns that
    /// are in the table schema that are not in the file schema.
    file_schema: SchemaRef,
    /// The schema of the table (merged schema) -- columns may be in different
    /// order than in the file and have columns that are not in the file schema
    table_schema: SchemaRef,
    /// A `SchemaAdapterFactory` used to map the file schema to the table schema.
    schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
}

impl FilterCandidateBuilder {
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        file_schema: Arc<Schema>,
        table_schema: Arc<Schema>,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Self {
        Self {
            expr,
            file_schema,
            table_schema,
            schema_adapter_factory,
        }
    }

    /// Attempt to build a `FilterCandidate` from the expression
    ///
    /// # Return values
    ///
    /// * `Ok(Some(candidate))` if the expression can be used as an ArrowFilter
    /// * `Ok(None)` if the expression cannot be used as an ArrowFilter
    /// * `Err(e)` if an error occurs while building the candidate
    pub fn build(self, metadata: &ParquetMetaData) -> Result<Option<FilterCandidate>> {
        let Some(required_indices_into_table_schema) =
            pushdown_columns(&self.expr, &self.table_schema)?
        else {
            return Ok(None);
        };

        let projected_table_schema = Arc::new(
            self.table_schema
                .project(&required_indices_into_table_schema)?,
        );

        let (schema_mapper, projection_into_file_schema) = self
            .schema_adapter_factory
            .create(Arc::clone(&projected_table_schema), self.table_schema)
            .map_schema(&self.file_schema)?;

        let required_bytes = size_of_columns(&projection_into_file_schema, metadata)?;
        let can_use_index = columns_sorted(&projection_into_file_schema, metadata)?;

        Ok(Some(FilterCandidate {
            expr: self.expr,
            required_bytes,
            can_use_index,
            projection: projection_into_file_schema,
            schema_mapper: Arc::clone(&schema_mapper),
            filter_schema: Arc::clone(&projected_table_schema),
        }))
    }
}

// a struct that implements TreeNodeRewriter to traverse a PhysicalExpr tree structure to determine
// if any column references in the expression would prevent it from being predicate-pushed-down.
// if non_primitive_columns || projected_columns, it can't be pushed down.
// can't be reused between calls to `rewrite`; each construction must be used only once.
struct PushdownChecker<'schema> {
    /// Does the expression require any non-primitive columns (like structs)?
    non_primitive_columns: bool,
    /// Does the expression reference any columns that are in the table
    /// schema but not in the file schema?
    /// This includes partition columns and projected columns.
    projected_columns: bool,
    // Indices into the table schema of the columns required to evaluate the expression
    required_columns: BTreeSet<usize>,
    table_schema: &'schema Schema,
}

impl<'schema> PushdownChecker<'schema> {
    fn new(table_schema: &'schema Schema) -> Self {
        Self {
            non_primitive_columns: false,
            projected_columns: false,
            required_columns: BTreeSet::default(),
            table_schema,
        }
    }

    fn check_single_column(&mut self, column_name: &str) -> Option<TreeNodeRecursion> {
        if let Ok(idx) = self.table_schema.index_of(column_name) {
            self.required_columns.insert(idx);
            if DataType::is_nested(self.table_schema.field(idx).data_type()) {
                self.non_primitive_columns = true;
                return Some(TreeNodeRecursion::Jump);
            }
        } else {
            // If the column does not exist in the (un-projected) table schema then
            // it must be a projected column.
            self.projected_columns = true;
            return Some(TreeNodeRecursion::Jump);
        }

        None
    }

    #[inline]
    fn prevents_pushdown(&self) -> bool {
        self.non_primitive_columns || self.projected_columns
    }
}

impl TreeNodeVisitor<'_> for PushdownChecker<'_> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            if let Some(recursion) = self.check_single_column(column.name()) {
                return Ok(recursion);
            }
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

// Checks if a given expression can be pushed down into `DataSourceExec` as opposed to being evaluated
// post-parquet-scan in a `FilterExec`. If it can be pushed down, this returns all the
// columns in the given expression so that they can be used in the parquet scanning, along with the
// expression rewritten as defined in [`PushdownChecker::f_up`]
fn pushdown_columns(
    expr: &Arc<dyn PhysicalExpr>,
    table_schema: &Schema,
) -> Result<Option<Vec<usize>>> {
    let mut checker = PushdownChecker::new(table_schema);
    expr.visit(&mut checker)?;
    Ok((!checker.prevents_pushdown())
        .then_some(checker.required_columns.into_iter().collect()))
}

/// Recurses through expr as a tree, finds all `column`s, and checks if any of them would prevent
/// this expression from being predicate pushed down. If any of them would, this returns false.
/// Otherwise, true.
/// Note that the schema passed in here is *not* the physical file schema (as it is not available at that point in time);
/// it is the schema of the table that this expression is being evaluated against minus any projected columns and partition columns.
pub fn can_expr_be_pushed_down_with_schemas(
    expr: &Arc<dyn PhysicalExpr>,
    file_schema: &Schema,
) -> bool {
    match pushdown_columns(expr, file_schema) {
        Ok(Some(_)) => true,
        Ok(None) | Err(_) => false,
    }
}

/// Calculate the total compressed size of all `Column`'s required for
/// predicate `Expr`.
///
/// This value represents the total amount of IO required to evaluate the
/// predicate.
fn size_of_columns(columns: &[usize], metadata: &ParquetMetaData) -> Result<usize> {
    let mut total_size = 0;
    let row_groups = metadata.row_groups();
    for idx in columns {
        for rg in row_groups.iter() {
            total_size += rg.column(*idx).compressed_size() as usize;
        }
    }

    Ok(total_size)
}

/// For a given set of `Column`s required for predicate `Expr` determine whether
/// all columns are sorted.
///
/// Sorted columns may be queried more efficiently in the presence of
/// a PageIndex.
fn columns_sorted(_columns: &[usize], _metadata: &ParquetMetaData) -> Result<bool> {
    // TODO How do we know this?
    Ok(false)
}

/// Build a [`RowFilter`] from the given predicate `Expr` if possible
///
/// # returns
/// * `Ok(Some(row_filter))` if the expression can be used as RowFilter
/// * `Ok(None)` if the expression cannot be used as an RowFilter
/// * `Err(e)` if an error occurs while building the filter
///
/// Note that the returned `RowFilter` may not contains all conjuncts in the
/// original expression. This is because some conjuncts may not be able to be
/// evaluated as an `ArrowPredicate` and will be ignored.
///
/// For example, if the expression is `a = 1 AND b = 2 AND c = 3` and `b = 2`
/// can not be evaluated for some reason, the returned `RowFilter` will contain
/// `a = 1` and `c = 3`.
pub fn build_row_filter(
    expr: &Arc<dyn PhysicalExpr>,
    physical_file_schema: &SchemaRef,
    predicate_file_schema: &SchemaRef,
    metadata: &ParquetMetaData,
    reorder_predicates: bool,
    file_metrics: &ParquetFileMetrics,
    schema_adapter_factory: &Arc<dyn SchemaAdapterFactory>,
) -> Result<Option<RowFilter>> {
    let rows_pruned = &file_metrics.pushdown_rows_pruned;
    let rows_matched = &file_metrics.pushdown_rows_matched;
    let time = &file_metrics.row_pushdown_eval_time;

    // Split into conjuncts:
    // `a = 1 AND b = 2 AND c = 3` -> [`a = 1`, `b = 2`, `c = 3`]
    let predicates = split_conjunction(expr);

    // Determine which conjuncts can be evaluated as ArrowPredicates, if any
    let mut candidates: Vec<FilterCandidate> = predicates
        .into_iter()
        .map(|expr| {
            FilterCandidateBuilder::new(
                Arc::clone(expr),
                Arc::clone(physical_file_schema),
                Arc::clone(predicate_file_schema),
                Arc::clone(schema_adapter_factory),
            )
            .build(metadata)
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect();

    // no candidates
    if candidates.is_empty() {
        return Ok(None);
    }

    if reorder_predicates {
        candidates.sort_unstable_by(|c1, c2| {
            match c1.can_use_index.cmp(&c2.can_use_index) {
                Ordering::Equal => c1.required_bytes.cmp(&c2.required_bytes),
                ord => ord,
            }
        });
    }

    // To avoid double-counting metrics when multiple predicates are used:
    // - All predicates should count rows_pruned (cumulative pruned rows)
    // - Only the last predicate should count rows_matched (final result)
    // This ensures: rows_matched + rows_pruned = total rows processed
    let total_candidates = candidates.len();

    candidates
        .into_iter()
        .enumerate()
        .map(|(idx, candidate)| {
            let is_last = idx == total_candidates - 1;

            // All predicates share the pruned counter (cumulative)
            let predicate_rows_pruned = rows_pruned.clone();

            // Only the last predicate tracks matched rows (final result)
            let predicate_rows_matched = if is_last {
                rows_matched.clone()
            } else {
                metrics::Count::new()
            };

            DatafusionArrowPredicate::try_new(
                candidate,
                metadata,
                predicate_rows_pruned,
                predicate_rows_matched,
                time.clone(),
            )
            .map(|pred| Box::new(pred) as _)
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|filters| Some(RowFilter::new(filters)))
}

#[cfg(test)]
mod test {
    use super::*;
    use datafusion_common::ScalarValue;

    use arrow::datatypes::{Field, TimeUnit::Nanosecond};
    use datafusion_datasource::schema_adapter::DefaultSchemaAdapterFactory;
    use datafusion_expr::{col, Expr};
    use datafusion_physical_expr::planner::logical2physical;
    use datafusion_physical_plan::metrics::{Count, Time};

    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::parquet_to_arrow_schema;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    // We should ignore predicate that read non-primitive columns
    #[test]
    fn test_filter_candidate_builder_ignore_complex_types() {
        let testdata = datafusion_common::test_util::parquet_test_data();
        let file = std::fs::File::open(format!("{testdata}/list_columns.parquet"))
            .expect("opening file");

        let reader = SerializedFileReader::new(file).expect("creating reader");

        let metadata = reader.metadata();

        let table_schema =
            parquet_to_arrow_schema(metadata.file_metadata().schema_descr(), None)
                .expect("parsing schema");

        let expr = col("int64_list").is_not_null();
        let expr = logical2physical(&expr, &table_schema);

        let schema_adapter_factory = Arc::new(DefaultSchemaAdapterFactory);
        let table_schema = Arc::new(table_schema.clone());

        let candidate = FilterCandidateBuilder::new(
            expr,
            table_schema.clone(),
            table_schema,
            schema_adapter_factory,
        )
        .build(metadata)
        .expect("building candidate");

        assert!(candidate.is_none());
    }

    #[test]
    fn test_filter_type_coercion() {
        let testdata = datafusion_common::test_util::parquet_test_data();
        let file = std::fs::File::open(format!("{testdata}/alltypes_plain.parquet"))
            .expect("opening file");

        let parquet_reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(file).expect("creating reader");
        let metadata = parquet_reader_builder.metadata().clone();
        let file_schema = parquet_reader_builder.schema().clone();

        // This is the schema we would like to coerce to,
        // which is different from the physical schema of the file.
        let table_schema = Schema::new(vec![Field::new(
            "timestamp_col",
            DataType::Timestamp(Nanosecond, Some(Arc::from("UTC"))),
            false,
        )]);

        // Test all should fail
        let expr = col("timestamp_col").lt(Expr::Literal(
            ScalarValue::TimestampNanosecond(Some(1), Some(Arc::from("UTC"))),
            None,
        ));
        let expr = logical2physical(&expr, &table_schema);
        let schema_adapter_factory = Arc::new(DefaultSchemaAdapterFactory);
        let table_schema = Arc::new(table_schema.clone());
        let candidate = FilterCandidateBuilder::new(
            expr,
            file_schema.clone(),
            table_schema.clone(),
            schema_adapter_factory,
        )
        .build(&metadata)
        .expect("building candidate")
        .expect("candidate expected");

        let mut row_filter = DatafusionArrowPredicate::try_new(
            candidate,
            &metadata,
            Count::new(),
            Count::new(),
            Time::new(),
        )
        .expect("creating filter predicate");

        let mut parquet_reader = parquet_reader_builder
            .with_projection(row_filter.projection().clone())
            .build()
            .expect("building reader");

        // Parquet file is small, we only need 1 record batch
        let first_rb = parquet_reader
            .next()
            .expect("expected record batch")
            .expect("expected error free record batch");

        let filtered = row_filter.evaluate(first_rb.clone());
        assert!(matches!(filtered, Ok(a) if a == BooleanArray::from(vec![false; 8])));

        // Test all should pass
        let expr = col("timestamp_col").gt(Expr::Literal(
            ScalarValue::TimestampNanosecond(Some(0), Some(Arc::from("UTC"))),
            None,
        ));
        let expr = logical2physical(&expr, &table_schema);
        let schema_adapter_factory = Arc::new(DefaultSchemaAdapterFactory);
        let candidate = FilterCandidateBuilder::new(
            expr,
            file_schema,
            table_schema,
            schema_adapter_factory,
        )
        .build(&metadata)
        .expect("building candidate")
        .expect("candidate expected");

        let mut row_filter = DatafusionArrowPredicate::try_new(
            candidate,
            &metadata,
            Count::new(),
            Count::new(),
            Time::new(),
        )
        .expect("creating filter predicate");

        let filtered = row_filter.evaluate(first_rb);
        assert!(matches!(filtered, Ok(a) if a == BooleanArray::from(vec![true; 8])));
    }

    #[test]
    fn nested_data_structures_prevent_pushdown() {
        let table_schema = Arc::new(get_lists_table_schema());

        let expr = col("utf8_list").is_not_null();
        let expr = logical2physical(&expr, &table_schema);
        check_expression_can_evaluate_against_schema(&expr, &table_schema);

        assert!(!can_expr_be_pushed_down_with_schemas(&expr, &table_schema));
    }

    #[test]
    fn projected_columns_prevent_pushdown() {
        let table_schema = get_basic_table_schema();

        let expr =
            Arc::new(Column::new("nonexistent_column", 0)) as Arc<dyn PhysicalExpr>;

        assert!(!can_expr_be_pushed_down_with_schemas(&expr, &table_schema));
    }

    #[test]
    fn basic_expr_doesnt_prevent_pushdown() {
        let table_schema = get_basic_table_schema();

        let expr = col("string_col").is_null();
        let expr = logical2physical(&expr, &table_schema);

        assert!(can_expr_be_pushed_down_with_schemas(&expr, &table_schema));
    }

    #[test]
    fn complex_expr_doesnt_prevent_pushdown() {
        let table_schema = get_basic_table_schema();

        let expr = col("string_col")
            .is_not_null()
            .or(col("bigint_col").gt(Expr::Literal(ScalarValue::Int64(Some(5)), None)));
        let expr = logical2physical(&expr, &table_schema);

        assert!(can_expr_be_pushed_down_with_schemas(&expr, &table_schema));
    }

    fn get_basic_table_schema() -> Schema {
        let testdata = datafusion_common::test_util::parquet_test_data();
        let file = std::fs::File::open(format!("{testdata}/alltypes_plain.parquet"))
            .expect("opening file");

        let reader = SerializedFileReader::new(file).expect("creating reader");

        let metadata = reader.metadata();

        parquet_to_arrow_schema(metadata.file_metadata().schema_descr(), None)
            .expect("parsing schema")
    }

    fn get_lists_table_schema() -> Schema {
        let testdata = datafusion_common::test_util::parquet_test_data();
        let file = std::fs::File::open(format!("{testdata}/list_columns.parquet"))
            .expect("opening file");

        let reader = SerializedFileReader::new(file).expect("creating reader");

        let metadata = reader.metadata();

        parquet_to_arrow_schema(metadata.file_metadata().schema_descr(), None)
            .expect("parsing schema")
    }

    /// Sanity check that the given expression could be evaluated against the given schema without any errors.
    /// This will fail if the expression references columns that are not in the schema or if the types of the columns are incompatible, etc.
    fn check_expression_can_evaluate_against_schema(
        expr: &Arc<dyn PhysicalExpr>,
        table_schema: &Arc<Schema>,
    ) -> bool {
        let batch = RecordBatch::new_empty(Arc::clone(table_schema));
        expr.evaluate(&batch).is_ok()
    }
}
