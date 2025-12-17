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
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicate, RowFilter};
use parquet::file::metadata::ParquetMetaData;

use datafusion_common::Result;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr::{PhysicalExpr, split_conjunction};

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
        })
    }
}

impl ArrowPredicate for DatafusionArrowPredicate {
    fn projection(&self) -> &ProjectionMask {
        &self.projection_mask
    }

    fn evaluate(&mut self, batch: RecordBatch) -> ArrowResult<BooleanArray> {
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
    /// Column indices into the parquet file schema required to evaluate this filter.
    projection: Vec<usize>,
    /// The Arrow schema containing only the columns required by this filter,
    /// projected from the file's Arrow schema.
    filter_schema: SchemaRef,
}

/// Helper to build a `FilterCandidate`.
///
/// This will do several things:
/// 1. Determine the columns required to evaluate the expression
/// 2. Calculate data required to estimate the cost of evaluating the filter
///
/// Note: This does *not* handle any adaptation of the expression to the file schema.
/// The expression must already be adapted before being passed in here, generally using
/// [`PhysicalExprAdapter`](datafusion_physical_expr_adapter::PhysicalExprAdapter).
struct FilterCandidateBuilder {
    expr: Arc<dyn PhysicalExpr>,
    /// The Arrow schema of this parquet file (the result of converting the
    /// parquet schema to Arrow, potentially with type coercions applied).
    file_schema: SchemaRef,
}

impl FilterCandidateBuilder {
    pub fn new(expr: Arc<dyn PhysicalExpr>, file_schema: Arc<Schema>) -> Self {
        Self { expr, file_schema }
    }

    /// Attempt to build a `FilterCandidate` from the expression
    ///
    /// # Return values
    ///
    /// * `Ok(Some(candidate))` if the expression can be used as an ArrowFilter
    /// * `Ok(None)` if the expression cannot be used as an ArrowFilter
    /// * `Err(e)` if an error occurs while building the candidate
    pub fn build(self, metadata: &ParquetMetaData) -> Result<Option<FilterCandidate>> {
        let Some(required_column_indices) =
            pushdown_columns(&self.expr, &self.file_schema)?
        else {
            return Ok(None);
        };

        let projected_schema =
            Arc::new(self.file_schema.project(&required_column_indices)?);

        let required_bytes = size_of_columns(&required_column_indices, metadata)?;
        let can_use_index = columns_sorted(&required_column_indices, metadata)?;

        Ok(Some(FilterCandidate {
            expr: self.expr,
            required_bytes,
            can_use_index,
            projection: required_column_indices,
            filter_schema: projected_schema,
        }))
    }
}

/// Traverses a `PhysicalExpr` tree to determine if any column references would
/// prevent the expression from being pushed down to the parquet decoder.
///
/// An expression cannot be pushed down if it references:
/// - Non-primitive columns (like structs or lists)
/// - Columns that don't exist in the file schema
struct PushdownChecker<'schema> {
    /// Does the expression require any non-primitive columns (like structs)?
    non_primitive_columns: bool,
    /// Does the expression reference any columns not present in the file schema?
    projected_columns: bool,
    /// Indices into the file schema of columns required to evaluate the expression.
    required_columns: BTreeSet<usize>,
    /// The Arrow schema of the parquet file.
    file_schema: &'schema Schema,
}

impl<'schema> PushdownChecker<'schema> {
    fn new(file_schema: &'schema Schema) -> Self {
        Self {
            non_primitive_columns: false,
            projected_columns: false,
            required_columns: BTreeSet::default(),
            file_schema,
        }
    }

    fn check_single_column(&mut self, column_name: &str) -> Option<TreeNodeRecursion> {
        if let Ok(idx) = self.file_schema.index_of(column_name) {
            self.required_columns.insert(idx);
            if DataType::is_nested(self.file_schema.field(idx).data_type()) {
                self.non_primitive_columns = true;
                return Some(TreeNodeRecursion::Jump);
            }
        } else {
            // Column does not exist in the file schema, so we can't push this down.
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
        if let Some(column) = node.as_any().downcast_ref::<Column>()
            && let Some(recursion) = self.check_single_column(column.name())
        {
            return Ok(recursion);
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

/// Checks if a given expression can be pushed down to the parquet decoder.
///
/// Returns `Some(column_indices)` if the expression can be pushed down,
/// where `column_indices` are the indices into the file schema of all columns
/// required to evaluate the expression.
///
/// Returns `None` if the expression cannot be pushed down (e.g., references
/// non-primitive types or columns not in the file).
fn pushdown_columns(
    expr: &Arc<dyn PhysicalExpr>,
    file_schema: &Schema,
) -> Result<Option<Vec<usize>>> {
    let mut checker = PushdownChecker::new(file_schema);
    expr.visit(&mut checker)?;
    Ok((!checker.prevents_pushdown())
        .then_some(checker.required_columns.into_iter().collect()))
}

/// Checks if a predicate expression can be pushed down to the parquet decoder.
///
/// Returns `true` if all columns referenced by the expression:
/// - Exist in the provided schema
/// - Are primitive types (not structs, lists, etc.)
///
/// # Arguments
/// * `expr` - The filter expression to check
/// * `file_schema` - The Arrow schema of the parquet file (or table schema when
///   the file schema is not yet available during planning)
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

/// Build a [`RowFilter`] from the given predicate expression if possible.
///
/// # Arguments
/// * `expr` - The filter predicate, already adapted to reference columns in `file_schema`
/// * `file_schema` - The Arrow schema of the parquet file (the result of converting
///   the parquet schema to Arrow, potentially with type coercions applied)
/// * `metadata` - Parquet file metadata used for cost estimation
/// * `reorder_predicates` - If true, reorder predicates to minimize I/O
/// * `file_metrics` - Metrics for tracking filter performance
///
/// # Returns
/// * `Ok(Some(row_filter))` if the expression can be used as a RowFilter
/// * `Ok(None)` if the expression cannot be used as a RowFilter
/// * `Err(e)` if an error occurs while building the filter
///
/// Note: The returned `RowFilter` may not contain all conjuncts from the original
/// expression. Conjuncts that cannot be evaluated as an `ArrowPredicate` are ignored.
///
/// For example, if the expression is `a = 1 AND b = 2 AND c = 3` and `b = 2`
/// cannot be evaluated for some reason, the returned `RowFilter` will contain
/// only `a = 1` and `c = 3`.
pub fn build_row_filter(
    expr: &Arc<dyn PhysicalExpr>,
    file_schema: &SchemaRef,
    metadata: &ParquetMetaData,
    reorder_predicates: bool,
    file_metrics: &ParquetFileMetrics,
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
            FilterCandidateBuilder::new(Arc::clone(expr), Arc::clone(file_schema))
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
    use datafusion_expr::{Expr, col};
    use datafusion_physical_expr::planner::logical2physical;
    use datafusion_physical_expr_adapter::{
        DefaultPhysicalExprAdapterFactory, PhysicalExprAdapterFactory,
    };
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

        let table_schema = Arc::new(table_schema.clone());

        let candidate = FilterCandidateBuilder::new(expr, table_schema)
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
        let expr = DefaultPhysicalExprAdapterFactory {}
            .create(Arc::new(table_schema.clone()), Arc::clone(&file_schema))
            .rewrite(expr)
            .expect("rewriting expression");
        let candidate = FilterCandidateBuilder::new(expr, file_schema.clone())
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
        // Rewrite the expression to add CastExpr for type coercion
        let expr = DefaultPhysicalExprAdapterFactory {}
            .create(Arc::new(table_schema), Arc::clone(&file_schema))
            .rewrite(expr)
            .expect("rewriting expression");
        let candidate = FilterCandidateBuilder::new(expr, file_schema)
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
