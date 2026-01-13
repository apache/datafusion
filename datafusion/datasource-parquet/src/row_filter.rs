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
//!
//! List-aware predicates (for example, `array_has`, `array_has_all`, and
//! `array_has_any`) can be evaluated directly during Parquet decoding. Struct
//! columns and other nested projections that are not explicitly supported will
//! continue to be evaluated after the batches are materialized.

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
use parquet::schema::types::SchemaDescriptor;

use datafusion_common::Result;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr::{PhysicalExpr, split_conjunction};

use datafusion_physical_plan::metrics;

use super::ParquetFileMetrics;
use super::supported_predicates::supports_list_predicates;

/// A "compiled" predicate passed to `ParquetRecordBatchStream` to perform
/// row-level filtering during parquet decoding.
///
/// See the module level documentation for more information.
///
/// Implements the `ArrowPredicate` trait used by the parquet decoder
///
/// An expression can be evaluated as a `DatafusionArrowPredicate` if it:
/// * Does not reference any projected columns
/// * References either primitive columns or list columns used by
///   supported predicates (such as `array_has_all` or NULL checks). Struct
///   columns are still evaluated after decoding.
#[derive(Debug)]
pub(crate) struct DatafusionArrowPredicate {
    /// the filter expression
    physical_expr: Arc<dyn PhysicalExpr>,
    /// Path to the leaf columns in the parquet schema required to evaluate the
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
            // Use leaf indices: when nested columns are involved, we must specify
            // leaf (primitive) column indices in the Parquet schema so the decoder
            // can properly project and filter nested structures.
            projection_mask: ProjectionMask::leaves(
                metadata.file_metadata().schema_descr(),
                candidate.projection.leaf_indices.iter().copied(),
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
    projection: LeafProjection,
    /// The Arrow schema containing only the columns required by this filter,
    /// projected from the file's Arrow schema.
    filter_schema: SchemaRef,
}

/// Projection specification for nested columns using Parquet leaf column indices.
///
/// For nested types like List and Struct, Parquet stores data in leaf columns
/// (the primitive fields). This struct tracks which leaf columns are needed
/// to evaluate a filter expression.
#[derive(Debug, Clone)]
struct LeafProjection {
    /// Leaf column indices in the Parquet schema descriptor.
    leaf_indices: Vec<usize>,
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
        let Some(required_columns) = pushdown_columns(&self.expr, &self.file_schema)?
        else {
            return Ok(None);
        };

        let root_indices: Vec<_> =
            required_columns.required_columns.into_iter().collect();
        let leaf_indices = leaf_indices_for_roots(
            &root_indices,
            metadata.file_metadata().schema_descr(),
            required_columns.nested,
        );

        let projected_schema = Arc::new(self.file_schema.project(&root_indices)?);

        let required_bytes = size_of_columns(&leaf_indices, metadata)?;
        let can_use_index = columns_sorted(&leaf_indices, metadata)?;

        Ok(Some(FilterCandidate {
            expr: self.expr,
            required_bytes,
            can_use_index,
            projection: LeafProjection { leaf_indices },
            filter_schema: projected_schema,
        }))
    }
}

/// Traverses a `PhysicalExpr` tree to determine if any column references would
/// prevent the expression from being pushed down to the parquet decoder.
///
/// An expression cannot be pushed down if it references:
/// - Unsupported nested columns (structs or list fields that are not covered by
///   the supported predicate set)
/// - Columns that don't exist in the file schema
struct PushdownChecker<'schema> {
    /// Does the expression require any non-primitive columns (like structs)?
    non_primitive_columns: bool,
    /// Does the expression reference any columns not present in the file schema?
    projected_columns: bool,
    /// Indices into the file schema of columns required to evaluate the expression.
    required_columns: BTreeSet<usize>,
    /// Tracks the nested column behavior found during traversal.
    nested_behavior: NestedColumnSupport,
    /// Whether nested list columns are supported by the predicate semantics.
    allow_list_columns: bool,
    /// The Arrow schema of the parquet file.
    file_schema: &'schema Schema,
}

impl<'schema> PushdownChecker<'schema> {
    fn new(file_schema: &'schema Schema, allow_list_columns: bool) -> Self {
        Self {
            non_primitive_columns: false,
            projected_columns: false,
            required_columns: BTreeSet::default(),
            nested_behavior: NestedColumnSupport::PrimitiveOnly,
            allow_list_columns,
            file_schema,
        }
    }

    fn check_single_column(&mut self, column_name: &str) -> Option<TreeNodeRecursion> {
        let idx = match self.file_schema.index_of(column_name) {
            Ok(idx) => idx,
            Err(_) => {
                // Column does not exist in the file schema, so we can't push this down.
                self.projected_columns = true;
                return Some(TreeNodeRecursion::Jump);
            }
        };

        self.required_columns.insert(idx);
        let data_type = self.file_schema.field(idx).data_type();

        if DataType::is_nested(data_type) {
            self.handle_nested_type(data_type)
        } else {
            None
        }
    }

    /// Determines whether a nested data type can be pushed down to Parquet decoding.
    ///
    /// Returns `Some(TreeNodeRecursion::Jump)` if the nested type prevents pushdown,
    /// `None` if the type is supported and pushdown can continue.
    fn handle_nested_type(&mut self, data_type: &DataType) -> Option<TreeNodeRecursion> {
        if self.is_nested_type_supported(data_type) {
            // Update to ListsSupported if we haven't encountered unsupported types yet
            if self.nested_behavior == NestedColumnSupport::PrimitiveOnly {
                self.nested_behavior = NestedColumnSupport::ListsSupported;
            }
            None
        } else {
            // Block pushdown for unsupported nested types:
            // - Structs (regardless of predicate support)
            // - Lists without supported predicates
            self.nested_behavior = NestedColumnSupport::Unsupported;
            self.non_primitive_columns = true;
            Some(TreeNodeRecursion::Jump)
        }
    }

    /// Checks if a nested data type is supported for list column pushdown.
    ///
    /// List columns are only supported if:
    /// 1. The data type is a list variant (List, LargeList, or FixedSizeList)
    /// 2. The expression contains supported list predicates (e.g., array_has_all)
    fn is_nested_type_supported(&self, data_type: &DataType) -> bool {
        let is_list = matches!(
            data_type,
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
        );
        self.allow_list_columns && is_list
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

/// Describes the nested column behavior for filter pushdown.
///
/// This enum makes explicit the different states a predicate can be in
/// with respect to nested column handling during Parquet decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NestedColumnSupport {
    /// Expression references only primitive (non-nested) columns.
    /// These can always be pushed down to the Parquet decoder.
    PrimitiveOnly,
    /// Expression references list columns with supported predicates
    /// (e.g., array_has, array_has_all, IS NULL).
    /// These can be pushed down to the Parquet decoder.
    ListsSupported,
    /// Expression references unsupported nested types (e.g., structs)
    /// or list columns without supported predicates.
    /// These cannot be pushed down and must be evaluated after decoding.
    Unsupported,
}

#[derive(Debug)]
struct PushdownColumns {
    required_columns: BTreeSet<usize>,
    nested: NestedColumnSupport,
}

/// Checks if a given expression can be pushed down to the parquet decoder.
///
/// Returns `Some(PushdownColumns)` if the expression can be pushed down,
/// where the struct contains the indices into the file schema of all columns
/// required to evaluate the expression.
///
/// Returns `None` if the expression cannot be pushed down (e.g., references
/// unsupported nested types or columns not in the file).
fn pushdown_columns(
    expr: &Arc<dyn PhysicalExpr>,
    file_schema: &Schema,
) -> Result<Option<PushdownColumns>> {
    let allow_list_columns = supports_list_predicates(expr);
    let mut checker = PushdownChecker::new(file_schema, allow_list_columns);
    expr.visit(&mut checker)?;
    Ok((!checker.prevents_pushdown()).then_some(PushdownColumns {
        required_columns: checker.required_columns,
        nested: checker.nested_behavior,
    }))
}

fn leaf_indices_for_roots(
    root_indices: &[usize],
    schema_descr: &SchemaDescriptor,
    nested: NestedColumnSupport,
) -> Vec<usize> {
    // For primitive-only columns, root indices ARE the leaf indices
    if nested == NestedColumnSupport::PrimitiveOnly {
        return root_indices.to_vec();
    }

    // For List columns, expand to the single leaf column (item field)
    // For Struct columns (unsupported), this would expand to multiple leaves
    let root_set: BTreeSet<_> = root_indices.iter().copied().collect();

    (0..schema_descr.num_columns())
        .filter(|leaf_idx| {
            root_set.contains(&schema_descr.get_column_root_idx(*leaf_idx))
        })
        .collect()
}

/// Checks if a predicate expression can be pushed down to the parquet decoder.
///
/// Returns `true` if all columns referenced by the expression:
/// - Exist in the provided schema
/// - Are primitive types OR list columns with supported predicates
///   (e.g., `array_has`, `array_has_all`, `array_has_any`, IS NULL, IS NOT NULL)
/// - Struct columns are not supported and will prevent pushdown
///
/// # Arguments
/// * `expr` - The filter expression to check
/// * `file_schema` - The Arrow schema of the parquet file (or table schema when
///   the file schema is not yet available during planning)
///
/// # Examples
///
/// Primitive column filters can be pushed down:
/// ```ignore
/// use datafusion_expr::{col, Expr};
/// use datafusion_common::ScalarValue;
/// use arrow::datatypes::{DataType, Field, Schema};
/// use std::sync::Arc;
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("age", DataType::Int32, false),
/// ]));
///
/// // Primitive filter: can be pushed down
/// let expr = col("age").gt(Expr::Literal(ScalarValue::Int32(Some(30)), None));
/// let expr = logical2physical(&expr, &schema);
/// assert!(can_expr_be_pushed_down_with_schemas(&expr, &schema));
/// ```
///
/// Struct column filters cannot be pushed down:
/// ```ignore
/// use arrow::datatypes::Fields;
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("person", DataType::Struct(
///         Fields::from(vec![Field::new("name", DataType::Utf8, true)])
///     ), true),
/// ]));
///
/// // Struct filter: cannot be pushed down
/// let expr = col("person").is_not_null();
/// let expr = logical2physical(&expr, &schema);
/// assert!(!can_expr_be_pushed_down_with_schemas(&expr, &schema));
/// ```
///
/// List column filters with supported predicates can be pushed down:
/// ```ignore
/// use datafusion_functions_nested::expr_fn::{array_has_all, make_array};
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("tags", DataType::List(
///         Arc::new(Field::new("item", DataType::Utf8, true))
///     ), true),
/// ]));
///
/// // Array filter with supported predicate: can be pushed down
/// let expr = array_has_all(col("tags"), make_array(vec![
///     Expr::Literal(ScalarValue::Utf8(Some("rust".to_string())), None)
/// ]));
/// let expr = logical2physical(&expr, &schema);
/// assert!(can_expr_be_pushed_down_with_schemas(&expr, &schema));
/// ```
pub fn can_expr_be_pushed_down_with_schemas(
    expr: &Arc<dyn PhysicalExpr>,
    file_schema: &Schema,
) -> bool {
    match pushdown_columns(expr, file_schema) {
        Ok(Some(_)) => true,
        Ok(None) | Err(_) => false,
    }
}

/// Calculate the total compressed size of all leaf columns required for
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

    use arrow::array::{ListBuilder, StringBuilder};
    use arrow::datatypes::{Field, TimeUnit::Nanosecond};
    use datafusion_expr::{Expr, col};
    use datafusion_functions_nested::array_has::{
        array_has_all_udf, array_has_any_udf, array_has_udf,
    };
    use datafusion_functions_nested::expr_fn::{
        array_has, array_has_all, array_has_any, make_array,
    };
    use datafusion_physical_expr::planner::logical2physical;
    use datafusion_physical_expr_adapter::{
        DefaultPhysicalExprAdapterFactory, PhysicalExprAdapterFactory,
    };
    use datafusion_physical_plan::metrics::{Count, ExecutionPlanMetricsSet, Time};

    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::parquet_to_arrow_schema;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use tempfile::NamedTempFile;

    // List predicates used by the decoder should be accepted for pushdown
    #[test]
    fn test_filter_candidate_builder_supports_list_types() {
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

        let list_index = table_schema
            .index_of("int64_list")
            .expect("list column should exist");

        let candidate = FilterCandidateBuilder::new(expr, table_schema)
            .build(metadata)
            .expect("building candidate")
            .expect("list pushdown should be supported");

        assert_eq!(candidate.projection.leaf_indices, vec![list_index]);
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
    fn struct_data_structures_prevent_pushdown() {
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "struct_col",
            DataType::Struct(
                vec![Arc::new(Field::new("a", DataType::Int32, true))].into(),
            ),
            true,
        )]));

        let expr = col("struct_col").is_not_null();
        let expr = logical2physical(&expr, &table_schema);

        assert!(!can_expr_be_pushed_down_with_schemas(&expr, &table_schema));
    }

    #[test]
    fn mixed_primitive_and_struct_prevents_pushdown() {
        // Even when a predicate contains both primitive and unsupported nested columns,
        // the entire predicate should not be pushed down because the struct column
        // cannot be evaluated during Parquet decoding.
        let table_schema = Arc::new(Schema::new(vec![
            Field::new(
                "struct_col",
                DataType::Struct(
                    vec![Arc::new(Field::new("a", DataType::Int32, true))].into(),
                ),
                true,
            ),
            Field::new("int_col", DataType::Int32, false),
        ]));

        // Expression: (struct_col IS NOT NULL) AND (int_col = 5)
        // Even though int_col is primitive, the presence of struct_col in the
        // conjunction should prevent pushdown of the entire expression.
        let expr = col("struct_col")
            .is_not_null()
            .and(col("int_col").eq(Expr::Literal(ScalarValue::Int32(Some(5)), None)));
        let expr = logical2physical(&expr, &table_schema);

        // The entire expression should not be pushed down
        assert!(!can_expr_be_pushed_down_with_schemas(&expr, &table_schema));

        // However, just the int_col predicate alone should be pushable
        let expr_int_only =
            col("int_col").eq(Expr::Literal(ScalarValue::Int32(Some(5)), None));
        let expr_int_only = logical2physical(&expr_int_only, &table_schema);
        assert!(can_expr_be_pushed_down_with_schemas(
            &expr_int_only,
            &table_schema
        ));
    }

    #[test]
    fn nested_lists_allow_pushdown_checks() {
        let table_schema = Arc::new(get_lists_table_schema());

        let expr = col("utf8_list").is_not_null();
        let expr = logical2physical(&expr, &table_schema);
        check_expression_can_evaluate_against_schema(&expr, &table_schema);

        assert!(can_expr_be_pushed_down_with_schemas(&expr, &table_schema));
    }

    #[test]
    fn array_has_all_pushdown_filters_rows() {
        // Test array_has_all: checks if array contains all of ["c"]
        // Rows with "c": row 1 and row 2
        let expr = array_has_all(
            col("letters"),
            make_array(vec![Expr::Literal(
                ScalarValue::Utf8(Some("c".to_string())),
                None,
            )]),
        );
        test_array_predicate_pushdown("array_has_all", expr, 1, 2, true);
    }

    /// Helper function to test array predicate pushdown functionality.
    ///
    /// Creates a Parquet file with a list column, applies the given predicate,
    /// and verifies that rows are correctly filtered during decoding.
    fn test_array_predicate_pushdown(
        func_name: &str,
        predicate_expr: Expr,
        expected_pruned: usize,
        expected_matched: usize,
        expect_list_support: bool,
    ) {
        let item_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "letters",
            DataType::List(item_field),
            true,
        )]));

        let mut builder = ListBuilder::new(StringBuilder::new());
        // Row 0: ["a", "b"]
        builder.values().append_value("a");
        builder.values().append_value("b");
        builder.append(true);

        // Row 1: ["c"]
        builder.values().append_value("c");
        builder.append(true);

        // Row 2: ["c", "d"]
        builder.values().append_value("c");
        builder.values().append_value("d");
        builder.append(true);

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(builder.finish())])
                .expect("record batch");

        let file = NamedTempFile::new().expect("temp file");
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), schema, None).expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let reader_file = file.reopen().expect("reopen file");
        let parquet_reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(reader_file)
                .expect("reader builder");
        let metadata = parquet_reader_builder.metadata().clone();
        let file_schema = parquet_reader_builder.schema().clone();

        let expr = logical2physical(&predicate_expr, &file_schema);
        if expect_list_support {
            assert!(supports_list_predicates(&expr));
        }

        let metrics = ExecutionPlanMetricsSet::new();
        let file_metrics =
            ParquetFileMetrics::new(0, &format!("{func_name}.parquet"), &metrics);

        let row_filter =
            build_row_filter(&expr, &file_schema, &metadata, false, &file_metrics)
                .expect("building row filter")
                .expect("row filter should exist");

        let reader = parquet_reader_builder
            .with_row_filter(row_filter)
            .build()
            .expect("build reader");

        let mut total_rows = 0;
        for batch in reader {
            let batch = batch.expect("record batch");
            total_rows += batch.num_rows();
        }

        assert_eq!(
            file_metrics.pushdown_rows_pruned.value(),
            expected_pruned,
            "{func_name}: expected {expected_pruned} pruned rows"
        );
        assert_eq!(
            file_metrics.pushdown_rows_matched.value(),
            expected_matched,
            "{func_name}: expected {expected_matched} matched rows"
        );
        assert_eq!(
            total_rows, expected_matched,
            "{func_name}: expected {expected_matched} total rows"
        );
    }

    #[test]
    fn array_has_pushdown_filters_rows() {
        // Test array_has: checks if "c" is in the array
        // Rows with "c": row 1 and row 2
        let expr = array_has(
            col("letters"),
            Expr::Literal(ScalarValue::Utf8(Some("c".to_string())), None),
        );
        test_array_predicate_pushdown("array_has", expr, 1, 2, true);
    }

    #[test]
    fn array_has_any_pushdown_filters_rows() {
        // Test array_has_any: checks if array contains any of ["a", "d"]
        // Row 0 has "a", row 2 has "d" - both should match
        let expr = array_has_any(
            col("letters"),
            make_array(vec![
                Expr::Literal(ScalarValue::Utf8(Some("a".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some("d".to_string())), None),
            ]),
        );
        test_array_predicate_pushdown("array_has_any", expr, 1, 2, true);
    }

    #[test]
    fn array_has_udf_pushdown_filters_rows() {
        let expr = array_has_udf().call(vec![
            col("letters"),
            Expr::Literal(ScalarValue::Utf8(Some("c".to_string())), None),
        ]);

        test_array_predicate_pushdown("array_has_udf", expr, 1, 2, true);
    }

    #[test]
    fn array_has_all_udf_pushdown_filters_rows() {
        let expr = array_has_all_udf().call(vec![
            col("letters"),
            make_array(vec![Expr::Literal(
                ScalarValue::Utf8(Some("c".to_string())),
                None,
            )]),
        ]);

        test_array_predicate_pushdown("array_has_all_udf", expr, 1, 2, true);
    }

    #[test]
    fn array_has_any_udf_pushdown_filters_rows() {
        let expr = array_has_any_udf().call(vec![
            col("letters"),
            make_array(vec![
                Expr::Literal(ScalarValue::Utf8(Some("a".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some("d".to_string())), None),
            ]),
        ]);

        test_array_predicate_pushdown("array_has_any_udf", expr, 1, 2, true);
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
