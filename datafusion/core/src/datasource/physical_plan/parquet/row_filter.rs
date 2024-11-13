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
use arrow::datatypes::{DataType, Schema};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ArrowPredicate, RowFilter};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;

use crate::datasource::schema_adapter::SchemaMapper;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::{arrow_datafusion_err, DataFusionError, Result, ScalarValue};
use datafusion_physical_expr::expressions::{Column, Literal};
use datafusion_physical_expr::utils::reassign_predicate_columns;
use datafusion_physical_expr::{split_conjunction, PhysicalExpr};

use crate::physical_plan::metrics;

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
    /// Columns required to evaluate the expression in the arrow schema
    projection: Vec<usize>,
    /// how many rows were filtered out by this predicate
    rows_pruned: metrics::Count,
    /// how many rows passed this predicate
    rows_matched: metrics::Count,
    /// how long was spent evaluating this predicate
    time: metrics::Time,
    /// used to perform type coercion while filtering rows
    schema_mapping: Arc<dyn SchemaMapper>,
}

impl DatafusionArrowPredicate {
    /// Create a new `DatafusionArrowPredicate` from a `FilterCandidate`
    pub fn try_new(
        candidate: FilterCandidate,
        schema: &Schema,
        metadata: &ParquetMetaData,
        rows_pruned: metrics::Count,
        rows_matched: metrics::Count,
        time: metrics::Time,
        schema_mapping: Arc<dyn SchemaMapper>,
    ) -> Result<Self> {
        let schema = Arc::new(schema.project(&candidate.projection)?);
        let physical_expr = reassign_predicate_columns(candidate.expr, &schema, true)?;

        // ArrowPredicate::evaluate is passed columns in the order they appear in the file
        // If the predicate has multiple columns, we therefore must project the columns based
        // on the order they appear in the file
        let projection = match candidate.projection.len() {
            0 | 1 => vec![],
            2.. => remap_projection(&candidate.projection),
        };

        Ok(Self {
            physical_expr,
            projection,
            projection_mask: ProjectionMask::roots(
                metadata.file_metadata().schema_descr(),
                candidate.projection,
            ),
            rows_pruned,
            rows_matched,
            time,
            schema_mapping,
        })
    }
}

impl ArrowPredicate for DatafusionArrowPredicate {
    fn projection(&self) -> &ProjectionMask {
        &self.projection_mask
    }

    fn evaluate(&mut self, mut batch: RecordBatch) -> ArrowResult<BooleanArray> {
        if !self.projection.is_empty() {
            batch = batch.project(&self.projection)?;
        };

        let batch = self.schema_mapping.map_partial_batch(batch)?;

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
    required_bytes: usize,
    can_use_index: bool,
    projection: Vec<usize>,
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
/// missing column is filled in with `NULL`  via a `SchemaAdapter`.
///
/// When a predicate is pushed down to the parquet reader, the predicate is
/// evaluated in the context of the file schema. If the predicate references a
/// column that is in the table schema but not in the file schema, the column
/// reference must be rewritten to a literal expression that represents the
/// `NULL` value that would be produced by the `SchemaAdapter`.
///
/// For example, if:
/// * The table schema is `id, name, address`
/// * The file schema is  `id, name` (missing the `address` column)
/// * predicate is `address = 'foo'`
///
/// When evaluating the predicate as a filter on the parquet file, the predicate
/// must be rewritten to `NULL = 'foo'` as the `address` column will be filled
/// in with `NULL` values during the rest of the evaluation.
struct FilterCandidateBuilder<'a> {
    expr: Arc<dyn PhysicalExpr>,
    /// The schema of this parquet file
    file_schema: &'a Schema,
    /// The schema of the table (merged schema) -- columns may be in different
    /// order than in the file and have columns that are not in the file schema
    table_schema: &'a Schema,
}

impl<'a> FilterCandidateBuilder<'a> {
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        file_schema: &'a Schema,
        table_schema: &'a Schema,
    ) -> Self {
        Self {
            expr,
            file_schema,
            table_schema,
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
        let Some((required_indices, rewritten_expr)) =
            pushdown_columns(self.expr, self.file_schema, self.table_schema)?
        else {
            return Ok(None);
        };

        let required_bytes = size_of_columns(&required_indices, metadata)?;
        let can_use_index = columns_sorted(&required_indices, metadata)?;

        Ok(Some(FilterCandidate {
            expr: rewritten_expr,
            required_bytes,
            can_use_index,
            projection: required_indices.into_iter().collect(),
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
    projected_columns: bool,
    // the indices of all the columns found within the given expression which exist inside the given
    // [`file_schema`]
    required_column_indices: BTreeSet<usize>,
    file_schema: &'schema Schema,
    table_schema: &'schema Schema,
}

impl<'schema> PushdownChecker<'schema> {
    fn new(file_schema: &'schema Schema, table_schema: &'schema Schema) -> Self {
        Self {
            non_primitive_columns: false,
            projected_columns: false,
            required_column_indices: BTreeSet::default(),
            file_schema,
            table_schema,
        }
    }

    fn check_single_column(&mut self, column_name: &str) -> Option<TreeNodeRecursion> {
        if let Ok(idx) = self.file_schema.index_of(column_name) {
            self.required_column_indices.insert(idx);

            if DataType::is_nested(self.file_schema.field(idx).data_type()) {
                self.non_primitive_columns = true;
                return Some(TreeNodeRecursion::Jump);
            }
        } else if self.table_schema.index_of(column_name).is_err() {
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

impl<'schema> TreeNodeRewriter for PushdownChecker<'schema> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(
        &mut self,
        node: Arc<dyn PhysicalExpr>,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            if let Some(recursion) = self.check_single_column(column.name()) {
                return Ok(Transformed::new(node, false, recursion));
            }
        }

        Ok(Transformed::no(node))
    }

    /// After visiting all children, rewrite column references to nulls if
    /// they are not in the file schema.
    /// We do this because they won't be relevant if they're not in the file schema, since that's
    /// the only thing we're dealing with here as this is only used for the parquet pushdown during
    /// scanning
    fn f_up(
        &mut self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            // if the expression is a column, is it in the file schema?
            if self.file_schema.field_with_name(column.name()).is_err() {
                return self
                    .table_schema
                    .field_with_name(column.name())
                    .and_then(|field| {
                        // Replace the column reference with a NULL (using the type from the table schema)
                        // e.g. `column = 'foo'` is rewritten be transformed to `NULL = 'foo'`
                        //
                        // See comments on `FilterCandidateBuilder` for more information
                        let null_value = ScalarValue::try_from(field.data_type())?;
                        Ok(Transformed::yes(Arc::new(Literal::new(null_value)) as _))
                    })
                    // If the column is not in the table schema, should throw the error
                    .map_err(|e| arrow_datafusion_err!(e));
            }
        }

        Ok(Transformed::no(expr))
    }
}

type ProjectionAndExpr = (BTreeSet<usize>, Arc<dyn PhysicalExpr>);

// Checks if a given expression can be pushed down into `ParquetExec` as opposed to being evaluated
// post-parquet-scan in a `FilterExec`. If it can be pushed down, this returns returns all the
// columns in the given expression so that they can be used in the parquet scanning, along with the
// expression rewritten as defined in [`PushdownChecker::f_up`]
fn pushdown_columns(
    expr: Arc<dyn PhysicalExpr>,
    file_schema: &Schema,
    table_schema: &Schema,
) -> Result<Option<ProjectionAndExpr>> {
    let mut checker = PushdownChecker::new(file_schema, table_schema);

    let expr = expr.rewrite(&mut checker).data()?;

    Ok((!checker.prevents_pushdown()).then_some((checker.required_column_indices, expr)))
}

/// creates a PushdownChecker for a single use to check a given column with the given schemes. Used
/// to check preemptively if a column name would prevent pushdowning.
/// effectively does the inverse of [`pushdown_columns`] does, but with a single given column
/// (instead of traversing the entire tree to determine this)
fn would_column_prevent_pushdown(
    column_name: &str,
    file_schema: &Schema,
    table_schema: &Schema,
) -> bool {
    let mut checker = PushdownChecker::new(file_schema, table_schema);

    // the return of this is only used for [`PushdownChecker::f_down()`], so we can safely ignore
    // it here. I'm just verifying we know the return type of this so nobody accidentally changes
    // the return type of this fn and it gets implicitly ignored here.
    let _: Option<TreeNodeRecursion> = checker.check_single_column(column_name);

    // and then return a value based on the state of the checker
    checker.prevents_pushdown()
}

/// Recurses through expr as a trea, finds all `column`s, and checks if any of them would prevent
/// this expression from being predicate pushed down. If any of them would, this returns false.
/// Otherwise, true.
pub fn can_expr_be_pushed_down_with_schemas(
    expr: &datafusion_expr::Expr,
    file_schema: &Schema,
    table_schema: &Schema,
) -> bool {
    let mut can_be_pushed = true;
    expr.apply(|expr| match expr {
        datafusion_expr::Expr::Column(column) => {
            can_be_pushed &=
                !would_column_prevent_pushdown(column.name(), file_schema, table_schema);
            Ok(if can_be_pushed {
                TreeNodeRecursion::Jump
            } else {
                TreeNodeRecursion::Stop
            })
        }
        _ => Ok(TreeNodeRecursion::Continue),
    })
    .unwrap(); // we never return an Err, so we can safely unwrap this
    can_be_pushed
}

/// Computes the projection required to go from the file's schema order to the projected
/// order expected by this filter
///
/// Effectively this computes the rank of each element in `src`
fn remap_projection(src: &[usize]) -> Vec<usize> {
    let len = src.len();

    // Compute the column mapping from projected order to file order
    // i.e. the indices required to sort projected schema into the file schema
    //
    // e.g. projection: [5, 9, 0] -> [2, 0, 1]
    let mut sorted_indexes: Vec<_> = (0..len).collect();
    sorted_indexes.sort_unstable_by_key(|x| src[*x]);

    // Compute the mapping from schema order to projected order
    // i.e. the indices required to sort file schema into the projected schema
    //
    // Above we computed the order of the projected schema according to the file
    // schema, and so we can use this as the comparator
    //
    // e.g. sorted_indexes [2, 0, 1] -> [1, 2, 0]
    let mut projection: Vec<_> = (0..len).collect();
    projection.sort_unstable_by_key(|x| sorted_indexes[*x]);
    projection
}

/// Calculate the total compressed size of all `Column`'s required for
/// predicate `Expr`.
///
/// This value represents the total amount of IO required to evaluate the
/// predicate.
fn size_of_columns(
    columns: &BTreeSet<usize>,
    metadata: &ParquetMetaData,
) -> Result<usize> {
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
fn columns_sorted(
    _columns: &BTreeSet<usize>,
    _metadata: &ParquetMetaData,
) -> Result<bool> {
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
    file_schema: &Schema,
    table_schema: &Schema,
    metadata: &ParquetMetaData,
    reorder_predicates: bool,
    file_metrics: &ParquetFileMetrics,
    schema_mapping: Arc<dyn SchemaMapper>,
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
            FilterCandidateBuilder::new(Arc::clone(expr), file_schema, table_schema)
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

    candidates
        .into_iter()
        .map(|candidate| {
            DatafusionArrowPredicate::try_new(
                candidate,
                file_schema,
                metadata,
                rows_pruned.clone(),
                rows_matched.clone(),
                time.clone(),
                Arc::clone(&schema_mapping),
            )
            .map(|pred| Box::new(pred) as _)
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|filters| Some(RowFilter::new(filters)))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::datasource::schema_adapter::{
        DefaultSchemaAdapterFactory, SchemaAdapterFactory,
    };

    use arrow::datatypes::Field;
    use arrow_schema::{Fields, TimeUnit::Nanosecond};
    use datafusion_expr::{cast, col, lit, Expr};
    use datafusion_physical_expr::planner::logical2physical;
    use datafusion_physical_plan::metrics::{Count, Time};

    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::parquet_to_arrow_schema;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use rand::prelude::*;

    // We should ignore predicate that read non-primitive columns
    #[test]
    fn test_filter_candidate_builder_ignore_complex_types() {
        let testdata = crate::test_util::parquet_test_data();
        let file = std::fs::File::open(format!("{testdata}/list_columns.parquet"))
            .expect("opening file");

        let reader = SerializedFileReader::new(file).expect("creating reader");

        let metadata = reader.metadata();

        let table_schema =
            parquet_to_arrow_schema(metadata.file_metadata().schema_descr(), None)
                .expect("parsing schema");

        let expr = col("int64_list").is_not_null();
        let expr = logical2physical(&expr, &table_schema);

        let candidate = FilterCandidateBuilder::new(expr, &table_schema, &table_schema)
            .build(metadata)
            .expect("building candidate");

        assert!(candidate.is_none());
    }

    // If a column exists in the table schema but not the file schema it should be rewritten to a null expression
    #[test]
    fn test_filter_candidate_builder_rewrite_missing_column() {
        let testdata = crate::test_util::parquet_test_data();
        let file = std::fs::File::open(format!("{testdata}/alltypes_plain.parquet"))
            .expect("opening file");

        let reader = SerializedFileReader::new(file).expect("creating reader");

        let metadata = reader.metadata();

        let table_schema =
            parquet_to_arrow_schema(metadata.file_metadata().schema_descr(), None)
                .expect("parsing schema");

        let file_schema = Schema::new(vec![
            Field::new("bigint_col", DataType::Int64, true),
            Field::new("float_col", DataType::Float32, true),
        ]);

        // The parquet file with `file_schema` just has `bigint_col` and `float_col` column, and don't have the `int_col`
        let expr = col("bigint_col").eq(cast(col("int_col"), DataType::Int64));
        let expr = logical2physical(&expr, &table_schema);
        let expected_candidate_expr =
            col("bigint_col").eq(cast(lit(ScalarValue::Int32(None)), DataType::Int64));
        let expected_candidate_expr =
            logical2physical(&expected_candidate_expr, &table_schema);

        let candidate = FilterCandidateBuilder::new(expr, &file_schema, &table_schema)
            .build(metadata)
            .expect("building candidate");

        assert!(candidate.is_some());

        assert_eq!(
            candidate.unwrap().expr.to_string(),
            expected_candidate_expr.to_string()
        );
    }

    #[test]
    fn test_filter_type_coercion() {
        let testdata = crate::test_util::parquet_test_data();
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

        let table_ref = Arc::new(table_schema.clone());
        let schema_adapter =
            DefaultSchemaAdapterFactory.create(Arc::clone(&table_ref), table_ref);
        let (schema_mapping, _) = schema_adapter
            .map_schema(&file_schema)
            .expect("creating schema mapping");

        let mut parquet_reader = parquet_reader_builder.build().expect("building reader");

        // Parquet file is small, we only need 1 recordbatch
        let first_rb = parquet_reader
            .next()
            .expect("expected record batch")
            .expect("expected error free record batch");

        // Test all should fail
        let expr = col("timestamp_col").lt(Expr::Literal(
            ScalarValue::TimestampNanosecond(Some(1), Some(Arc::from("UTC"))),
        ));
        let expr = logical2physical(&expr, &table_schema);
        let candidate = FilterCandidateBuilder::new(expr, &file_schema, &table_schema)
            .build(&metadata)
            .expect("building candidate")
            .expect("candidate expected");

        let mut row_filter = DatafusionArrowPredicate::try_new(
            candidate,
            &file_schema,
            &metadata,
            Count::new(),
            Count::new(),
            Time::new(),
            Arc::clone(&schema_mapping),
        )
        .expect("creating filter predicate");

        let filtered = row_filter.evaluate(first_rb.clone());
        assert!(matches!(filtered, Ok(a) if a == BooleanArray::from(vec![false; 8])));

        // Test all should pass
        let expr = col("timestamp_col").gt(Expr::Literal(
            ScalarValue::TimestampNanosecond(Some(0), Some(Arc::from("UTC"))),
        ));
        let expr = logical2physical(&expr, &table_schema);
        let candidate = FilterCandidateBuilder::new(expr, &file_schema, &table_schema)
            .build(&metadata)
            .expect("building candidate")
            .expect("candidate expected");

        let mut row_filter = DatafusionArrowPredicate::try_new(
            candidate,
            &file_schema,
            &metadata,
            Count::new(),
            Count::new(),
            Time::new(),
            schema_mapping,
        )
        .expect("creating filter predicate");

        let filtered = row_filter.evaluate(first_rb);
        assert!(matches!(filtered, Ok(a) if a == BooleanArray::from(vec![true; 8])));
    }

    #[test]
    fn test_remap_projection() {
        let mut rng = thread_rng();
        for _ in 0..100 {
            // A random selection of column indexes in arbitrary order
            let projection: Vec<_> = (0..100).map(|_| rng.gen()).collect();

            // File order is the projection sorted
            let mut file_order = projection.clone();
            file_order.sort_unstable();

            let remap = remap_projection(&projection);
            // Applying the remapped projection to the file order should yield the original
            let remapped: Vec<_> = remap.iter().map(|r| file_order[*r]).collect();
            assert_eq!(projection, remapped)
        }
    }

    #[test]
    fn nested_data_structures_prevent_pushdown() {
        let table_schema = get_basic_table_schema();

        let file_schema = Schema::new(vec![Field::new(
            "list_col",
            DataType::Struct(Fields::empty()),
            true,
        )]);

        let expr = col("list_col").is_not_null();

        assert!(!can_expr_be_pushed_down_with_schemas(
            &expr,
            &file_schema,
            &table_schema
        ));
    }

    #[test]
    fn projected_columns_prevent_pushdown() {
        let table_schema = get_basic_table_schema();

        let file_schema =
            Schema::new(vec![Field::new("existing_col", DataType::Int64, true)]);

        let expr = col("nonexistent_column").is_null();

        assert!(!can_expr_be_pushed_down_with_schemas(
            &expr,
            &file_schema,
            &table_schema
        ));
    }

    #[test]
    fn basic_expr_doesnt_prevent_pushdown() {
        let table_schema = get_basic_table_schema();

        let file_schema = Schema::new(vec![Field::new("str_col", DataType::Utf8, true)]);

        let expr = col("str_col").is_null();

        assert!(can_expr_be_pushed_down_with_schemas(
            &expr,
            &file_schema,
            &table_schema
        ));
    }

    #[test]
    fn complex_expr_doesnt_prevent_pushdown() {
        let table_schema = get_basic_table_schema();

        let file_schema = Schema::new(vec![
            Field::new("str_col", DataType::Utf8, true),
            Field::new("int_col", DataType::UInt64, true),
        ]);

        let expr = col("str_col")
            .is_not_null()
            .or(col("int_col").gt(Expr::Literal(ScalarValue::UInt64(Some(5)))));

        assert!(can_expr_be_pushed_down_with_schemas(
            &expr,
            &file_schema,
            &table_schema
        ));
    }

    fn get_basic_table_schema() -> Schema {
        let testdata = crate::test_util::parquet_test_data();
        let file = std::fs::File::open(format!("{testdata}/alltypes_plain.parquet"))
            .expect("opening file");

        let reader = SerializedFileReader::new(file).expect("creating reader");

        let metadata = reader.metadata();

        parquet_to_arrow_schema(metadata.file_metadata().schema_descr(), None)
            .expect("parsing schema")
    }
}
