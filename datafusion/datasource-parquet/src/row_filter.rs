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
//! 4. Re-order predicates by total size (from step 3).
//! 5. "Compile" each predicate `Expr` to a `DatafusionArrowPredicate`.
//! 6. Build the `RowFilter` from the ordered predicates.
//!
//! List-aware predicates (for example, `array_has`, `array_has_all`, and
//! `array_has_any`) can be evaluated directly during Parquet decoding.
//! Struct field access via `get_field` is also supported when the accessed
//! leaf is a primitive type. Filters that reference entire struct columns
//! rather than individual fields cannot be pushed down and are instead
//! evaluated after the full batches are materialized.
//!
//! For example, given a struct column `s {name: Utf8, value: Int32}`:
//! - `WHERE s['value'] > 5` — pushed down (accesses a primitive leaf)
//! - `WHERE s IS NOT NULL`  — not pushed down (references the whole struct)

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use datafusion_functions::core::getfield::GetFieldFunc;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicate, RowFilter};
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescriptor;

use datafusion_common::Result;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::expressions::{Column, Literal};
use datafusion_physical_expr::utils::{collect_columns, reassign_expr_columns};
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
///   supported predicates (such as `array_has_all` or NULL checks).
/// * References struct fields via `get_field` where the accessed leaf
///   is a primitive type (e.g. `get_field(struct_col, 'field') > 5`).
///   Direct references to whole struct columns are still evaluated after
///   decoding.
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
        rows_pruned: metrics::Count,
        rows_matched: metrics::Count,
        time: metrics::Time,
    ) -> Result<Self> {
        let physical_expr =
            reassign_expr_columns(candidate.expr, &candidate.read_plan.projected_schema)?;

        Ok(Self {
            physical_expr,
            projection_mask: candidate.read_plan.projection_mask,
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
    /// The resolved Parquet read plan (leaf indices + projected schema).
    read_plan: ParquetReadPlan,
}

/// The result of resolving which Parquet leaf columns and Arrow schema fields
/// are needed to evaluate an expression against a Parquet file
///
/// This is the shared output of the column resolution pipeline used by both
/// the row filter to build `ArrowPredicate`s and the opener to build `ProjectionMask`s
#[derive(Debug, Clone)]
pub(crate) struct ParquetReadPlan {
    /// Projection mask built from leaf column indices in the Parquet schema.
    /// Using a `ProjectionMask` directly (rather than raw indices) prevents
    /// bugs from accidentally mixing up root vs leaf indices.
    pub projection_mask: ProjectionMask,
    /// The projected Arrow schema containing only the columns/fields required
    /// Struct types are pruned to include only the accessed sub-fields
    pub projected_schema: SchemaRef,
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
        Ok(
            build_parquet_read_plan(&self.expr, &self.file_schema, metadata)?.map(
                |(read_plan, required_bytes)| FilterCandidate {
                    expr: self.expr,
                    required_bytes,
                    read_plan,
                },
            ),
        )
    }
}

/// Traverses a `PhysicalExpr` tree to determine if any column references would
/// prevent the expression from being pushed down to the parquet decoder.
///
/// An expression cannot be pushed down if it references:
/// - Unsupported nested columns (whole struct references or list fields that are
///   not covered by the supported predicate set)
/// - Columns that don't exist in the file schema
///
/// Struct field access via `get_field` is supported when the resolved leaf type
/// is primitive (e.g. `get_field(struct_col, 'field') > 5`).
struct PushdownChecker<'schema> {
    /// Does the expression require any non-primitive columns (like structs)?
    non_primitive_columns: bool,
    /// Does the expression reference any columns not present in the file schema?
    projected_columns: bool,
    /// Indices into the file schema of columns required to evaluate the expression.
    /// Does not include struct columns accessed via `get_field`.
    required_columns: Vec<usize>,
    /// Struct field accesses via `get_field`.
    struct_field_accesses: Vec<StructFieldAccess>,
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
            required_columns: Vec::new(),
            struct_field_accesses: Vec::new(),
            allow_list_columns,
            file_schema,
        }
    }

    /// Checks whether a struct's root column exists in the file schema and, if so,
    /// records its index so the entire struct is decoded for filter evaluation.
    ///
    /// This is called when we see a `get_field` expression that resolves to a
    /// primitive leaf type. We only need the *root* column index because the
    /// Parquet reader decodes all leaves of a struct together.
    ///
    /// # Example
    ///
    /// Given file schema `{a: Int32, s: Struct(foo: Utf8, bar: Int64)}` and the
    /// expression `get_field(s, 'foo') = 'hello'`:
    ///
    /// - `column_name` = `"s"` (the root struct column)
    /// - `file_schema.index_of("s")` returns `1`
    /// - We push `1` into `required_columns`
    /// - Return `None` (no issue — traversal continues in the caller)
    ///
    /// If `"s"` is not in the file schema (e.g. a projected-away column), we set
    /// `projected_columns = true` and return `Jump` to skip the subtree.
    fn check_struct_field_column(
        &mut self,
        column_name: &str,
        field_path: Vec<String>,
    ) -> Option<TreeNodeRecursion> {
        let Ok(idx) = self.file_schema.index_of(column_name) else {
            self.projected_columns = true;
            return Some(TreeNodeRecursion::Jump);
        };

        self.struct_field_accesses.push(StructFieldAccess {
            root_index: idx,
            field_path,
        });

        None
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

        // Duplicates are handled by dedup() in into_sorted_columns()
        self.required_columns.push(idx);
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
            None
        } else {
            // Block pushdown for unsupported nested types:
            // - Structs (regardless of predicate support)
            // - Lists without supported predicates
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

    /// Consumes the checker and returns sorted, deduplicated column indices
    /// wrapped in a `PushdownColumns` struct.
    ///
    /// This method sorts the column indices and removes duplicates. The sort
    /// is required because downstream code relies on column indices being in
    /// ascending order for correct schema projection.
    fn into_sorted_columns(mut self) -> PushdownColumns {
        self.required_columns.sort_unstable();
        self.required_columns.dedup();
        PushdownColumns {
            required_columns: self.required_columns,
            struct_field_accesses: self.struct_field_accesses,
        }
    }
}

impl TreeNodeVisitor<'_> for PushdownChecker<'_> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        // Handle struct field access like `s['foo']['bar'] > 10`.
        //
        // DataFusion represents nested field access as `get_field(Column("s"), "foo")`
        // (or chained: `get_field(get_field(Column("s"), "foo"), "bar")`).
        //
        // We intercept the outermost `get_field` on the way *down* the tree so
        // the visitor never reaches the raw `Column("s")` node. Without this,
        // `check_single_column` would see that `s` is a Struct and reject it.
        //
        // The strategy:
        //   1. Match `get_field` whose first arg is a `Column` (the struct root).
        //   2. Check that the *resolved* return type is primitive — meaning we've
        //      drilled all the way to a leaf (e.g. `s['foo']` → Utf8).
        //   3. Record the root column index via `check_struct_field_column` and
        //      return `Jump` to skip visiting the children (the Column and the
        //      literal field-name args), since we've already handled them.
        //
        // If the return type is still nested (e.g. `s['nested_struct']` → Struct),
        // we fall through and let normal traversal continue, which will
        // eventually reject the expression when it hits the struct Column.
        if let Some(func) =
            ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(node.as_ref())
        {
            let args = func.args();

            if let Some(column) = args.first().and_then(|a| a.downcast_ref::<Column>()) {
                // for Map columns, get_field performs a runtime key lookup rather than a
                // schema-level field access so the entire Map column must be read,
                // we skip the struct field optimization and defer to normal Column traversal
                let is_map_column = self
                    .file_schema
                    .index_of(column.name())
                    .ok()
                    .map(|idx| {
                        matches!(
                            self.file_schema.field(idx).data_type(),
                            DataType::Map(_, _)
                        )
                    })
                    .unwrap_or(false);

                let return_type = func.return_type();

                if !is_map_column
                    && (!DataType::is_nested(return_type)
                        || self.is_nested_type_supported(return_type))
                {
                    // try to resolve all field name arguments to strinrg literals
                    // if any argument is not a string literal, we can not determine the exact
                    // leaf path so we fall back to reading the entire struct root column
                    let field_path = args[1..]
                        .iter()
                        .map(|arg| {
                            arg.downcast_ref::<Literal>().and_then(|lit| {
                                lit.value().try_as_str().flatten().map(|s| s.to_string())
                            })
                        })
                        .collect();

                    match field_path {
                        Some(path) => {
                            if let Some(recursion) =
                                self.check_struct_field_column(column.name(), path)
                            {
                                return Ok(recursion);
                            }
                        }
                        None => {
                            // Could not resolve field path — fall back to
                            // reading the entire struct root column.
                            if let Some(recursion) =
                                self.check_single_column(column.name())
                            {
                                return Ok(recursion);
                            }
                        }
                    }

                    return Ok(TreeNodeRecursion::Jump);
                }
            }
        }

        if let Some(column) = node.downcast_ref::<Column>()
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
/// Result of checking which columns are required for filter pushdown.
#[derive(Debug)]
struct PushdownColumns {
    /// Sorted, unique column indices into the file schema required to evaluate
    /// the filter expression. Must be in ascending order for correct schema
    /// projection matching. Does not include struct columns accessed via `get_field`.
    required_columns: Vec<usize>,
    /// Struct field accesses via `get_field`. Each entry records the root struct
    /// column index and the field path being accessed.
    struct_field_accesses: Vec<StructFieldAccess>,
}

/// Records a struct field access via `get_field(struct_col, 'field1', 'field2', ...)`.
///
/// This allows the row filter to project only the specific Parquet leaf columns
/// needed by the filter, rather than all leaves of the struct.
#[derive(Debug, Clone)]
struct StructFieldAccess {
    /// Arrow root column index of the struct in the file schema.
    root_index: usize,
    /// Field names forming the path into the struct.
    /// e.g., `["value"]` for `s['value']`, `["outer", "inner"]` for `s['outer']['inner']`.
    field_path: Vec<String>,
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
    Ok((!checker.prevents_pushdown()).then(|| checker.into_sorted_columns()))
}

/// Resolves which Parquet leaf columns and Arrow schema fields are needed
/// to evaluate `expr` against a Parquet file
///
/// Returns `Ok(Some((plan, required_bytes)))` when the expression can be
/// evaluated using only pushdown-compatible columns. `Ok(None)` when it
/// cannot (it references whole struct columns or columns missing from disk).
///
/// The `required_bytes` is the total compressed size of all referenced columns
/// across all row groups, used to estimate filter evaluation cost.
///
/// Note: this is a shared entry point used by both row filter construction and
/// the opener's projection logic
pub(crate) fn build_parquet_read_plan(
    expr: &Arc<dyn PhysicalExpr>,
    file_schema: &Schema,
    metadata: &ParquetMetaData,
) -> Result<Option<(ParquetReadPlan, usize)>> {
    let schema_descr = metadata.file_metadata().schema_descr();

    let Some(required_columns) = pushdown_columns(expr, file_schema)? else {
        return Ok(None);
    };

    let root_indices = &required_columns.required_columns;

    let mut leaf_indices =
        leaf_indices_for_roots(root_indices.iter().copied(), schema_descr);

    let struct_leaf_indices = resolve_struct_field_leaves(
        &required_columns.struct_field_accesses,
        file_schema,
        schema_descr,
    );
    leaf_indices.extend_from_slice(&struct_leaf_indices);
    leaf_indices.sort_unstable();
    leaf_indices.dedup();

    let required_bytes = size_of_columns(&leaf_indices, metadata)?;

    let projection_mask =
        ProjectionMask::leaves(schema_descr, leaf_indices.iter().copied());

    let projected_schema = build_filter_schema(
        file_schema,
        root_indices,
        &required_columns.struct_field_accesses,
    );

    Ok(Some((
        ParquetReadPlan {
            projection_mask,
            projected_schema,
        },
        required_bytes,
    )))
}

/// Builds a unified [`ParquetReadPlan`] for a set of projection expressions
///
/// Unlike [`build_parquet_read_plan`] (which is used for filter pushdown and
/// returns `None` when an expression references unsupported nested types or
/// missing columns), this function always succeeds. It collects every column
/// that *can* be resolved in the file and produces a leaf-level projection
/// mask. Columns missing from the file are silently skipped since the projection
/// layer handles those by inserting nulls.
pub(crate) fn build_projection_read_plan(
    exprs: impl IntoIterator<Item = Arc<dyn PhysicalExpr>>,
    file_schema: &Schema,
    schema_descr: &SchemaDescriptor,
) -> ParquetReadPlan {
    // fast path: if every expression is a plain Column reference, skip all
    // struct analysis and use root-level projection directly
    let exprs = exprs.into_iter().collect::<Vec<_>>();
    let all_plain_columns = exprs.iter().all(|e| e.downcast_ref::<Column>().is_some());

    if all_plain_columns {
        let mut root_indices: Vec<usize> = exprs
            .iter()
            .map(|e| e.downcast_ref::<Column>().unwrap().index())
            .collect();
        root_indices.sort_unstable();
        root_indices.dedup();

        let projection_mask =
            ProjectionMask::roots(schema_descr, root_indices.iter().copied());
        let projected_schema = Arc::new(
            file_schema
                .project(&root_indices)
                .expect("valid column indices"),
        );

        return ParquetReadPlan {
            projection_mask,
            projected_schema,
        };
    }

    // secondary fast path: if the schema has no struct columns, we can skip
    // PushdownChecker traversal and use root-level projection
    let has_struct_columns = file_schema
        .fields()
        .iter()
        .any(|f| matches!(f.data_type(), DataType::Struct(_)));

    if !has_struct_columns {
        let mut root_indices = exprs
            .into_iter()
            .flat_map(|e| collect_columns(&e).into_iter().map(|col| col.index()))
            .collect::<Vec<_>>();

        root_indices.sort_unstable();
        root_indices.dedup();

        let projection_mask =
            ProjectionMask::roots(schema_descr, root_indices.iter().copied());

        let projected_schema = Arc::new(
            file_schema
                .project(&root_indices)
                .expect("valid column indices"),
        );

        return ParquetReadPlan {
            projection_mask,
            projected_schema,
        };
    }

    let mut all_root_indices = Vec::new();
    let mut all_struct_accesses = Vec::new();

    for expr in exprs {
        let mut checker = PushdownChecker::new(file_schema, true);
        let _ = expr.visit(&mut checker);
        let columns = checker.into_sorted_columns();

        all_root_indices.extend_from_slice(&columns.required_columns);
        all_struct_accesses.extend(columns.struct_field_accesses);
    }

    all_root_indices.sort_unstable();
    all_root_indices.dedup();

    // when no struct field accesses were found, fall back to root-level projection
    // to match the performance of the simple path
    if all_struct_accesses.is_empty() {
        let projection_mask =
            ProjectionMask::roots(schema_descr, all_root_indices.iter().copied());
        let projected_schema = Arc::new(
            file_schema
                .project(&all_root_indices)
                .expect("valid column indices"),
        );

        return ParquetReadPlan {
            projection_mask,
            projected_schema,
        };
    }

    let leaf_indices = {
        let mut out =
            leaf_indices_for_roots(all_root_indices.iter().copied(), schema_descr);
        let struct_leaf_indices =
            resolve_struct_field_leaves(&all_struct_accesses, file_schema, schema_descr);

        out.extend_from_slice(&struct_leaf_indices);
        out.sort_unstable();
        out.dedup();

        out
    };

    let projection_mask =
        ProjectionMask::leaves(schema_descr, leaf_indices.iter().copied());

    let projected_schema =
        build_filter_schema(file_schema, &all_root_indices, &all_struct_accesses);

    ParquetReadPlan {
        projection_mask,
        projected_schema,
    }
}

fn leaf_indices_for_roots<I>(
    root_indices: I,
    schema_descr: &SchemaDescriptor,
) -> Vec<usize>
where
    I: IntoIterator<Item = usize>,
{
    // Always map root (Arrow) indices to Parquet leaf indices via the schema
    // descriptor. Arrow root indices only equal Parquet leaf indices when the
    // schema has no group columns (Struct, Map, etc.); when group columns
    // exist, their children become separate leaves and shift all subsequent
    // leaf indices.
    // Struct columns are unsupported.
    let root_set: BTreeSet<_> = root_indices.into_iter().collect();

    (0..schema_descr.num_columns())
        .filter(|leaf_idx| {
            root_set.contains(&schema_descr.get_column_root_idx(*leaf_idx))
        })
        .collect()
}

/// Resolves struct field access to specific Parquet leaf column indices
///
/// For every `StructFieldAccess`, finds the leaf columns in the Parquet schema
/// whose path matches the struct root name + field path. This avoids reading all
/// leaves of a struct when only specific fields are needed
fn resolve_struct_field_leaves(
    accesses: &[StructFieldAccess],
    file_schema: &Schema,
    schema_descr: &SchemaDescriptor,
) -> Vec<usize> {
    let mut leaf_indices = Vec::new();

    for access in accesses {
        let root_name = file_schema.field(access.root_index).name();
        let prefix = std::iter::once(root_name.as_str())
            .chain(access.field_path.iter().map(|p| p.as_str()))
            .collect::<Vec<_>>();

        for leaf_idx in 0..schema_descr.num_columns() {
            let col = schema_descr.column(leaf_idx);
            let col_path = col.path().parts();

            // A leaf matches if its path starts with our prefix.
            // e.g., prefix=["s", "value"] matches leaf path ["s", "value"]
            // prefix=["s", "outer"] matches ["s", "outer", "inner"]

            // a leaf matches iff its path starts with our prefix
            // for example: prefix=["s", "value"] matches leaf path ["s", "value"]
            //              prefix=["s", "outer"] matches ["s", "outer", "inner"]
            let leaf_matches_path = col_path.len() >= prefix.len()
                && col_path.iter().zip(prefix.iter()).all(|(a, b)| a == b);

            if leaf_matches_path {
                leaf_indices.push(leaf_idx);
            }
        }
    }

    leaf_indices
}

/// Builds a filter schema that includes only the fields actually accessed by the
/// filter expression.
///
/// For regular (non-struct) columns, the full field type is used.
/// For struct columns accessed via `get_field`, a pruned struct type is created
/// containing only the fields along the access path. Note: it must match the schema
/// that the Parquet reader produces when projecting specific struct leaves
fn build_filter_schema(
    file_schema: &Schema,
    regular_indices: &[usize],
    struct_field_accesses: &[StructFieldAccess],
) -> SchemaRef {
    let regular_set: BTreeSet<usize> = regular_indices.iter().copied().collect();

    let all_indices = regular_indices
        .iter()
        .copied()
        .chain(
            struct_field_accesses
                .iter()
                .map(|&StructFieldAccess { root_index, .. }| root_index),
        )
        .collect::<BTreeSet<_>>();

    let fields = all_indices
        .iter()
        .map(|&idx| {
            let field = file_schema.field(idx);

            // if this column appears as a regular (whole-column) reference,
            // keep the full type
            //
            // Pruning is only valid when the column is accessed exclusively
            // through struct field accesses
            if regular_set.contains(&idx) {
                return Arc::new(field.clone());
            }

            // collect all field paths that access this root struct column
            let field_paths = struct_field_accesses
                .iter()
                .filter_map(
                    |&StructFieldAccess {
                         root_index,
                         ref field_path,
                     }| {
                        (root_index == idx).then_some(field_path.as_slice())
                    },
                )
                .collect::<Vec<_>>();

            if field_paths.is_empty() {
                return Arc::new(field.clone());
            }

            let pruned_data_type = prune_struct_type(field.data_type(), &field_paths);
            Arc::new(Field::new(
                field.name(),
                pruned_data_type,
                field.is_nullable(),
            ))
        })
        .collect::<Vec<_>>();

    Arc::new(Schema::new_with_metadata(
        fields,
        file_schema.metadata().clone(),
    ))
}

fn prune_struct_type(dt: &DataType, paths: &[&[String]]) -> DataType {
    let DataType::Struct(fields) = dt else {
        return dt.clone();
    };

    let needed = paths
        .iter()
        .filter_map(|p| p.first().map(|s| s.as_str()))
        .collect::<BTreeSet<_>>();

    let pruned_fields = fields
        .iter()
        .filter_map(|f| {
            if !needed.contains(f.name().as_str()) {
                return None;
            }

            let sub_paths = paths
                .iter()
                .filter_map(|path| {
                    if path.first().map(|s| s.as_str()) == Some(f.name()) {
                        Some(&path[1..])
                    } else {
                        None
                    }
                })
                .filter(|sub| !sub.is_empty())
                .collect::<Vec<_>>();

            let out = if sub_paths.is_empty() {
                // Leaf of access path — keep the field as-is.
                Arc::clone(f)
            } else {
                // Recurse into nested struct.
                let pruned = prune_struct_type(f.data_type(), &sub_paths);
                Arc::new(Field::new(f.name(), pruned, f.is_nullable()))
            };

            Some(out)
        })
        .collect::<Vec<_>>();

    DataType::Struct(pruned_fields.into())
}

/// Checks if a predicate expression can be pushed down to the parquet decoder.
///
/// Returns `true` if all columns referenced by the expression:
/// - Exist in the provided schema
/// - Are primitive types OR list columns with supported predicates
///   (e.g., `array_has`, `array_has_all`, `array_has_any`, IS NULL, IS NOT NULL)
/// - Are struct columns accessed via `get_field` where the leaf type is primitive
/// - Direct references to whole struct columns will prevent pushdown
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
        candidates.sort_unstable_by_key(|c| c.required_bytes);
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
    use arrow::datatypes::Fields;
    use datafusion_common::ScalarValue;

    use arrow::array::{
        Int32Array, ListBuilder, StringArray, StringBuilder, StructArray,
    };
    use arrow::datatypes::{Field, TimeUnit::Nanosecond};
    use datafusion_expr::{Expr, col};
    use datafusion_functions::core::get_field;
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

    use datafusion_physical_expr::expressions::Column as PhysicalColumn;

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

        let expected_mask =
            ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [list_index]);
        assert_eq!(candidate.read_plan.projection_mask, expected_mask);
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
            .expect("creating expr adapter")
            .rewrite(expr)
            .expect("rewriting expression");
        let candidate = FilterCandidateBuilder::new(expr, file_schema.clone())
            .build(&metadata)
            .expect("building candidate")
            .expect("candidate expected");

        let mut row_filter = DatafusionArrowPredicate::try_new(
            candidate,
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
            .expect("creating expr adapter")
            .rewrite(expr)
            .expect("rewriting expression");
        let candidate = FilterCandidateBuilder::new(expr, file_schema)
            .build(&metadata)
            .expect("building candidate")
            .expect("candidate expected");

        let mut row_filter = DatafusionArrowPredicate::try_new(
            candidate,
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

    /// Regression test: when a schema has Struct columns, Arrow field indices diverge
    /// from Parquet leaf indices (Struct children become separate leaves). The
    /// `PrimitiveOnly` fast-path in `leaf_indices_for_roots` assumes they are equal,
    /// so a filter on a primitive column *after* a Struct gets the wrong leaf index.
    ///
    /// Schema:
    ///   Arrow indices:   col_a=0  struct_col=1  col_b=2
    ///   Parquet leaves:  col_a=0  struct_col.x=1  struct_col.y=2  col_b=3
    ///
    /// A filter on col_b should project Parquet leaf 3, but the bug causes it to
    /// project leaf 2 (struct_col.y).
    #[test]
    fn test_filter_pushdown_leaf_index_with_struct_in_schema() {
        use arrow::array::{Int32Array, StringArray, StructArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Int32, false),
            Field::new(
                "struct_col",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("x", DataType::Int32, true)),
                        Arc::new(Field::new("y", DataType::Int32, true)),
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new("col_b", DataType::Utf8, false),
        ]));

        let col_a = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let struct_col = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("x", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![10, 20, 30])) as _,
            ),
            (
                Arc::new(Field::new("y", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![100, 200, 300])) as _,
            ),
        ]));
        let col_b = Arc::new(StringArray::from(vec!["aaa", "target", "zzz"]));

        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![col_a, struct_col, col_b])
                .unwrap();

        let file = NamedTempFile::new().expect("temp file");
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), Arc::clone(&schema), None)
                .expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let reader_file = file.reopen().expect("reopen file");
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader_file)
            .expect("reader builder");
        let metadata = builder.metadata().clone();
        let file_schema = builder.schema().clone();

        // sanity check: 4 Parquet leaves, 3 Arrow fields
        assert_eq!(metadata.file_metadata().schema_descr().num_columns(), 4);
        assert_eq!(file_schema.fields().len(), 3);

        // build a filter candidate for `col_b = 'target'` through the public API
        let expr = col("col_b").eq(Expr::Literal(
            ScalarValue::Utf8(Some("target".to_string())),
            None,
        ));
        let expr = logical2physical(&expr, &file_schema);

        let candidate = FilterCandidateBuilder::new(expr, file_schema)
            .build(&metadata)
            .expect("building candidate")
            .expect("filter on primitive col_b should be pushable");

        // col_b is Parquet leaf 3 (shifted by struct_col's two children).
        let expected_mask =
            ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [3]);
        assert_eq!(
            candidate.read_plan.projection_mask, expected_mask,
            "projection_mask should select only leaf 3 for col_b"
        );
    }

    /// get_field(struct_col, 'a') on a struct with a primitive leaf should allow pushdown.
    #[test]
    fn get_field_on_struct_allows_pushdown() {
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "struct_col",
            DataType::Struct(
                vec![Arc::new(Field::new("a", DataType::Int32, true))].into(),
            ),
            true,
        )]));

        // get_field(struct_col, 'a') > 5
        let get_field_expr = get_field().call(vec![
            col("struct_col"),
            Expr::Literal(ScalarValue::Utf8(Some("a".to_string())), None),
        ]);
        let expr = get_field_expr.gt(Expr::Literal(ScalarValue::Int32(Some(5)), None));
        let expr = logical2physical(&expr, &table_schema);

        assert!(can_expr_be_pushed_down_with_schemas(&expr, &table_schema));
    }

    /// get_field on a struct field that resolves to a nested type should still block pushdown.
    #[test]
    fn get_field_on_nested_leaf_prevents_pushdown() {
        let inner_struct = DataType::Struct(
            vec![Arc::new(Field::new("x", DataType::Int32, true))].into(),
        );
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "struct_col",
            DataType::Struct(
                vec![Arc::new(Field::new("nested", inner_struct, true))].into(),
            ),
            true,
        )]));

        // get_field(struct_col, 'nested') IS NOT NULL — the leaf is still a struct
        let get_field_expr = get_field().call(vec![
            col("struct_col"),
            Expr::Literal(ScalarValue::Utf8(Some("nested".to_string())), None),
        ]);
        let expr = get_field_expr.is_not_null();
        let expr = logical2physical(&expr, &table_schema);

        assert!(!can_expr_be_pushed_down_with_schemas(&expr, &table_schema));
    }

    /// get_field returning a list inside a struct should allow pushdown when
    /// wrapped in a supported list predicate like `array_has_any`.
    /// e.g. `array_has_any(get_field(s, 'items'), make_array('x'))`
    #[test]
    fn get_field_list_leaf_with_array_predicate_allows_pushdown() {
        let item_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("id", DataType::Int32, true)),
                    Arc::new(Field::new("items", DataType::List(item_field), true)),
                ]
                .into(),
            ),
            true,
        )]));

        // array_has_any(get_field(s, 'items'), make_array('x'))
        let get_field_expr = get_field().call(vec![
            col("s"),
            Expr::Literal(ScalarValue::Utf8(Some("items".to_string())), None),
        ]);
        let expr = array_has_any(
            get_field_expr,
            make_array(vec![Expr::Literal(
                ScalarValue::Utf8(Some("x".to_string())),
                None,
            )]),
        );
        let expr = logical2physical(&expr, &table_schema);

        assert!(can_expr_be_pushed_down_with_schemas(&expr, &table_schema));
    }

    /// get_field on a struct produces correct Parquet leaf indices.
    #[test]
    fn get_field_filter_candidate_has_correct_leaf_indices() {
        use arrow::array::{Int32Array, StringArray, StructArray};

        // Schema: id (Int32), s (Struct{value: Int32, label: Utf8})
        // Parquet leaves: id=0, s.value=1, s.label=2
        let struct_fields: Fields = vec![
            Arc::new(Field::new("value", DataType::Int32, false)),
            Arc::new(Field::new("label", DataType::Utf8, false)),
        ]
        .into();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("s", DataType::Struct(struct_fields.clone()), false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StructArray::new(
                    struct_fields,
                    vec![
                        Arc::new(Int32Array::from(vec![10, 20, 30])) as _,
                        Arc::new(StringArray::from(vec!["a", "b", "c"])) as _,
                    ],
                    None,
                )),
            ],
        )
        .unwrap();

        let file = NamedTempFile::new().expect("temp file");
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), Arc::clone(&schema), None)
                .expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let reader_file = file.reopen().expect("reopen file");
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader_file)
            .expect("reader builder");
        let metadata = builder.metadata().clone();
        let file_schema = builder.schema().clone();

        // get_field(s, 'value') > 5
        let get_field_expr = get_field().call(vec![
            col("s"),
            Expr::Literal(ScalarValue::Utf8(Some("value".to_string())), None),
        ]);
        let expr = get_field_expr.gt(Expr::Literal(ScalarValue::Int32(Some(5)), None));
        let expr = logical2physical(&expr, &file_schema);

        let candidate = FilterCandidateBuilder::new(expr, file_schema)
            .build(&metadata)
            .expect("building candidate")
            .expect("get_field filter on struct should be pushable");

        // The filter accesses only s.value, so only Parquet leaf 1 is needed.
        // Leaf 2 (s.label) is not read, reducing unnecessary I/O.
        let expected_mask =
            ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [1]);
        assert_eq!(
            candidate.read_plan.projection_mask, expected_mask,
            "projection_mask should select only the accessed struct field leaf"
        );
    }

    /// Deeply nested get_field: get_field(struct_col, 'outer', 'inner') where the
    /// leaf is primitive should allow pushdown. The logical simplifier flattens
    /// nested get_field(get_field(col, 'a'), 'b') into get_field(col, 'a', 'b').
    #[test]
    fn get_field_deeply_nested_allows_pushdown() {
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(
                vec![Arc::new(Field::new(
                    "outer",
                    DataType::Struct(
                        vec![Arc::new(Field::new("inner", DataType::Int32, true))].into(),
                    ),
                    true,
                ))]
                .into(),
            ),
            true,
        )]));

        // s['outer']['inner'] > 5
        let get_field_expr = get_field().call(vec![
            col("s"),
            Expr::Literal(ScalarValue::Utf8(Some("outer".to_string())), None),
            Expr::Literal(ScalarValue::Utf8(Some("inner".to_string())), None),
        ]);
        let expr = get_field_expr.gt(Expr::Literal(ScalarValue::Int32(Some(5)), None));
        let expr = logical2physical(&expr, &table_schema);

        assert!(can_expr_be_pushed_down_with_schemas(&expr, &table_schema));
    }

    /// End-to-end: deeply nested get_field filter produces correct leaf indices
    /// and the filter actually works against a Parquet file.
    #[test]
    fn get_field_deeply_nested_filter_candidate() {
        use arrow::array::{Int32Array, StringArray, StructArray};

        // Schema: id (Int32), s (Struct{outer: Struct{extra: Int32, inner: Int32}, tag: Utf8})
        // Parquet leaves: id=0, s.outer.extra=1, s.outer.inner=2, s.tag=3
        let inner_fields: Fields = vec![
            Arc::new(Field::new("extra", DataType::Int32, false)),
            Arc::new(Field::new("inner", DataType::Int32, false)),
        ]
        .into();
        let outer_fields: Fields = vec![
            Arc::new(Field::new(
                "outer",
                DataType::Struct(inner_fields.clone()),
                false,
            )),
            Arc::new(Field::new("tag", DataType::Utf8, false)),
        ]
        .into();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("s", DataType::Struct(outer_fields.clone()), false),
        ]));

        let inner_struct = StructArray::new(
            inner_fields,
            vec![
                Arc::new(Int32Array::from(vec![100, 200, 300])) as _,
                Arc::new(Int32Array::from(vec![10, 20, 30])) as _,
            ],
            None,
        );
        let outer_struct = StructArray::new(
            outer_fields,
            vec![
                Arc::new(inner_struct) as _,
                Arc::new(StringArray::from(vec!["x", "y", "z"])) as _,
            ],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(outer_struct),
            ],
        )
        .unwrap();

        let file = NamedTempFile::new().expect("temp file");
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), Arc::clone(&schema), None)
                .expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let reader_file = file.reopen().expect("reopen file");
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader_file)
            .expect("reader builder");
        let metadata = builder.metadata().clone();
        let file_schema = builder.schema().clone();

        // Parquet should have 4 leaves: id=0, s.outer.extra=1, s.outer.inner=2, s.tag=3
        assert_eq!(metadata.file_metadata().schema_descr().num_columns(), 4);

        // get_field(s, 'outer', 'inner') > 15
        // Should only need leaf 2 (s.outer.inner), not leaf 1 (s.outer.extra) or leaf 3 (s.tag).
        let get_field_expr = get_field().call(vec![
            col("s"),
            Expr::Literal(ScalarValue::Utf8(Some("outer".to_string())), None),
            Expr::Literal(ScalarValue::Utf8(Some("inner".to_string())), None),
        ]);
        let expr = get_field_expr.gt(Expr::Literal(ScalarValue::Int32(Some(15)), None));
        let expr = logical2physical(&expr, &file_schema);

        let candidate = FilterCandidateBuilder::new(expr, file_schema)
            .build(&metadata)
            .expect("building candidate")
            .expect("deeply nested get_field filter should be pushable");

        // Only s.outer.inner (leaf 2) should be projected,
        let expected_mask =
            ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [2]);
        assert_eq!(
            candidate.read_plan.projection_mask, expected_mask,
            "projection_mask should select only leaf 2 for s.outer.inner, skipping sibling and cousin leaves"
        );
    }

    /// End-to-end: get_field filter on a struct column with multiple fields
    /// reads only the needed leaf and correctly filters rows during Parquet decoding.
    #[test]
    fn get_field_end_to_end_filters_rows() {
        // Schema: id (Int32), s (Struct{value: Int32, label: Utf8})
        // Parquet leaves: id=0, s.value=1, s.label=2
        let struct_fields: Fields = vec![
            Arc::new(Field::new("value", DataType::Int32, false)),
            Arc::new(Field::new("label", DataType::Utf8, false)),
        ]
        .into();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("s", DataType::Struct(struct_fields.clone()), false),
        ]));

        // +----+--------------------------+
        // | id | s                        |
        // +----+--------------------------+
        // |  1 | {value: 10, label: "a"}  |
        // |  2 | {value: 20, label: "b"}  |
        // |  3 | {value: 30, label: "c"}  |
        // +----+--------------------------+
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StructArray::new(
                    struct_fields,
                    vec![
                        Arc::new(Int32Array::from(vec![10, 20, 30])) as _,
                        Arc::new(StringArray::from(vec!["a", "b", "c"])) as _,
                    ],
                    None,
                )),
            ],
        )
        .unwrap();

        let file = NamedTempFile::new().expect("temp file");
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), Arc::clone(&schema), None)
                .expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let reader_file = file.reopen().expect("reopen file");
        let parquet_reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(reader_file)
                .expect("reader builder");
        let metadata = parquet_reader_builder.metadata().clone();
        let file_schema = parquet_reader_builder.schema().clone();

        // get_field(s, 'value') > 15  — should match rows with value=20 and value=30
        let get_field_expr = get_field().call(vec![
            col("s"),
            Expr::Literal(ScalarValue::Utf8(Some("value".to_string())), None),
        ]);
        let predicate_expr =
            get_field_expr.gt(Expr::Literal(ScalarValue::Int32(Some(15)), None));
        let expr = logical2physical(&predicate_expr, &file_schema);

        let metrics = ExecutionPlanMetricsSet::new();
        let file_metrics = ParquetFileMetrics::new(0, "struct_e2e.parquet", &metrics);

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

        assert_eq!(total_rows, 2, "expected 2 rows matching value > 15");
        assert_eq!(file_metrics.pushdown_rows_pruned.value(), 1);
        assert_eq!(file_metrics.pushdown_rows_matched.value(), 2);
    }

    #[test]
    fn projection_read_plan_preserves_full_struct() {
        // Schema: id (Int32), s (Struct{value: Int32, label: Utf8})
        // Parquet leaves: id=0, s.value=1, s.label=2
        let struct_fields: Fields = vec![
            Arc::new(Field::new("value", DataType::Int32, false)),
            Arc::new(Field::new("label", DataType::Utf8, false)),
        ]
        .into();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("s", DataType::Struct(struct_fields.clone()), false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StructArray::new(
                    struct_fields,
                    vec![
                        Arc::new(Int32Array::from(vec![10, 20, 30])) as _,
                        Arc::new(StringArray::from(vec!["a", "b", "c"])) as _,
                    ],
                    None,
                )),
            ],
        )
        .unwrap();

        let file = NamedTempFile::new().expect("temp file");
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), Arc::clone(&schema), None)
                .expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let reader_file = file.reopen().expect("reopen file");
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader_file)
            .expect("reader builder");
        let metadata = builder.metadata().clone();
        let file_schema = builder.schema().clone();
        let schema_descr = metadata.file_metadata().schema_descr();

        // Simulate SELECT * output projection: Column("id") and Column("s")
        // Plus a get_field(s, 'value') expression from the pushed-down filter
        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(PhysicalColumn::new("id", 0)),
            Arc::new(PhysicalColumn::new("s", 1)),
            logical2physical(
                &get_field().call(vec![
                    col("s"),
                    Expr::Literal(ScalarValue::Utf8(Some("value".to_string())), None),
                ]),
                &file_schema,
            ),
        ];

        let read_plan = build_projection_read_plan(exprs, &file_schema, schema_descr);

        // The projected schema must have the FULL struct type because Column("s")
        // is in the projection. It should NOT be narrowed to Struct{value: Int32}.
        let s_field = read_plan.projected_schema.field_with_name("s").unwrap();
        assert_eq!(
            s_field.data_type(),
            &DataType::Struct(
                vec![
                    Arc::new(Field::new("value", DataType::Int32, false)),
                    Arc::new(Field::new("label", DataType::Utf8, false)),
                ]
                .into()
            ),
        );

        // all3 Parquet leaves should be in the projection mask
        let expected_mask = ProjectionMask::leaves(schema_descr, [0, 1, 2]);
        assert_eq!(read_plan.projection_mask, expected_mask,);
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
