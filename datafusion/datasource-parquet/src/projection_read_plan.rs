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

//! Resolution of expressions against a Parquet file's schema into a
//! [`ParquetReadPlan`]: the leaf-level [`ProjectionMask`] to install on the
//! decoder plus the Arrow schema the decoder will emit under that mask.
//!
//! This is shared by the opener's projection handling (via
//! [`build_projection_read_plan`]) and row-filter construction (via
//! [`crate::row_filter`]), which both need to translate column and struct
//! field references into Parquet leaf indices. [`PushdownChecker`], the
//! expression traversal that discovers those references, lives here as well
//! so that [`crate::row_filter`] depends on this module and not vice versa.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use datafusion_common::Result;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_functions::core::file_row_index::FileRowIndexFunc;
use datafusion_functions::core::getfield::GetFieldFunc;
use datafusion_physical_expr::expressions::{Column, Literal};
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};

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

/// Records a struct field access via `get_field(struct_col, 'field1', 'field2', ...)`.
///
/// This allows the row filter to project only the specific Parquet leaf columns
/// needed by the filter, rather than all leaves of the struct.
#[derive(Debug, Clone)]
pub(crate) struct StructFieldAccess {
    /// Arrow root column index of the struct in the file schema.
    pub(crate) root_index: usize,
    /// Field names forming the path into the struct.
    /// e.g., `["value"]` for `s['value']`, `["outer", "inner"]` for `s['outer']['inner']`.
    pub(crate) field_path: Vec<String>,
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
pub(crate) struct PushdownChecker<'schema> {
    /// Does the expression require any non-primitive columns (like structs)?
    non_primitive_columns: bool,
    /// Does the expression reference any columns not present in the file schema?
    projected_columns: bool,
    /// Does the expression references a ScalarUDF that requires some rewrite
    /// and therefore can't be pushed down into the row-filter.
    has_unpushable_udfs: bool,
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
    pub(crate) fn new(file_schema: &'schema Schema, allow_list_columns: bool) -> Self {
        Self {
            non_primitive_columns: false,
            projected_columns: false,
            has_unpushable_udfs: false,
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
    pub(crate) fn prevents_pushdown(&self) -> bool {
        self.non_primitive_columns || self.projected_columns || self.has_unpushable_udfs
    }

    /// Consumes the checker and returns sorted, deduplicated column indices
    /// wrapped in a `PushdownColumns` struct.
    ///
    /// This method sorts the column indices and removes duplicates. The sort
    /// is required because downstream code relies on column indices being in
    /// ascending order for correct schema projection.
    pub(crate) fn into_sorted_columns(mut self) -> PushdownColumns {
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

        if ScalarFunctionExpr::try_downcast_func::<FileRowIndexFunc>(node.as_ref())
            .is_some()
        {
            self.has_unpushable_udfs = true;
            return Ok(TreeNodeRecursion::Jump);
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

/// Result of checking which columns are required for filter pushdown.
#[derive(Debug)]
pub(crate) struct PushdownColumns {
    /// Sorted, unique column indices into the file schema required to evaluate
    /// the filter expression. Must be in ascending order for correct schema
    /// projection matching. Does not include struct columns accessed via `get_field`.
    pub(crate) required_columns: Vec<usize>,
    /// Struct field accesses via `get_field`. Each entry records the root struct
    /// column index and the field path being accessed.
    pub(crate) struct_field_accesses: Vec<StructFieldAccess>,
}

/// Builds a unified [`ParquetReadPlan`] for a set of projection expressions
///
/// Unlike [`crate::row_filter::build_parquet_read_plan`] (which is used for
/// filter pushdown and returns `None` when an expression references
/// unsupported nested types or missing columns), this function always
/// succeeds. It collects every column that *can* be resolved in the file and
/// produces a leaf-level projection mask. Columns missing from the file are
/// silently skipped since the projection layer handles those by inserting
/// nulls.
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

pub(crate) fn leaf_indices_for_roots<I>(
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
pub(crate) fn resolve_struct_field_leaves(
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
            //       prefix=["s", "outer"] matches ["s", "outer", "inner"]
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
pub(crate) fn build_filter_schema(
    file_schema: &Schema,
    regular_indices: &[usize],
    struct_field_accesses: &[StructFieldAccess],
) -> SchemaRef {
    let regular_set: BTreeSet<usize> = regular_indices.iter().copied().collect();
    let paths_by_root = group_access_paths_by_root(struct_field_accesses);

    let all_indices = regular_indices
        .iter()
        .copied()
        .chain(paths_by_root.keys().copied())
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

            let Some(field_paths) = paths_by_root.get(&idx) else {
                return Arc::new(field.clone());
            };

            let pruned_data_type = prune_struct_type(field.data_type(), field_paths);
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

/// Groups struct field access paths once for the root schema level.
///
/// Each map entry contains the complete field paths accessed below a root
/// column. Recursive pruning groups these paths by their next component at each
/// nested struct level.
fn group_access_paths_by_root(
    struct_field_accesses: &[StructFieldAccess],
) -> BTreeMap<usize, Vec<&[String]>> {
    let mut paths_by_root: BTreeMap<usize, Vec<&[String]>> = BTreeMap::new();
    for StructFieldAccess {
        root_index,
        field_path,
    } in struct_field_accesses
    {
        paths_by_root
            .entry(*root_index)
            .or_default()
            .push(field_path.as_slice());
    }

    paths_by_root
}

/// Groups access paths once for the current struct level.
///
/// The map key is the field name at this level. The map value is the list of
/// remaining path suffixes below that field. An empty suffix means the access
/// path terminates at that field, so the full field must be preserved.
fn group_paths_by_next_field<'a>(
    paths: &'a [&'a [String]],
) -> BTreeMap<&'a str, Vec<&'a [String]>> {
    let mut paths_by_field: BTreeMap<&str, Vec<&[String]>> = BTreeMap::new();
    for path in paths {
        if let Some((field, sub_path)) = path.split_first() {
            paths_by_field
                .entry(field.as_str())
                .or_default()
                .push(sub_path);
        }
    }

    paths_by_field
}

fn prune_struct_type(dt: &DataType, paths: &[&[String]]) -> DataType {
    let DataType::Struct(fields) = dt else {
        return dt.clone();
    };

    let paths_by_field = group_paths_by_next_field(paths);

    let pruned_fields = fields
        .iter()
        .filter_map(|f| {
            let sub_paths = paths_by_field.get(f.name().as_str())?;

            let out = if sub_paths.iter().any(|sub| sub.is_empty()) {
                // Leaf of access path — keep the field as-is.
                Arc::clone(f)
            } else {
                // Recurse into nested struct.
                let pruned = prune_struct_type(f.data_type(), sub_paths);
                Arc::new(Field::new(f.name(), pruned, f.is_nullable()))
            };

            Some(out)
        })
        .collect::<Vec<_>>();

    DataType::Struct(pruned_fields.into())
}

#[cfg(test)]
mod test {
    use super::*;
    use Column as PhysicalColumn;
    use arrow::array::{Int32Array, RecordBatch, StringArray, StructArray};
    use arrow::datatypes::Fields;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{Expr, col};
    use datafusion_functions::core::get_field;
    use datafusion_physical_expr::planner::logical2physical;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::NamedTempFile;

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
}
