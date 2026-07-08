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
//! field references into Parquet leaf indices.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use datafusion_common::tree_node::TreeNode;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::utils::collect_columns;

use crate::nested_schema_pruning::{
    CastColumnAccess, clip_for_cast, count_leaves, field_with_type,
    prune_type_by_kept_offsets,
};
use crate::row_filter::PushdownChecker;

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
    prune_nested_casts: bool,
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

    // secondary fast path: if no column contains a struct at any nesting
    // level, there are no leaves to prune and we can skip PushdownChecker
    // traversal and use root-level projection
    let has_struct_columns = file_schema
        .fields()
        .iter()
        .any(|f| contains_struct(f.data_type()));

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
    let mut all_cast_accesses = Vec::new();

    for expr in exprs {
        let mut checker = PushdownChecker::new(file_schema, true)
            .with_cast_collection(prune_nested_casts);
        let _ = expr.visit(&mut checker);
        let columns = checker.into_sorted_columns();

        all_root_indices.extend_from_slice(&columns.required_columns);
        all_struct_accesses.extend(columns.struct_field_accesses);
        all_cast_accesses.extend(columns.cast_accesses);
    }

    all_root_indices.sort_unstable();
    all_root_indices.dedup();

    // A whole-column reference reads every leaf of the root, so clipping the
    // same root for a cast (or a struct field access) would be overridden
    // anyway: drop those accesses up front.
    let whole_roots: BTreeSet<usize> = all_root_indices.iter().copied().collect();
    all_cast_accesses.retain(|c| !whole_roots.contains(&c.root_index));

    if !all_cast_accesses.is_empty() {
        return build_read_plan_with_cast_clipping(
            file_schema,
            schema_descr,
            &all_root_indices,
            &all_struct_accesses,
            &all_cast_accesses,
        );
    }

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

/// Does this type contain a struct at any nesting depth?
fn contains_struct(dt: &DataType) -> bool {
    match dt {
        DataType::Struct(_) => true,
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => contains_struct(f.data_type()),
        DataType::Dictionary(_, value) => contains_struct(value),
        DataType::RunEndEncoded(_, value) => contains_struct(value.data_type()),
        _ => false,
    }
}

/// Builds a [`ParquetReadPlan`] when at least one projected root column is
/// consumed through a cast to a narrower nested type.
///
/// Per root, in ascending root-index order:
/// - roots referenced as whole columns keep every leaf and their full
///   physical field (whole-column reads take precedence; cast accesses on
///   such roots were already dropped by the caller);
/// - roots consumed only through casts and/or `get_field` accesses keep the
///   union of the leaves those accesses consume, and their projected field
///   type is derived from that union.
///
/// Falls back to keeping all leaves of a root whenever its clipping is not
/// provably safe (see [`crate::nested_schema_pruning`]).
fn build_read_plan_with_cast_clipping(
    file_schema: &Schema,
    schema_descr: &SchemaDescriptor,
    whole_root_indices: &[usize],
    struct_accesses: &[StructFieldAccess],
    cast_accesses: &[CastColumnAccess],
) -> ParquetReadPlan {
    let whole_roots: BTreeSet<usize> = whole_root_indices.iter().copied().collect();

    // Union of kept *absolute* leaf indices per clipped root.
    let mut kept_by_root: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new();
    // Roots where clipping had to be abandoned (treated as whole reads).
    let mut fallback_roots: BTreeSet<usize> = BTreeSet::new();

    for access in cast_accesses {
        let root = access.root_index;
        if fallback_roots.contains(&root) {
            continue;
        }
        let physical_type = file_schema.field(root).data_type();
        let root_leaves = leaf_indices_for_roots([root], schema_descr);

        // Defensive: the arrow type's leaf count must agree with the parquet
        // schema (it can diverge if the file embeds a different arrow
        // schema). If not, never risk a wrong mask — read the whole root.
        if root_leaves.len() != count_leaves(physical_type) {
            fallback_roots.insert(root);
            kept_by_root.remove(&root);
            continue;
        }

        match clip_for_cast(physical_type, &access.target_type) {
            Some(kept_offsets) => {
                let start = root_leaves[0];
                kept_by_root
                    .entry(root)
                    .or_default()
                    .extend(kept_offsets.iter().map(|o| start + o));
            }
            // Nothing prunable for this cast: every leaf is consumed.
            None => {
                fallback_roots.insert(root);
                kept_by_root.remove(&root);
            }
        }
    }

    // `get_field` accesses union into the same per-root sets. Accesses on
    // whole-read or fallback roots are covered by the full read.
    for access in struct_accesses {
        let root = access.root_index;
        if whole_roots.contains(&root) || fallback_roots.contains(&root) {
            continue;
        }
        let leaves = resolve_struct_field_leaves(
            std::slice::from_ref(access),
            file_schema,
            schema_descr,
        );
        match kept_by_root.get_mut(&root) {
            Some(kept) => kept.extend(leaves),
            None => {
                kept_by_root.entry(root).or_default().extend(leaves);
            }
        }
    }

    // Assemble mask + schema over every referenced root in ascending order.
    let all_roots: BTreeSet<usize> = whole_roots
        .iter()
        .copied()
        .chain(fallback_roots.iter().copied())
        .chain(kept_by_root.keys().copied())
        .collect();

    let mut leaf_indices: Vec<usize> = Vec::new();
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(all_roots.len());

    for &root in &all_roots {
        let field = file_schema.field(root);
        match kept_by_root.get(&root) {
            Some(kept) if !kept.is_empty() => {
                let root_leaves = leaf_indices_for_roots([root], schema_descr);
                let start = root_leaves[0];
                let relative: BTreeSet<usize> =
                    kept.iter().map(|abs| abs - start).collect();
                match prune_type_by_kept_offsets(field.data_type(), &relative) {
                    Some(pruned_type) => {
                        leaf_indices.extend(kept.iter().copied());
                        fields.push(field_with_type(field, pruned_type));
                    }
                    // Cannot express the kept set as a type: full read.
                    None => {
                        leaf_indices.extend(root_leaves);
                        fields.push(Arc::new(field.clone()));
                    }
                }
            }
            // Whole-column read (whole reference, fallback, or empty set).
            _ => {
                leaf_indices.extend(leaf_indices_for_roots([root], schema_descr));
                fields.push(Arc::new(field.clone()));
            }
        }
    }

    leaf_indices.sort_unstable();
    leaf_indices.dedup();

    ParquetReadPlan {
        projection_mask: ProjectionMask::leaves(
            schema_descr,
            leaf_indices.iter().copied(),
        ),
        projected_schema: Arc::new(Schema::new_with_metadata(
            fields,
            file_schema.metadata().clone(),
        )),
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
            // prefix=["s", "outer"] matches ["s", "outer", "inner"]

            // a leaf matches if its path starts with our prefix
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

        let read_plan =
            build_projection_read_plan(exprs, &file_schema, schema_descr, true);

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
