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

//! Schema-driven nested projection pruning.
//!
//! When a scan's projection consumes a nested column only through a cast to a
//! *narrower* nested type, for example the file contains
//! `events: List<Struct<x, y, z, ...>>` but the expression is
//! `CAST(events AS List<Struct<x, y>>)`, the Parquet reader does not need to
//! fetch or decode the leaves the cast target never names. This module
//! computes which Parquet leaves survive such a cast, and the Arrow type the
//! reader will emit for them, by walking the physical and target type trees
//! in parallel and matching struct fields by name (the equivalent of Spark's
//! `ParquetReadSupport.clipParquetSchema`).
//!
//! This situation arises whenever a table's logical schema declares a nested
//! column narrower than the physical Parquet file: the physical expression
//! adapter rewrites the projected column into exactly such a whole-column
//! cast (see `datafusion_physical_expr_adapter`). Engines like Spark
//! communicate nested projection pruning to the scan this way, as a clipped
//! read *schema* rather than as `get_field` expressions.
//!
//! # Safety of clipping
//!
//! The runtime cast for nested types
//! ([`datafusion_common::nested_struct::cast_column`]) consumes source struct
//! children exclusively by looking up the *target* field names, recursively
//! through list wrappers. Physical subtrees not named by the target are
//! provably dead: removing them from the read cannot change the cast's
//! output. That holds for *any*
//! [`CastExpr`](datafusion_physical_expr::expressions::CastExpr) over a
//! nested type, not just the ones the schema adapter inserts:
//! `ColumnarValue::cast_to` routes every
//! cast for which
//! [`requires_nested_struct_cast`](datafusion_common::nested_struct::requires_nested_struct_cast)
//! holds, the same predicate the projection analysis gates on, through
//! `cast_column`.
//!
//! Struct-level nullability is preserved because the Parquet reader
//! reconstructs ancestor validity from the definition levels of any surviving
//! leaf, so every struct level that is clipped must keep at least one leaf.
//! A struct cast with zero field-name overlap at *any* nesting depth would
//! break that: the reader drops a field whose leaves are all masked out, so
//! the emitted type would not match the one predicted here. Such a cast is
//! rejected during physical planning
//! (`datafusion_common::nested_struct::validate_struct_compatibility`, called
//! recursively from `DefaultPhysicalExprAdapter::rewrite`) and by the logical
//! planner's own castability check, so it should never reach this module; if
//! one does anyway (a custom `PhysicalExprAdapter` could build one),
//! [`clip_for_cast`] detects the empty level and declines to clip.
//!
//! The clip is *total*: any type shape it does not understand (maps,
//! dictionaries, wrapper-kind mismatches, ...) keeps all of its leaves, so
//! the worst case is today's behavior of reading the full column. Map values
//! are deliberately not clipped: the runtime cast routes maps through Arrow's
//! positional struct cast, which requires all children to be present. Nor are
//! `ListView`/`LargeListView`/`Dictionary` wrappers clipped here, even though
//! `cast_column` does recurse through them by name. That is a conservative
//! choice (safe, since the worst case is still just a full read) left as a
//! candidate follow-up rather than something this module currently handles.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, Fields};

/// The single child type one level of container nesting wraps, or `None` for
/// a type this module does not descend through (leaves, `Struct`, `Map`, and
/// wrapper kinds this module intentionally does not clip, see the module
/// doc). Shared by [`count_leaves`] and [`contains_struct`], which otherwise
/// need to agree on the exact same set of container variants.
fn nested_child(dt: &DataType) -> Option<&DataType> {
    match dt {
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => Some(f.data_type()),
        DataType::Dictionary(_, value) => Some(value),
        DataType::RunEndEncoded(_, value) => Some(value.data_type()),
        _ => None,
    }
}

/// Clip `physical` against `cast_target`, returning the Parquet leaves the
/// cast actually consumes (as offsets relative to the root column's first
/// leaf, sorted ascending and non-empty) together with the Arrow type the
/// reader will emit for exactly those leaves.
///
/// Returns `None` when nothing can be pruned (every leaf is consumed, or the
/// shapes do not allow safe clipping), in which case the caller should read
/// the whole column as before. This function never fails: unknown shapes
/// degrade to keeping all leaves.
pub(crate) fn clip_for_cast(
    physical: &DataType,
    cast_target: &DataType,
) -> Option<(Vec<usize>, DataType)> {
    let total = count_leaves(physical);
    let mut kept = Vec::new();
    let mut next_leaf = 0;
    let mut unclippable = false;
    let pruned_type = clip_type(
        physical,
        cast_target,
        &mut next_leaf,
        &mut kept,
        &mut unclippable,
    );
    debug_assert_eq!(next_leaf, total, "leaf accounting must cover the type");
    if unclippable || kept.is_empty() || kept.len() >= total {
        return None;
    }
    Some((kept, pruned_type))
}

/// Number of Parquet leaf columns a (Parquet-derived) Arrow type occupies.
pub(crate) fn count_leaves(dt: &DataType) -> usize {
    match dt {
        DataType::Struct(fields) => {
            fields.iter().map(|f| count_leaves(f.data_type())).sum()
        }
        _ => nested_child(dt).map_or(1, count_leaves),
    }
}

/// Does this type contain a struct at any nesting depth? Used as a fast-path
/// gate: a root with no struct anywhere in its type has no leaves this
/// module could ever clip.
pub(crate) fn contains_struct(dt: &DataType) -> bool {
    matches!(dt, DataType::Struct(_)) || nested_child(dt).is_some_and(contains_struct)
}

/// Above this many target fields, matching physical children against them one
/// by one turns into a quadratic string comparison; build a name lookup
/// instead. Below it the map's allocation costs more than the linear scan it
/// saves (Spark's `ParquetReadSupport.clipParquetGroupFields` builds the map
/// unconditionally; struct widths in practice are small enough that the
/// threshold is worth the branch).
const LINEAR_FIELD_SCAN_MAX: usize = 8;

/// Find `name` among `fields`, using `by_name` when it was worth building.
/// Duplicate names resolve to the first occurrence either way.
fn lookup_field<'a>(
    fields: &'a Fields,
    by_name: &Option<HashMap<&'a str, &'a FieldRef>>,
    name: &str,
) -> Option<&'a FieldRef> {
    match by_name {
        Some(map) => map.get(name).copied(),
        None => fields.iter().find(|f| f.name() == name),
    }
}

/// Recursive walker: advances `next_leaf` across every leaf of `physical`,
/// pushing the offsets the cast target consumes into `kept`, and returns the
/// Arrow type the reader emits for those kept leaves.
///
/// `unclippable` is set when a shape is encountered whose emitted type this
/// module cannot predict; the caller must then read the whole column. The walk
/// still runs to completion so `next_leaf` stays a valid leaf count.
fn clip_type(
    physical: &DataType,
    target: &DataType,
    next_leaf: &mut usize,
    kept: &mut Vec<usize>,
    unclippable: &mut bool,
) -> DataType {
    match (physical, target) {
        (DataType::Struct(p_children), DataType::Struct(t_children)) => {
            let t_by_name = (t_children.len() > LINEAR_FIELD_SCAN_MAX).then(|| {
                let mut map = HashMap::with_capacity(t_children.len());
                for tc in t_children.iter() {
                    map.entry(tc.name().as_str()).or_insert(tc);
                }
                map
            });
            let kept_children: Fields = p_children
                .iter()
                .filter_map(|pc| {
                    let Some(tc) = lookup_field(t_children, &t_by_name, pc.name()) else {
                        skip_leaves(pc.data_type(), next_leaf);
                        return None;
                    };
                    let before = kept.len();
                    let pruned = clip_type(
                        pc.data_type(),
                        tc.data_type(),
                        next_leaf,
                        kept,
                        unclippable,
                    );
                    if kept.len() == before {
                        // This child matched by name but kept no leaves at
                        // all, which only happens when a nested struct level
                        // below it shares no field name with its target. The
                        // reader drops a field whose leaves are all masked
                        // out, so the emitted type could not be predicted;
                        // give up on clipping this column entirely rather
                        // than promise a type the decoder will not produce.
                        // (`DefaultPhysicalExprAdapter` never builds such a
                        // cast — `validate_struct_compatibility` rejects a
                        // zero-overlap struct level at planning time — but a
                        // custom `PhysicalExprAdapter` could.)
                        *unclippable = true;
                    }
                    Some(field_with_type(pc, pruned))
                })
                .collect();
            DataType::Struct(kept_children)
        }
        (DataType::List(p_item), DataType::List(t_item)) => {
            let pruned = clip_type(
                p_item.data_type(),
                t_item.data_type(),
                next_leaf,
                kept,
                unclippable,
            );
            DataType::List(field_with_type(p_item, pruned))
        }
        (DataType::LargeList(p_item), DataType::LargeList(t_item)) => {
            let pruned = clip_type(
                p_item.data_type(),
                t_item.data_type(),
                next_leaf,
                kept,
                unclippable,
            );
            DataType::LargeList(field_with_type(p_item, pruned))
        }
        // Anything else, leaf pairs, wrapper-kind mismatches, maps,
        // dictionaries, fixed-size lists, views, is kept wholesale.
        _ => keep_all_leaves(physical, next_leaf, kept),
    }
}

/// Keep every leaf of `dt` (no pruning below this point); returns `dt`
/// unchanged since nothing was clipped.
fn keep_all_leaves(
    dt: &DataType,
    next_leaf: &mut usize,
    kept: &mut Vec<usize>,
) -> DataType {
    let n = count_leaves(dt);
    kept.extend(*next_leaf..*next_leaf + n);
    *next_leaf += n;
    dt.clone()
}

fn skip_leaves(dt: &DataType, next_leaf: &mut usize) {
    *next_leaf += count_leaves(dt);
}

/// A projected root column that is consumed through a cast to a narrower
/// nested type (`CAST(col AS target_type)`), recorded during projection
/// analysis.
#[derive(Debug, Clone)]
pub(crate) struct CastColumnAccess {
    /// Arrow root column index of the column in the file schema.
    pub(crate) root_index: usize,
    /// The cast's target type.
    pub(crate) target_type: DataType,
}

/// Rebuild `field` with a new data type, preserving name, nullability and
/// metadata.
pub(crate) fn field_with_type(field: &Field, data_type: DataType) -> FieldRef {
    Arc::new(field.clone().with_data_type(data_type))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utf8(name: &str) -> Field {
        Field::new(name, DataType::Utf8, true)
    }

    fn int64(name: &str) -> Field {
        Field::new(name, DataType::Int64, true)
    }

    fn struct_of(fields: Vec<Field>) -> DataType {
        DataType::Struct(Fields::from(fields))
    }

    fn list_of(item: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("item", item, true)))
    }

    #[test]
    fn count_leaves_shapes() {
        assert_eq!(count_leaves(&DataType::Int32), 1);
        assert_eq!(count_leaves(&struct_of(vec![utf8("a"), int64("b")])), 2);
        assert_eq!(
            count_leaves(&list_of(struct_of(vec![
                utf8("a"),
                struct_of(vec![int64("x"), int64("y")]).into_field("s")
            ]))),
            3
        );
        let map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                struct_of(vec![utf8("key"), int64("value")]),
                false,
            )),
            false,
        );
        assert_eq!(count_leaves(&map), 2);
        let dict =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        assert_eq!(count_leaves(&dict), 1);
        // Wrapper kinds must be descended through, not counted as one leaf.
        // A dictionary or run-end-encoded *value* that is itself a struct has
        // as many leaves as the struct: counting it as 1 would misalign every
        // later leaf index in the mask.
        assert_eq!(
            count_leaves(&DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(struct_of(vec![utf8("a"), int64("b")]))
            )),
            2
        );
        assert_eq!(
            count_leaves(&DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new(
                    "values",
                    struct_of(vec![utf8("a"), int64("b")]),
                    true
                ))
            )),
            2
        );
    }

    /// [`contains_struct`] gates the projection fast path, so it has to agree
    /// with [`count_leaves`] about which wrappers are descended through.
    #[test]
    fn contains_struct_shapes() {
        assert!(!contains_struct(&DataType::Int32));
        assert!(!contains_struct(&list_of(DataType::Int32)));
        assert!(contains_struct(&struct_of(vec![int64("a")])));
        assert!(contains_struct(&list_of(struct_of(vec![int64("a")]))));
        assert!(contains_struct(&DataType::LargeList(Arc::new(Field::new(
            "item",
            struct_of(vec![int64("a")]),
            true
        )))));
        assert!(contains_struct(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(struct_of(vec![int64("a")]))
        )));
        assert!(!contains_struct(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8)
        )));
        // A map's entries are a struct, so a map always contains one.
        assert!(contains_struct(&DataType::Map(
            Arc::new(Field::new(
                "entries",
                struct_of(vec![utf8("key"), int64("value")]),
                false
            )),
            false
        )));
    }

    /// `{a, b, c} CAST TO {b}` keeps only b's leaf.
    #[test]
    fn clip_struct_subset() {
        let physical = struct_of(vec![utf8("a"), int64("b"), utf8("c")]);
        let target = struct_of(vec![int64("b")]);
        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![1]);
        assert_eq!(emitted, struct_of(vec![int64("b")]));
    }

    /// Target field order does not matter: emitted type is in physical order.
    #[test]
    fn clip_struct_reordered_target() {
        let physical = struct_of(vec![utf8("a"), int64("b"), utf8("c")]);
        let target = struct_of(vec![utf8("c"), utf8("a")]);
        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0, 2]);
        assert_eq!(emitted, struct_of(vec![utf8("a"), utf8("c")]));
    }

    /// Target fields missing from the physical type are ignored (the runtime
    /// cast null-fills them).
    #[test]
    fn clip_struct_target_field_missing_from_physical() {
        let physical = struct_of(vec![utf8("a"), int64("b")]);
        let target = struct_of(vec![utf8("a"), int64("z")]);
        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0]);
        assert_eq!(emitted, struct_of(vec![utf8("a")]));
    }

    /// Leaf-level type mismatch (promotion) still clips: the emitted type
    /// keeps the physical leaf type; the cast performs the promotion.
    #[test]
    fn clip_keeps_physical_leaf_types() {
        let physical =
            struct_of(vec![Field::new("x", DataType::Int32, true), utf8("pad")]);
        let target = struct_of(vec![int64("x")]);
        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0]);
        assert_eq!(
            emitted,
            struct_of(vec![Field::new("x", DataType::Int32, true)])
        );
    }

    /// Nested struct-in-struct clips at both levels.
    #[test]
    fn clip_nested_struct() {
        let inner_physical = struct_of(vec![int64("x"), utf8("pad_inner")]);
        let physical = struct_of(vec![
            inner_physical.clone().into_field("inner"),
            utf8("pad_outer"),
        ]);
        let target = struct_of(vec![struct_of(vec![int64("x")]).into_field("inner")]);
        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0]);
        assert_eq!(
            emitted,
            struct_of(vec![struct_of(vec![int64("x")]).into_field("inner")])
        );
    }

    /// List<Struct>, the headline case.
    #[test]
    fn clip_list_of_struct() {
        let physical = list_of(struct_of(vec![int64("x"), utf8("y"), utf8("pad")]));
        let target = list_of(struct_of(vec![int64("x"), utf8("y")]));
        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0, 1]);
        assert_eq!(emitted, list_of(struct_of(vec![int64("x"), utf8("y")])));
    }

    /// Two levels of `list<struct>` nesting, the inner one also narrowed,
    /// the `events: array<struct<..., items: array<struct<...>>>>` shape
    /// reported in `datafusion-comet#4859`, where a sibling struct field at
    /// the outer level (`aux`, standing in for that report's
    /// `latency_parts`) is dropped entirely rather than clipped.
    #[test]
    fn clip_two_level_nested_list_of_struct() {
        let physical = list_of(struct_of(vec![
            int64("a"),
            utf8("pad"),
            struct_of(vec![int64("x"), utf8("y")]).into_field("aux"),
            list_of(struct_of(vec![int64("g"), utf8("pad2")])).into_field("items"),
        ]));
        let target = list_of(struct_of(vec![
            int64("a"),
            list_of(struct_of(vec![int64("g")])).into_field("items"),
        ]));

        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        // a=0, pad=1, aux.x=2, aux.y=3, items.g=4, items.pad2=5: only a and
        // items.g survive; pad, all of aux, and items.pad2 are dropped.
        assert_eq!(kept, vec![0, 4]);
        assert_eq!(
            emitted,
            list_of(struct_of(vec![
                int64("a"),
                list_of(struct_of(vec![int64("g")])).into_field("items"),
            ]))
        );
    }

    #[test]
    fn clip_large_list_of_struct() {
        let item = |fields| Arc::new(Field::new("item", struct_of(fields), true));
        let physical = DataType::LargeList(item(vec![int64("x"), utf8("pad")]));
        let target = DataType::LargeList(item(vec![int64("x")]));
        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0]);
        assert_eq!(emitted, DataType::LargeList(item(vec![int64("x")])));
    }

    /// Wrapper-kind mismatch cannot be clipped.
    #[test]
    fn no_clip_on_wrapper_mismatch() {
        let physical = list_of(struct_of(vec![int64("x"), utf8("pad")]));
        let target = DataType::LargeList(Arc::new(Field::new(
            "item",
            struct_of(vec![int64("x")]),
            true,
        )));
        assert!(clip_for_cast(&physical, &target).is_none());
    }

    /// Maps are opaque: never clipped.
    #[test]
    fn no_clip_on_map() {
        let entries = |fields| Arc::new(Field::new("entries", struct_of(fields), false));
        let physical =
            DataType::Map(entries(vec![utf8("key"), int64("a"), int64("b")]), false);
        let target = DataType::Map(entries(vec![utf8("key"), int64("a")]), false);
        assert!(clip_for_cast(&physical, &target).is_none());
    }

    /// Identical types: nothing to prune.
    #[test]
    fn no_clip_when_identical() {
        let t = struct_of(vec![utf8("a"), int64("b")]);
        assert!(clip_for_cast(&t, &t).is_none());
    }

    /// Non-nested types: nothing to prune.
    #[test]
    fn no_clip_on_primitives() {
        assert!(clip_for_cast(&DataType::Int32, &DataType::Int64).is_none());
    }

    /// A struct level with zero field-name overlap can't actually reach this
    /// code: `validate_struct_compatibility` rejects it during physical
    /// planning (see the module doc), so `clip_for_cast` is only ever called
    /// with targets that overlap at every nesting level. If it were reached
    /// anyway, the generic catch-all keeps every leaf, still safe, just
    /// unpruned.
    #[test]
    fn no_clip_on_zero_overlap() {
        let physical = struct_of(vec![utf8("a"), int64("b")]);
        let target = struct_of(vec![utf8("z")]);
        assert!(clip_for_cast(&physical, &target).is_none());
    }

    /// A *nested* struct level with zero field-name overlap must not be
    /// clipped, even when a sibling keeps leaves. The reader drops a field
    /// whose leaves are all masked out (pinned by
    /// [`reader_drops_struct_child_with_no_selected_leaves`]), so predicting
    /// `{inner: Struct[], c}` here would be a schema the decoder never
    /// produces. Read the whole column instead.
    #[test]
    fn no_clip_when_nested_struct_level_has_no_overlap() {
        let physical = struct_of(vec![
            struct_of(vec![int64("a"), int64("b")]).into_field("inner"),
            int64("c"),
        ]);
        let target = struct_of(vec![
            struct_of(vec![int64("z")]).into_field("inner"),
            int64("c"),
        ]);
        assert!(clip_for_cast(&physical, &target).is_none());
    }

    /// Same, one level deeper and behind a list wrapper.
    #[test]
    fn no_clip_when_nested_list_struct_level_has_no_overlap() {
        let physical = struct_of(vec![
            list_of(struct_of(vec![int64("a"), int64("b")])).into_field("items"),
            int64("c"),
        ]);
        let target = struct_of(vec![
            list_of(struct_of(vec![int64("z")])).into_field("items"),
            int64("c"),
        ]);
        assert!(clip_for_cast(&physical, &target).is_none());
    }

    /// Wide structs take the name-map matching path rather than the linear
    /// scan; both must produce the same clip.
    #[test]
    fn clip_wide_struct_matches_by_name() {
        let width = LINEAR_FIELD_SCAN_MAX * 4;
        let physical = struct_of((0..width).map(|i| int64(&format!("f{i}"))).collect());
        // Even fields only, declared in reverse order: the emitted type is
        // still in physical order.
        let target = struct_of(
            (0..width)
                .rev()
                .filter(|i| i % 2 == 0)
                .map(|i| int64(&format!("f{i}")))
                .collect(),
        );
        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, (0..width).filter(|i| i % 2 == 0).collect::<Vec<_>>());
        assert_eq!(
            emitted,
            struct_of(
                (0..width)
                    .filter(|i| i % 2 == 0)
                    .map(|i| int64(&format!("f{i}")))
                    .collect()
            )
        );
    }

    /// Duplicate physical field names both match the single target field and
    /// are both kept, which is what the reader emits for that mask.
    #[test]
    fn clip_keeps_duplicate_physical_field_names() {
        let physical = struct_of(vec![int64("a"), utf8("pad"), int64("a")]);
        let target = struct_of(vec![int64("a")]);
        let (kept, emitted) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0, 2]);
        assert_eq!(emitted, struct_of(vec![int64("a"), int64("a")]));
    }

    /// Pins the arrow-rs behavior the empty-level guard above depends on: a
    /// struct child none of whose leaves are selected disappears from the
    /// type the reader emits, rather than surviving as an empty struct.
    #[test]
    fn reader_drops_struct_child_with_no_selected_leaves() {
        use arrow::array::{ArrayRef, Int64Array, StructArray};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::arrow::{ArrowWriter, ProjectionMask};

        let inner_fields = Fields::from(vec![int64("a"), int64("b")]);
        let outer_fields = Fields::from(vec![
            Field::new("inner", DataType::Struct(inner_fields.clone()), true),
            int64("c"),
        ]);
        let inner: ArrayRef = Arc::new(StructArray::new(
            inner_fields,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(Int64Array::from(vec![3, 4])) as ArrayRef,
            ],
            None,
        ));
        let outer = StructArray::new(
            outer_fields.clone(),
            vec![inner, Arc::new(Int64Array::from(vec![5, 6])) as ArrayRef],
            None,
        );
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "s",
            DataType::Struct(outer_fields),
            true,
        )]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(outer)]).unwrap();

        let file = tempfile::NamedTempFile::new().unwrap();
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap()).unwrap();
        assert_eq!(builder.parquet_schema().num_columns(), 3);
        // Keep only s.c (leaf 2): every leaf of s.inner is masked out.
        let mask = ProjectionMask::leaves(builder.parquet_schema(), [2usize]);
        let reader = builder.with_projection(mask).build().unwrap();
        let out: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(
            out[0].schema().field(0).data_type(),
            &struct_of(vec![int64("c")]),
            "the fully masked `inner` child is dropped, not emitted as an empty struct"
        );
    }

    /// Pins the arrow-rs behavior this module relies on: selecting a subset
    /// of leaves under a `List<Struct>` column with `ProjectionMask::leaves`
    /// makes the reader emit exactly the type predicted by [`clip_for_cast`],
    /// and null list rows / null struct elements survive (their validity is
    /// reconstructed from the surviving leaves' definition levels).
    #[test]
    fn arrow_reader_emits_clipped_type_for_masked_list_struct() {
        use arrow::array::{
            Array, ArrayRef, Int64Array, ListArray, StringArray, StructArray,
        };
        use arrow::buffer::{NullBuffer, OffsetBuffer};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::arrow::{ArrowWriter, ProjectionMask};

        let item_fields = Fields::from(vec![int64("x"), utf8("y"), utf8("pad")]);
        let item_field = Arc::new(Field::new(
            "item",
            DataType::Struct(item_fields.clone()),
            true,
        ));
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "events",
            DataType::List(Arc::clone(&item_field)),
            true,
        )]));

        // 3 elements; element 1 is a NULL struct. Rows: [e0, e1], NULL, [e2].
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            Arc::new(StringArray::from(vec![Some("p0"), None, Some("p2")])),
        ];
        let struct_validity = NullBuffer::from(vec![true, false, true]);
        let values = StructArray::new(item_fields, columns, Some(struct_validity));
        let list_validity = NullBuffer::from(vec![true, false, true]);
        let events = ListArray::new(
            item_field,
            OffsetBuffer::from_lengths([2, 0, 1]),
            Arc::new(values),
            Some(list_validity),
        );
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(events)]).unwrap();

        let file = tempfile::NamedTempFile::new().unwrap();
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Clip to the narrow target {x, y}.
        let physical = batch.schema().field(0).data_type().clone();
        let target = list_of(struct_of(vec![int64("x"), utf8("y")]));
        let (kept, predicted_type) = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0, 1]);

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap()).unwrap();
        let mask = ProjectionMask::leaves(builder.parquet_schema(), kept.iter().copied());
        let reader = builder.with_projection(mask).build().unwrap();
        let out: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(out.len(), 1);
        let out = &out[0];

        // Emitted type matches the prediction.
        assert_eq!(out.schema().field(0).data_type(), &predicted_type);

        // Null semantics survive the clip.
        let events = out.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        assert!(events.is_valid(0));
        assert!(events.is_null(1));
        assert!(events.is_valid(2));
        let structs = events
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(structs.len(), 3);
        assert!(structs.is_valid(0));
        assert!(structs.is_null(1));
        assert!(structs.is_valid(2));
        let x = structs
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(x.value(0), 1);
        assert_eq!(x.value(2), 3);
    }

    trait IntoField {
        fn into_field(self, name: &str) -> Field;
    }

    impl IntoField for DataType {
        fn into_field(self, name: &str) -> Field {
            Field::new(name, self, true)
        }
    }
}
