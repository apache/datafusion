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
//! *narrower* nested type — e.g. the file contains
//! `events: List<Struct<x, y, z, ...>>` but the expression is
//! `CAST(events AS List<Struct<x, y>>)` — the parquet reader does not need to
//! fetch or decode the leaves the cast target never names. This module
//! computes which parquet leaves survive such a cast by walking the physical
//! and target type trees in parallel, matching struct fields by name (the
//! equivalent of Spark's `ParquetReadSupport.clipParquetSchema`).
//!
//! This situation arises whenever a table's logical schema declares a nested
//! column narrower than the physical parquet file: the physical expression
//! adapter rewrites the projected column into exactly such a whole-column
//! cast (see `datafusion_physical_expr_adapter`). Engines like Spark
//! communicate nested projection pruning to the scan this way — as a clipped
//! read *schema* rather than as `get_field` expressions.
//!
//! # Safety of clipping
//!
//! The runtime cast for nested types
//! ([`datafusion_common::nested_struct::cast_column`]) consumes source struct
//! children exclusively by looking up the *target* field names, recursively
//! through list wrappers. Physical subtrees not named by the target are
//! provably dead: removing them from the read cannot change the cast's
//! output. Struct-level nullability is preserved because the parquet reader
//! reconstructs ancestor validity from the definition levels of any surviving
//! leaf, and the clip always keeps at least one leaf per struct level.
//!
//! The clip is *total*: any type shape it does not understand (maps,
//! dictionaries, wrapper-kind mismatches, ...) keeps all of its leaves, so
//! the worst case is today's behavior of reading the full column. Map values
//! are deliberately not clipped: the runtime cast routes maps through Arrow's
//! positional struct cast, which requires all children to be present.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};

/// Clip `physical` against `cast_target`, returning which parquet leaves the
/// cast actually consumes, as offsets relative to the root column's first
/// leaf (sorted ascending and non-empty). The Arrow type the reader will
/// emit for these offsets is derived with [`prune_type_by_kept_offsets`].
///
/// Returns `None` when nothing can be pruned (every leaf is consumed, or the
/// shapes do not allow safe clipping), in which case the caller should read
/// the whole column as before. This function never fails: unknown shapes
/// degrade to keeping all leaves.
pub(crate) fn clip_for_cast(
    physical: &DataType,
    cast_target: &DataType,
) -> Option<Vec<usize>> {
    let total = count_leaves(physical);
    let mut kept = Vec::new();
    let mut next_leaf = 0;
    clip_type(physical, cast_target, &mut next_leaf, &mut kept);
    debug_assert_eq!(next_leaf, total, "leaf accounting must cover the type");
    if kept.is_empty() || kept.len() >= total {
        return None;
    }
    Some(kept)
}

/// Number of parquet leaf columns a (parquet-derived) Arrow type occupies.
pub(crate) fn count_leaves(dt: &DataType) -> usize {
    match dt {
        DataType::Struct(fields) => {
            fields.iter().map(|f| count_leaves(f.data_type())).sum()
        }
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => count_leaves(f.data_type()),
        DataType::Dictionary(_, value) => count_leaves(value),
        DataType::RunEndEncoded(_, value) => count_leaves(value.data_type()),
        _ => 1,
    }
}

/// Recursive walker: advances `next_leaf` across every leaf of `physical`,
/// pushing the offsets the cast target consumes into `kept`.
fn clip_type(
    physical: &DataType,
    target: &DataType,
    next_leaf: &mut usize,
    kept: &mut Vec<usize>,
) {
    match (physical, target) {
        (DataType::Struct(p_children), DataType::Struct(t_children)) => {
            let matches: Vec<Option<&FieldRef>> = p_children
                .iter()
                .map(|pc| t_children.iter().find(|tc| tc.name() == pc.name()))
                .collect();

            if matches.iter().all(Option::is_none) {
                // No field name overlap at this level. The runtime cast will
                // fail (or a custom cast may do something unusual), so keep
                // the first non-empty child wholesale to preserve at least
                // one leaf and the struct's definition levels; behavior is
                // then identical to an unclipped read.
                return keep_first_child(p_children, next_leaf, kept);
            }

            for (pc, tc) in p_children.iter().zip(matches) {
                match tc {
                    Some(tc) => {
                        clip_type(pc.data_type(), tc.data_type(), next_leaf, kept)
                    }
                    None => skip_leaves(pc.data_type(), next_leaf),
                }
            }
        }
        (DataType::List(p_item), DataType::List(t_item))
        | (DataType::LargeList(p_item), DataType::LargeList(t_item)) => {
            clip_type(p_item.data_type(), t_item.data_type(), next_leaf, kept)
        }
        // Anything else — leaf pairs, wrapper-kind mismatches, maps,
        // dictionaries, fixed-size lists, views — is kept wholesale.
        _ => keep_all_leaves(physical, next_leaf, kept),
    }
}

/// Fallback for a struct level with zero name overlap: keep the first child
/// that owns at least one leaf, skip the rest.
fn keep_first_child(
    children: &arrow::datatypes::Fields,
    next_leaf: &mut usize,
    kept: &mut Vec<usize>,
) {
    let mut kept_one = false;
    for child in children {
        if !kept_one && count_leaves(child.data_type()) > 0 {
            keep_all_leaves(child.data_type(), next_leaf, kept);
            kept_one = true;
        } else {
            skip_leaves(child.data_type(), next_leaf);
        }
    }
}

fn keep_all_leaves(dt: &DataType, next_leaf: &mut usize, kept: &mut Vec<usize>) {
    let n = count_leaves(dt);
    kept.extend(*next_leaf..*next_leaf + n);
    *next_leaf += n;
}

fn skip_leaves(dt: &DataType, next_leaf: &mut usize) {
    *next_leaf += count_leaves(dt);
}

/// Derive the Arrow type the reader emits for `physical` when only the leaf
/// offsets in `kept` (relative to this type's first leaf) are selected.
///
/// Used when a root column's kept set is the *union* of several accesses
/// (e.g. a narrowing cast plus `get_field` accesses), where no single cast
/// target describes the result. Subtrees that own no kept leaf are removed;
/// struct levels that lose all children are removed entirely (`None`).
pub(crate) fn prune_type_by_kept_offsets(
    physical: &DataType,
    kept: &BTreeSet<usize>,
) -> Option<DataType> {
    let mut next_leaf = 0;
    prune_type_walker(physical, kept, &mut next_leaf)
}

fn prune_type_walker(
    physical: &DataType,
    kept: &BTreeSet<usize>,
    next_leaf: &mut usize,
) -> Option<DataType> {
    match physical {
        DataType::Struct(children) => {
            let kept_children: Vec<FieldRef> = children
                .iter()
                .filter_map(|c| {
                    prune_type_walker(c.data_type(), kept, next_leaf)
                        .map(|t| Arc::new(c.as_ref().clone().with_data_type(t)) as _)
                })
                .collect();
            (!kept_children.is_empty()).then(|| DataType::Struct(kept_children.into()))
        }
        DataType::List(item) => prune_type_walker(item.data_type(), kept, next_leaf)
            .map(|t| DataType::List(Arc::new(item.as_ref().clone().with_data_type(t)))),
        DataType::LargeList(item) => prune_type_walker(item.data_type(), kept, next_leaf)
            .map(|t| {
                DataType::LargeList(Arc::new(item.as_ref().clone().with_data_type(t)))
            }),
        other => {
            // Opaque subtree: all-or-nothing. Kept sets are always aligned to
            // whole subtrees below struct levels, so partial overlap cannot
            // occur; any kept leaf implies the whole subtree was kept.
            let n = count_leaves(other);
            let start = *next_leaf;
            *next_leaf += n;
            kept.range(start..start + n)
                .next()
                .is_some()
                .then(|| other.clone())
        }
    }
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
    use arrow::datatypes::Fields;

    /// Emitted type for kept offsets, via the same derivation production
    /// uses.
    fn emitted(physical: &DataType, kept: &[usize]) -> DataType {
        let set: BTreeSet<usize> = kept.iter().copied().collect();
        prune_type_by_kept_offsets(physical, &set).unwrap()
    }

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
    }

    /// `{a, b, c} CAST TO {b}` keeps only b's leaf.
    #[test]
    fn clip_struct_subset() {
        let physical = struct_of(vec![utf8("a"), int64("b"), utf8("c")]);
        let target = struct_of(vec![int64("b")]);
        let kept = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![1]);
        assert_eq!(emitted(&physical, &kept), struct_of(vec![int64("b")]));
    }

    /// Target field order does not matter: emitted type is in physical order.
    #[test]
    fn clip_struct_reordered_target() {
        let physical = struct_of(vec![utf8("a"), int64("b"), utf8("c")]);
        let target = struct_of(vec![utf8("c"), utf8("a")]);
        let kept = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0, 2]);
        assert_eq!(
            emitted(&physical, &kept),
            struct_of(vec![utf8("a"), utf8("c")])
        );
    }

    /// Target fields missing from the physical type are ignored (the runtime
    /// cast null-fills them).
    #[test]
    fn clip_struct_target_field_missing_from_physical() {
        let physical = struct_of(vec![utf8("a"), int64("b")]);
        let target = struct_of(vec![utf8("a"), int64("z")]);
        let kept = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0]);
        assert_eq!(emitted(&physical, &kept), struct_of(vec![utf8("a")]));
    }

    /// Leaf-level type mismatch (promotion) still clips: the emitted type
    /// keeps the physical leaf type; the cast performs the promotion.
    #[test]
    fn clip_keeps_physical_leaf_types() {
        let physical =
            struct_of(vec![Field::new("x", DataType::Int32, true), utf8("pad")]);
        let target = struct_of(vec![int64("x")]);
        let kept = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0]);
        assert_eq!(
            emitted(&physical, &kept),
            struct_of(vec![Field::new("x", DataType::Int32, true)])
        );
    }

    /// Zero name overlap keeps the first physical child (>=1 leaf invariant).
    #[test]
    fn clip_struct_no_overlap_keeps_first_child() {
        let physical = struct_of(vec![utf8("a"), int64("b")]);
        let target = struct_of(vec![utf8("z")]);
        let kept = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0]);
        assert_eq!(emitted(&physical, &kept), struct_of(vec![utf8("a")]));
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
        let kept = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0]);
        assert_eq!(
            emitted(&physical, &kept),
            struct_of(vec![struct_of(vec![int64("x")]).into_field("inner")])
        );
    }

    /// List<Struct> — the headline case.
    #[test]
    fn clip_list_of_struct() {
        let physical = list_of(struct_of(vec![int64("x"), utf8("y"), utf8("pad")]));
        let target = list_of(struct_of(vec![int64("x"), utf8("y")]));
        let kept = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0, 1]);
        assert_eq!(
            emitted(&physical, &kept),
            list_of(struct_of(vec![int64("x"), utf8("y")]))
        );
    }

    #[test]
    fn clip_large_list_of_struct() {
        let item = |fields| Arc::new(Field::new("item", struct_of(fields), true));
        let physical = DataType::LargeList(item(vec![int64("x"), utf8("pad")]));
        let target = DataType::LargeList(item(vec![int64("x")]));
        let kept = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0]);
        assert_eq!(
            emitted(&physical, &kept),
            DataType::LargeList(item(vec![int64("x")]))
        );
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

    #[test]
    fn prune_type_by_offsets_union() {
        // struct{a, inner{x, y}, c} with kept leaves {0 (a), 2 (inner.y)}
        let physical = struct_of(vec![
            utf8("a"),
            struct_of(vec![int64("x"), int64("y")]).into_field("inner"),
            utf8("c"),
        ]);
        let kept: BTreeSet<usize> = [0, 2].into_iter().collect();
        let pruned = prune_type_by_kept_offsets(&physical, &kept).unwrap();
        assert_eq!(
            pruned,
            struct_of(vec![
                utf8("a"),
                struct_of(vec![int64("y")]).into_field("inner"),
            ])
        );
    }

    #[test]
    fn prune_type_by_offsets_through_list() {
        let physical = list_of(struct_of(vec![int64("x"), utf8("y"), utf8("pad")]));
        let kept: BTreeSet<usize> = [1].into_iter().collect();
        let pruned = prune_type_by_kept_offsets(&physical, &kept).unwrap();
        assert_eq!(pruned, list_of(struct_of(vec![utf8("y")])));
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
        let kept = clip_for_cast(&physical, &target).unwrap();
        assert_eq!(kept, vec![0, 1]);

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap()).unwrap();
        let mask = ProjectionMask::leaves(builder.parquet_schema(), kept.iter().copied());
        let reader = builder.with_projection(mask).build().unwrap();
        let out: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(out.len(), 1);
        let out = &out[0];

        // Emitted type matches the prediction.
        assert_eq!(
            out.schema().field(0).data_type(),
            &emitted(&physical, &kept)
        );

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
