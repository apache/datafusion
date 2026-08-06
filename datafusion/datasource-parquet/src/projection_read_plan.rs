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
use datafusion_functions::core::input_file_name::InputFileNameFunc;
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use datafusion_common::Result;
use datafusion_common::nested_struct::requires_nested_struct_cast;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_functions::core::file_row_index::FileRowIndexFunc;
use datafusion_functions::core::getfield::GetFieldFunc;
use datafusion_physical_expr::expressions::{CastExpr, Column, Literal};
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};

use crate::nested_schema_pruning::{
    CastColumnAccess, clip_for_cast, contains_struct, count_leaves, field_with_type,
    type_for_leaf_subset,
};

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

/// Trie of nested struct accesses, keyed at the top by the root column index in
/// the file schema and then by field names down each access path.
///
/// # Example
///
/// For a filter expression
///
/// ```sql
/// WHERE s['outer']['a'] > 10
///   AND s['outer']['b'] < 20
///   AND s['outer']['inner']['c'] IS NOT NULL
/// ```
///
/// where `s` is column index `2` in the file schema, three accesses are
/// recorded — all with `root_index = 2` and paths `["outer","a"]`,
/// `["outer","b"]`, `["outer","inner","c"]`. They produce a trie in which
/// the shared `"outer"` prefix is represented by a single intermediate node:
///
/// ```text
/// roots:
///   2 ──► node { selected_here: false }
///         children:
///           "outer" ──► node { selected_here: false }
///                       children:
///                         "a"     ──► { selected_here: true,  children: {} }
///                         "b"     ──► { selected_here: true,  children: {} }
///                         "inner" ──► { selected_here: false,
///                                       children: {
///                                         "c" ──► { selected_here: true,
///                                                   children: {} }
///                                       } }
/// ```
#[derive(Debug, Default)]
struct StructAccessTree<'a> {
    roots: BTreeMap<usize, StructAccessNode<'a>>,
}

/// One node in a [`StructAccessTree`].
///
/// `selected_here` is `true` when at least one access path terminates at this
/// node. Duplicate paths are idempotent.
#[derive(Debug, Default)]
struct StructAccessNode<'a> {
    children: BTreeMap<&'a str, StructAccessNode<'a>>,
    selected_here: bool,
}

impl<'a> StructAccessTree<'a> {
    /// Builds a [`StructAccessTree`] from a flat list of accesses.
    ///
    /// For each [`StructFieldAccess`], walks from the given root index down
    /// the field path, creating intermediate nodes as needed, and sets the
    /// terminal node's `selected_here` to `true`. Paths sharing a prefix
    /// collapse onto common intermediate nodes.
    fn from_accesses(accesses: &'a [StructFieldAccess]) -> Self {
        let mut tree = Self::default();
        for StructFieldAccess {
            root_index,
            field_path,
        } in accesses
        {
            let mut node = tree.roots.entry(*root_index).or_default();
            for component in field_path {
                node = node.children.entry(component.as_str()).or_default();
            }
            node.selected_here = true;
        }
        tree
    }

    /// Returns the node for the given file-schema column index, or `None` if
    /// no access path was recorded under that root.
    fn root(&self, idx: usize) -> Option<&StructAccessNode<'a>> {
        self.roots.get(&idx)
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
    /// Whole-column casts to a narrower nested type
    /// (`CAST(col AS narrower_struct)`). Only collected when
    /// [`Self::with_cast_collection`] enables it (projection analysis);
    /// filter pushdown leaves this off.
    cast_accesses: Vec<CastColumnAccess>,
    /// Whether to collect [`Self::cast_accesses`].
    collect_cast_accesses: bool,
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
            cast_accesses: Vec::new(),
            collect_cast_accesses: false,
            allow_list_columns,
            file_schema,
        }
    }

    /// Enable collection of whole-column casts to narrower nested types.
    pub(crate) fn with_cast_collection(mut self) -> Self {
        self.collect_cast_accesses = true;
        self
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
            cast_accesses: self.cast_accesses,
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
                    // if any field name argument is not a string literal we cannot
                    // determine the exact leaf path, so we fall back to reading the
                    // entire struct root column
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

        // Handle whole-column casts to a narrower nested type, e.g.
        // `CAST(events AS List<Struct<subset of fields>>)` as inserted by the
        // physical expression adapter when the logical file schema declares a
        // nested column narrower than the physical file. Recording the cast
        // target lets the projection read only the leaves the cast consumes
        // (see `crate::nested_schema_pruning`).
        if self.collect_cast_accesses
            && let Some(cast) = node.downcast_ref::<CastExpr>()
            && let Some(column) = cast.expr().downcast_ref::<Column>()
            && let Ok(idx) = self.file_schema.index_of(column.name())
            && requires_nested_struct_cast(
                self.file_schema.field(idx).data_type(),
                cast.cast_type(),
            )
        {
            self.cast_accesses.push(CastColumnAccess {
                root_index: idx,
                target_type: cast.cast_type().clone(),
            });
            return Ok(TreeNodeRecursion::Jump);
        }

        if let Some(column) = node.downcast_ref::<Column>()
            && let Some(recursion) = self.check_single_column(column.name())
        {
            return Ok(recursion);
        }

        if ScalarFunctionExpr::try_downcast_func::<InputFileNameFunc>(node.as_ref())
            .is_some()
            || ScalarFunctionExpr::try_downcast_func::<FileRowIndexFunc>(node.as_ref())
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
    /// Whole-column casts to a narrower nested type. Empty unless cast
    /// collection was enabled on the checker.
    pub(crate) cast_accesses: Vec<CastColumnAccess>,
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

        return root_level_plan(&root_indices, file_schema, schema_descr);
    }

    // secondary fast path: if none of the *projected* columns contains a
    // struct at any nesting level, there are no leaves to prune and we can
    // skip the PushdownChecker traversal and use root-level projection.
    //
    // Gating on the projected roots rather than on every field of the file
    // schema keeps this step O(projected columns): a wide file with a nested
    // column the projection never touches should not push the whole
    // projection through the slower, name-resolving path. Any column whose
    // `index` does not line up with the file schema (a stale `Column` from an
    // earlier rewrite) falls through to that path, which resolves by name.
    let projected_columns = exprs.iter().flat_map(collect_columns).collect::<Vec<_>>();
    let all_resolvable_and_struct_free = projected_columns.iter().all(|col| {
        file_schema
            .fields()
            .get(col.index())
            .is_some_and(|f| f.name() == col.name() && !contains_struct(f.data_type()))
    });

    if all_resolvable_and_struct_free {
        let mut root_indices = projected_columns
            .iter()
            .map(|c| c.index())
            .collect::<Vec<_>>();
        root_indices.sort_unstable();
        root_indices.dedup();

        return root_level_plan(&root_indices, file_schema, schema_descr);
    }

    let mut all_root_indices = Vec::new();
    let mut all_struct_accesses = Vec::new();
    let mut all_cast_accesses = Vec::new();

    for expr in exprs {
        let mut checker = PushdownChecker::new(file_schema, true).with_cast_collection();
        let _ = expr.visit(&mut checker);
        let columns = checker.into_sorted_columns();

        all_root_indices.extend_from_slice(&columns.required_columns);
        all_struct_accesses.extend(columns.struct_field_accesses);
        all_cast_accesses.extend(columns.cast_accesses);
    }

    all_root_indices.sort_unstable();
    all_root_indices.dedup();

    // A whole-column reference reads every leaf of the root, so a cast
    // access on the same root would be overridden anyway: drop those up
    // front. `all_root_indices` is already sorted, so a binary search
    // avoids building a second set just for this filter.
    all_cast_accesses.retain(|c| all_root_indices.binary_search(&c.root_index).is_err());

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
        return root_level_plan(&all_root_indices, file_schema, schema_descr);
    }

    let (read_plan, _leaf_indices) = assemble_read_plan(
        &all_root_indices,
        &all_struct_accesses,
        file_schema,
        schema_descr,
    );

    read_plan
}

/// Builds a [`ParquetReadPlan`] when at least one projected root column is
/// consumed through a cast to a narrower nested type.
///
/// Per root, in ascending root-index order:
/// - roots referenced as whole columns keep every leaf and their full
///   physical field (whole-column reads take precedence; cast accesses on
///   such roots were already dropped by the caller);
/// - roots consumed through one or more casts keep the union of the leaves
///   their targets name (see `crate::nested_schema_pruning`);
/// - a root consumed through both casts and `get_field` accesses keeps the
///   union of both access kinds;
/// - roots consumed only through `get_field` accesses keep the union of the
///   leaves those accesses reach, as before;
/// - any other referenced root, a cast that can't be safely clipped (see
///   `nested_schema_pruning::clip_for_cast`), or a merged leaf set whose
///   emitted Arrow type can't be derived safely, falls back to a full read of
///   that root.
fn build_read_plan_with_cast_clipping(
    file_schema: &Schema,
    schema_descr: &SchemaDescriptor,
    whole_root_indices: &[usize],
    struct_accesses: &[StructFieldAccess],
    cast_accesses: &[CastColumnAccess],
) -> ParquetReadPlan {
    let whole_roots: BTreeSet<usize> = whole_root_indices.iter().copied().collect();
    // Every referenced root's Parquet leaves, grouped in one pass over the
    // schema descriptor rather than one `leaf_indices_for_roots` scan per
    // root (this function may look up several roots).
    let leaves_by_root = leaves_grouped_by_root(schema_descr);

    // Root -> relative leaf offsets required by every narrowing cast on that
    // root. A set makes repeated and overlapping targets a natural union.
    let mut kept_offsets_by_root: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new();
    // Roots with a cast access that must fall back to a full read.
    let mut fallback_roots: BTreeSet<usize> = BTreeSet::new();

    for access in cast_accesses {
        let root = access.root_index;
        if whole_roots.contains(&root) || fallback_roots.contains(&root) {
            continue;
        }

        let physical_type = file_schema.field(root).data_type();
        let root_leaves = leaves_by_root.get(&root).map_or(&[][..], Vec::as_slice);

        // Defensive: the arrow type's leaf count must agree with the
        // Parquet schema (it can diverge if the file embeds a different
        // arrow schema). If not, never risk a wrong mask: read the whole
        // root.
        if root_leaves.len() != count_leaves(physical_type) {
            fallback_roots.insert(root);
            continue;
        }

        match clip_for_cast(physical_type, &access.target_type) {
            Some((kept_offsets, _pruned_type)) => {
                kept_offsets_by_root
                    .entry(root)
                    .or_default()
                    .extend(kept_offsets);
            }
            // Nothing prunable for this cast: every leaf is consumed.
            None => {
                kept_offsets_by_root.remove(&root);
                fallback_roots.insert(root);
            }
        }
    }

    // Add leaves reached through `get_field` to cast roots. The resolver
    // returns absolute Parquet leaf indices; convert them back to offsets in
    // their root so they can share the same union as cast clipping.
    for leaf in resolve_struct_field_leaves(struct_accesses, file_schema, schema_descr) {
        let root = schema_descr.get_column_root_idx(leaf);
        if !kept_offsets_by_root.contains_key(&root) {
            continue;
        }
        let Some(offset) = leaves_by_root
            .get(&root)
            .and_then(|root_leaves| root_leaves.binary_search(&leaf).ok())
        else {
            kept_offsets_by_root.remove(&root);
            fallback_roots.insert(root);
            continue;
        };
        kept_offsets_by_root
            .get_mut(&root)
            .expect("root presence checked above")
            .insert(offset);
    }

    // Derive the reader's one emitted Arrow type from each merged leaf set.
    // Any unsupported partial wrapper retains the total fallback guarantee.
    let mut clipped_by_root: BTreeMap<usize, (Vec<usize>, DataType)> = BTreeMap::new();
    for (root, kept_offsets) in kept_offsets_by_root {
        if fallback_roots.contains(&root) {
            continue;
        }
        let physical_type = file_schema.field(root).data_type();
        let root_leaves = leaves_by_root.get(&root).map_or(&[][..], Vec::as_slice);
        let kept_offsets = kept_offsets.into_iter().collect::<Vec<_>>();
        let Some(pruned_type) = type_for_leaf_subset(physical_type, &kept_offsets) else {
            fallback_roots.insert(root);
            continue;
        };
        let absolute = kept_offsets
            .into_iter()
            .map(|offset| root_leaves[offset])
            .collect();
        clipped_by_root.insert(root, (absolute, pruned_type));
    }

    // `get_field` accesses on roots not already read in full (as a whole
    // column, or as a cast that fell back) keep the existing (non-cast) leaf
    // resolution.
    let get_field_accesses: Vec<StructFieldAccess> = struct_accesses
        .iter()
        .filter(|a| {
            !whole_roots.contains(&a.root_index)
                && !fallback_roots.contains(&a.root_index)
                && !clipped_by_root.contains_key(&a.root_index)
        })
        .cloned()
        .collect();

    let mut leaf_indices: Vec<usize> = Vec::new();
    let mut fields: BTreeMap<usize, Arc<Field>> = BTreeMap::new();

    for root in whole_roots.iter().chain(fallback_roots.iter()) {
        // A root with no parquet leaves contributes nothing to the mask;
        // `ProjectionMask::roots` handles that case the same way, so match it
        // rather than indexing and panicking.
        if let Some(leaves) = leaves_by_root.get(root) {
            leaf_indices.extend(leaves.iter().copied());
        }
        fields.insert(*root, Arc::new(file_schema.field(*root).clone()));
    }

    for (&root, (kept, pruned_type)) in &clipped_by_root {
        leaf_indices.extend(kept.iter().copied());
        fields.insert(
            root,
            field_with_type(file_schema.field(root), pruned_type.clone()),
        );
    }

    if !get_field_accesses.is_empty() {
        let get_field_tree = StructAccessTree::from_accesses(&get_field_accesses);
        leaf_indices.extend(resolve_struct_field_leaves(&get_field_tree, schema_descr));
        let get_field_schema = build_filter_schema(file_schema, &[], &get_field_tree);
        let get_field_roots: BTreeSet<usize> =
            get_field_accesses.iter().map(|a| a.root_index).collect();
        // `build_filter_schema` emits one field per accessed root in
        // ascending root order, which is the order `get_field_roots` iterates
        // in, so the two line up positionally. Pairing them beats looking each
        // one up by name: no repeated linear scans, and no ambiguity if two
        // roots happen to share a name.
        debug_assert_eq!(get_field_roots.len(), get_field_schema.fields().len());
        for (root, field) in get_field_roots.iter().zip(get_field_schema.fields()) {
            fields.insert(*root, Arc::clone(field));
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
            fields.into_values().collect::<Vec<_>>(),
            file_schema.metadata().clone(),
        )),
    }
}

/// Groups every Parquet leaf index by its root (Arrow) column index, in one
/// pass over the schema descriptor.
fn leaves_grouped_by_root(
    schema_descr: &SchemaDescriptor,
) -> BTreeMap<usize, Vec<usize>> {
    let mut by_root: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for leaf_idx in 0..schema_descr.num_columns() {
        by_root
            .entry(schema_descr.get_column_root_idx(leaf_idx))
            .or_default()
            .push(leaf_idx);
    }
    by_root
}

/// Builds a leaf-level [`ParquetReadPlan`] covering `root_indices` in full plus
/// the individual leaves reached by `struct_field_accesses`.
///
/// `root_indices` must be sorted, deduplicated indices into `file_schema`.
///
/// Also returns the resolved Parquet leaf indices, sorted and deduplicated, so
/// callers can size the columns the decoder will read.
pub(crate) fn assemble_read_plan(
    root_indices: &[usize],
    struct_field_accesses: &[StructFieldAccess],
    file_schema: &Schema,
    schema_descr: &SchemaDescriptor,
) -> (ParquetReadPlan, Vec<usize>) {
    let access_tree = StructAccessTree::from_accesses(struct_field_accesses);

    let mut leaf_indices =
        leaf_indices_for_roots(root_indices.iter().copied(), schema_descr);
    leaf_indices
        .extend_from_slice(&resolve_struct_field_leaves(&access_tree, schema_descr));
    leaf_indices.sort_unstable();
    leaf_indices.dedup();

    let projection_mask =
        ProjectionMask::leaves(schema_descr, leaf_indices.iter().copied());
    let projected_schema = build_filter_schema(file_schema, root_indices, &access_tree);

    (
        ParquetReadPlan {
            projection_mask,
            projected_schema,
        },
        leaf_indices,
    )
}

/// Builds a [`ParquetReadPlan`] that decodes whole root columns.
///
/// `root_indices` must be sorted, deduplicated indices into `file_schema`. Every
/// leaf below each root is decoded, and the projected schema keeps each root
/// field's full type. Callers that need to decode only some leaves of a struct
/// root must build the plan from leaf indices instead.
fn root_level_plan(
    root_indices: &[usize],
    file_schema: &Schema,
    schema_descr: &SchemaDescriptor,
) -> ParquetReadPlan {
    let projection_mask =
        ProjectionMask::roots(schema_descr, root_indices.iter().copied());
    let projected_schema = Arc::new(
        file_schema
            .project(root_indices)
            .expect("valid column indices"),
    );

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
    let root_set: BTreeSet<_> = root_indices.into_iter().collect();

    (0..schema_descr.num_columns())
        .filter(|leaf_idx| {
            root_set.contains(&schema_descr.get_column_root_idx(*leaf_idx))
        })
        .collect()
}

/// Returns the Parquet leaf column indices selected by the access tree.
///
/// # Matching
///
/// Iterates Parquet leaves in ascending order (`0..num_columns()`). For each
/// leaf:
///
/// 1. **Root dispatch.** Look up the leaf's root index — the top-level Arrow
///    column it belongs to — via `SchemaDescriptor::get_column_root_idx`. If
///    that root is absent from the access tree (the filter never touched any
///    field under it), skip the leaf without further work.
///
/// 2. **Path walk.** Otherwise, take the leaf's dotted column path
///    (`col.path().parts()`), drop the first component (the root field name,
///    already used in step 1), and walk the remaining components against the
///    matching trie subtree via [`leaf_under_tree`].
///
/// 3. **Inclusion.** The leaf is added to the result iff the walk reaches a
///    node with `selected_here = true` — either an ancestor along the
///    descent (subsumption: a shallower access subsumes the leaf) or the
///    terminal node reached at the end of the path (exact match).
///
/// # Returns
///
/// `Vec<usize>` of Parquet leaf column indices. The scan visits each leaf
/// exactly once and pushes in iteration order, so the result is in ascending
/// order and free of duplicates by construction — callers do not need to
/// sort or dedup.
fn resolve_struct_field_leaves(
    access_tree: &StructAccessTree<'_>,
    schema_descr: &SchemaDescriptor,
) -> Vec<usize> {
    let mut leaf_indices = Vec::new();

    for leaf_idx in 0..schema_descr.num_columns() {
        let root_idx = schema_descr.get_column_root_idx(leaf_idx);
        let Some(root_node) = access_tree.roots.get(&root_idx) else {
            continue;
        };
        // The first part is the root field name, already used in step 1; walk
        // the rest against the tree.
        let col = schema_descr.column(leaf_idx);
        let Some((_root_name, rest)) = col.path().parts().split_first() else {
            continue;
        };
        if leaf_under_tree(root_node, rest) {
            leaf_indices.push(leaf_idx);
        }
    }

    leaf_indices
}

/// True when the leaf path beneath a root is selected by the access tree.
///
/// A shallower `selected_here` node subsumes deeper accesses: once the walk
/// reaches such a node, every leaf below it is included.
fn leaf_under_tree(mut node: &StructAccessNode<'_>, path: &[String]) -> bool {
    for component in path {
        if node.selected_here {
            return true;
        }
        let Some(child) = node.children.get(component.as_str()) else {
            return false;
        };
        node = child;
    }
    node.selected_here
}

/// Builds the Arrow schema used to evaluate the filter expression.
///
/// The returned schema is a **subset** of `file_schema`, restricted to the
/// columns the filter actually touches and (for struct columns accessed
/// only through nested paths) **pruned** to only the accessed fields.
///
/// # Inputs
///
/// - `file_schema` — the full file schema; provides the source `Field`s
///   (names, types, nullability, metadata).
/// - `regular_indices` — file-schema column indices the filter references
///   as **whole columns** (non-struct columns, or struct roots referenced
///   in their entirety). Must be sorted, deduplicated.
/// - `access_tree` — the trie of nested struct field accesses recorded by
///   [`PushdownChecker`].
///
/// # Behavior
///
/// The set of columns to include is the union of `regular_indices` and
/// `access_tree.roots.keys()`. For each column index in that union, decide
/// how the field appears in the output:
///
/// 1. **Whole-column reference** (`idx` is in `regular_indices`). Keep the
///    field's full type unchanged. This is the **whole-root override**:
///    pruning is only valid when a column is accessed *exclusively* through
///    nested field accesses; if any predicate references the whole column,
///    the projected schema must preserve the full type for that column.
///
/// 2. **Nested-access-only struct root.** Look up the column's node in the
///    access tree and call [`prune_struct_type`] on the field's `DataType`
///    with that node. Wrap the pruned type in a new `Field` carrying the
///    original name and nullability.
///
/// Column order in the output schema follows ascending file-schema index
/// (via the `BTreeSet` union), matching the order the Parquet reader
/// produces when projecting these columns.
///
/// # Returns
///
/// An `Arc<Schema>` whose fields are a subset of `file_schema`'s, with
/// struct types pruned per the access tree. The schema's metadata is
/// inherited from `file_schema`.
fn build_filter_schema(
    file_schema: &Schema,
    regular_indices: &[usize],
    access_tree: &StructAccessTree<'_>,
) -> SchemaRef {
    let regular_set: BTreeSet<usize> = regular_indices.iter().copied().collect();

    let all_indices = regular_indices
        .iter()
        .copied()
        .chain(access_tree.roots.keys().copied())
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

            let Some(node) = access_tree.root(idx) else {
                return Arc::new(field.clone());
            };

            let pruned_data_type = prune_struct_type(field.data_type(), node);
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

/// Returns a copy of `dt` with non-accessed struct children removed.
///
/// # Behavior
///
/// - If `node.selected_here` is `true`, the input type is returned
///   unchanged. An access path terminates at this node, so the whole
///   subtree (every field of `dt`, recursively) is required. This mirrors
///   the subsumption check in [`leaf_under_tree`] so the projection mask
///   and the projected schema agree even if a producer ever records an
///   access whose `field_path` terminates above a struct.
///
/// - Otherwise, if `dt` is not a `DataType::Struct`, it is cloned and
///   returned unchanged. The trie only ever guides struct-level pruning;
///   other types pass through.
///
/// - Otherwise, `dt` is a struct and its fields are iterated in their
///   original order. For each field `f`:
///   1. Look up `f.name()` in `node.children`.
///      - **Absent.** No access goes through this field. Drop it.
///      - **Present, child node's `selected_here` is `true`.** An access
///        path terminates at this field. Keep the entire subtree by
///        cloning `f` unchanged (`Arc::clone` — no new `Field`).
///      - **Present, child node's `selected_here` is `false`.** Some
///        access goes through this field to a deeper terminal. Recurse
///        into `f.data_type()` with the matching child node, then wrap
///        the pruned type in a fresh `Field` with `f`'s name and
///        nullability.
///
/// Field ordering is preserved (consumers must match the order the Parquet
/// reader produces when projecting specific leaves). Iterating Arrow's
/// `Fields` directly — rather than iterating `node.children` — is what
/// preserves that order.
///
/// # Returns
///
/// A new `DataType::Struct` whose fields are a subset of `dt`'s, restricted
/// to the paths represented by `node`. The original `dt` is not modified.
fn prune_struct_type(dt: &DataType, node: &StructAccessNode<'_>) -> DataType {
    if node.selected_here {
        // Subsumption: the entire subtree below this node is required.
        return dt.clone();
    }

    let DataType::Struct(fields) = dt else {
        return dt.clone();
    };

    let pruned_fields = fields
        .iter()
        .filter_map(|f| {
            let child = node.children.get(f.name().as_str())?;

            let out = if child.selected_here {
                // Access path terminates at this field — preserve the whole subtree.
                Arc::clone(f)
            } else {
                // Recurse into nested struct.
                let pruned = prune_struct_type(f.data_type(), child);
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
    use parquet::file::metadata::ParquetMetaData;
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

        // all 3 Parquet leaves should be in the projection mask
        let expected_mask = ProjectionMask::leaves(schema_descr, [0, 1, 2]);
        assert_eq!(read_plan.projection_mask, expected_mask,);
    }

    /// Writes the id/struct fixture and returns the schema and metadata a
    /// reader sees for it, so callers don't each repeat the reopen +
    /// `ParquetRecordBatchReaderBuilder` boilerplate.
    ///
    /// Schema: id (Int32), s (Struct{value: Int32, label: Utf8, pad: Utf8}).
    /// Parquet leaves: id=0, s.value=1, s.label=2, s.pad=3.
    fn write_id_struct_file() -> (SchemaRef, Arc<ParquetMetaData>) {
        let struct_fields: Fields = vec![
            Arc::new(Field::new("value", DataType::Int32, false)),
            Arc::new(Field::new("label", DataType::Utf8, false)),
            Arc::new(Field::new("pad", DataType::Utf8, false)),
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
                        Arc::new(StringArray::from(vec!["p0", "p1", "p2"])) as _,
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

        let builder = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap())
            .expect("reader builder");
        (builder.schema().clone(), builder.metadata().clone())
    }

    /// Writes a two-struct-root fixture so tests can combine a cast on one
    /// root with an access on another.
    ///
    /// Schema: a (Struct{p: Int32, q: Utf8}), b (Struct{m: Int32, n: Utf8}).
    /// Parquet leaves: a.p=0, a.q=1, b.m=2, b.n=3.
    fn write_two_struct_file() -> (SchemaRef, Arc<ParquetMetaData>) {
        let group = |first: &str, second: &str| -> Fields {
            vec![
                Arc::new(Field::new(first, DataType::Int32, false)),
                Arc::new(Field::new(second, DataType::Utf8, false)),
            ]
            .into()
        };
        let (a_fields, b_fields) = (group("p", "q"), group("m", "n"));

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Struct(a_fields.clone()), false),
            Field::new("b", DataType::Struct(b_fields.clone()), false),
        ]));

        let values = |fields: Fields, ints: [i32; 2], strs: [&str; 2]| {
            Arc::new(StructArray::new(
                fields,
                vec![
                    Arc::new(Int32Array::from(ints.to_vec())) as _,
                    Arc::new(StringArray::from(strs.to_vec())) as _,
                ],
                None,
            )) as _
        };
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                values(a_fields, [1, 2], ["a0", "a1"]),
                values(b_fields, [3, 4], ["b0", "b1"]),
            ],
        )
        .unwrap();

        let file = NamedTempFile::new().expect("temp file");
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), Arc::clone(&schema), None)
                .expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let builder = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap())
            .expect("reader builder");
        (builder.schema().clone(), builder.metadata().clone())
    }

    /// Builds `CAST(Column(name, index) AS Struct{fields})`.
    fn cast_to_struct(
        name: &str,
        index: usize,
        fields: Vec<(&str, DataType)>,
    ) -> Arc<dyn PhysicalExpr> {
        let target = DataType::Struct(
            fields
                .into_iter()
                .map(|(n, dt)| Arc::new(Field::new(n, dt, true)))
                .collect::<Vec<_>>()
                .into(),
        );
        Arc::new(CastExpr::new(
            Arc::new(PhysicalColumn::new(name, index)),
            target,
            None,
        ))
    }

    /// Builds `get_field(Column(name, index), field)`.
    fn get_field_of(
        file_schema: &Schema,
        name: &str,
        field: &str,
    ) -> Arc<dyn PhysicalExpr> {
        logical2physical(
            &get_field().call(vec![
                col(name),
                Expr::Literal(ScalarValue::Utf8(Some(field.to_string())), None),
            ]),
            file_schema,
        )
    }

    /// Clipping a cast whose only surviving field is *not* the struct's first
    /// one: the kept offsets are relative to the root's first leaf and must be
    /// rebased onto it. With `s` starting at leaf 1 and `label` at offset 1,
    /// getting the arithmetic wrong reads `id` (leaf 0) instead of `s.label`.
    #[test]
    fn build_projection_read_plan_clips_cast_to_a_non_leading_field() {
        let (file_schema, metadata) = write_id_struct_file();
        let schema_descr = metadata.file_metadata().schema_descr();

        let exprs = vec![cast_to_struct("s", 1, vec![("label", DataType::Utf8)])];
        let read_plan = build_projection_read_plan(exprs, &file_schema, schema_descr);

        assert_eq!(
            read_plan.projection_mask,
            ProjectionMask::leaves(schema_descr, [2])
        );
        let s_field = read_plan.projected_schema.field_with_name("s").unwrap();
        assert_eq!(
            s_field.data_type(),
            &DataType::Struct(
                vec![Arc::new(Field::new("label", DataType::Utf8, false))].into()
            ),
        );
    }

    /// A cast on one root and a `get_field` on a *different* root: each root
    /// keeps only what it needs, and both appear in the projected schema in
    /// root order.
    #[test]
    fn build_projection_read_plan_clips_cast_beside_get_field_on_another_root() {
        let (file_schema, metadata) = write_two_struct_file();
        let schema_descr = metadata.file_metadata().schema_descr();

        let exprs = vec![
            cast_to_struct("a", 0, vec![("p", DataType::Int32)]),
            get_field_of(&file_schema, "b", "n"),
        ];
        let read_plan = build_projection_read_plan(exprs, &file_schema, schema_descr);

        // a.p (leaf 0) from the clip, b.n (leaf 3) from the field access.
        assert_eq!(
            read_plan.projection_mask,
            ProjectionMask::leaves(schema_descr, [0, 3])
        );
        let field_types = read_plan
            .projected_schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            field_types,
            vec![
                (
                    "a".to_string(),
                    DataType::Struct(
                        vec![Arc::new(Field::new("p", DataType::Int32, false))].into()
                    )
                ),
                (
                    "b".to_string(),
                    DataType::Struct(
                        vec![Arc::new(Field::new("n", DataType::Utf8, false))].into()
                    )
                ),
            ]
        );
    }

    /// A repeated third cast does not duplicate leaves or widen the union.
    #[test]
    fn build_projection_read_plan_keeps_union_after_a_third_cast() {
        let (file_schema, metadata) = write_id_struct_file();
        let schema_descr = metadata.file_metadata().schema_descr();

        let exprs = vec![
            cast_to_struct("s", 1, vec![("value", DataType::Int32)]),
            cast_to_struct("s", 1, vec![("label", DataType::Utf8)]),
            cast_to_struct("s", 1, vec![("value", DataType::Int32)]),
        ];
        let read_plan = build_projection_read_plan(exprs, &file_schema, schema_descr);

        assert_eq!(
            read_plan.projection_mask,
            ProjectionMask::leaves(schema_descr, [1, 2])
        );
        let s_field = read_plan.projected_schema.field_with_name("s").unwrap();
        assert_eq!(
            s_field.data_type(),
            &DataType::Struct(
                vec![
                    Arc::new(Field::new("value", DataType::Int32, false)),
                    Arc::new(Field::new("label", DataType::Utf8, false)),
                ]
                .into()
            )
        );
    }

    /// A whole-column reference wins over a `get_field` access on the same
    /// root even when another root is being clipped: `a` keeps every leaf and
    /// its full type, `b` keeps only the cast target's.
    #[test]
    fn build_projection_read_plan_whole_column_beats_get_field_beside_a_clip() {
        let (file_schema, metadata) = write_two_struct_file();
        let schema_descr = metadata.file_metadata().schema_descr();

        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(PhysicalColumn::new("a", 0)),
            get_field_of(&file_schema, "a", "p"),
            cast_to_struct("b", 1, vec![("m", DataType::Int32)]),
        ];
        let read_plan = build_projection_read_plan(exprs, &file_schema, schema_descr);

        // Every leaf of `a` (0, 1) plus b.m (leaf 2).
        assert_eq!(
            read_plan.projection_mask,
            ProjectionMask::leaves(schema_descr, [0, 1, 2])
        );
        let a_field = read_plan.projected_schema.field_with_name("a").unwrap();
        assert_eq!(
            a_field.data_type(),
            file_schema.field(0).data_type(),
            "the whole-column reference must keep `a`'s full type"
        );
    }

    /// Columns are resolved by *name*: a `Column` whose index points at a
    /// different field (a stale index left by an earlier rewrite) must not be
    /// taken at face value by the struct fast-path gate.
    #[test]
    fn build_projection_read_plan_resolves_stale_column_indices_by_name() {
        let (file_schema, metadata) = write_id_struct_file();
        let schema_descr = metadata.file_metadata().schema_descr();

        // `s` is at index 1; this claims index 0, which is `id`.
        let exprs = vec![cast_to_struct("s", 0, vec![("value", DataType::Int32)])];
        let read_plan = build_projection_read_plan(exprs, &file_schema, schema_descr);

        assert_eq!(
            read_plan.projection_mask,
            ProjectionMask::leaves(schema_descr, [1]),
            "the cast must resolve to `s`, not to whatever sits at index 0"
        );
    }

    /// A projection consisting solely of a narrowing cast over a struct root
    /// clips the read to the cast target's leaves.
    #[test]
    fn build_projection_read_plan_clips_cast_over_struct() {
        let (file_schema, metadata) = write_id_struct_file();
        let schema_descr = metadata.file_metadata().schema_descr();

        let narrow = DataType::Struct(
            vec![Arc::new(Field::new("value", DataType::Int32, true))].into(),
        );
        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(PhysicalColumn::new("id", 0)),
            Arc::new(CastExpr::new(
                Arc::new(PhysicalColumn::new("s", 1)),
                narrow.clone(),
                None,
            )),
        ];

        let read_plan = build_projection_read_plan(exprs, &file_schema, schema_descr);

        // Only id's leaf (0) and s.value's leaf (1) should be read: s.label
        // and s.pad are clipped away.
        let expected_mask = ProjectionMask::leaves(schema_descr, [0, 1]);
        assert_eq!(read_plan.projection_mask, expected_mask);

        let s_field = read_plan.projected_schema.field_with_name("s").unwrap();
        assert_eq!(
            s_field.data_type(),
            &DataType::Struct(
                vec![Arc::new(Field::new("value", DataType::Int32, false))].into()
            ),
        );
    }

    /// Two casts on the same root with the *same* target still clip: this is
    /// the shape the expression adapter produces when one column is
    /// referenced several times (`SELECT s, s FROM narrowed`).
    #[test]
    fn build_projection_read_plan_clips_repeated_identical_casts() {
        let (file_schema, metadata) = write_id_struct_file();
        let schema_descr = metadata.file_metadata().schema_descr();

        let narrow = DataType::Struct(
            vec![Arc::new(Field::new("value", DataType::Int32, true))].into(),
        );
        let cast = || -> Arc<dyn PhysicalExpr> {
            Arc::new(CastExpr::new(
                Arc::new(PhysicalColumn::new("s", 1)),
                narrow.clone(),
                None,
            ))
        };

        let read_plan =
            build_projection_read_plan(vec![cast(), cast()], &file_schema, schema_descr);

        assert_eq!(
            read_plan.projection_mask,
            ProjectionMask::leaves(schema_descr, [1])
        );
    }

    /// Two casts on the same root with disjoint targets share the union of
    /// their leaves, while an unreferenced sibling remains pruned.
    #[test]
    fn build_projection_read_plan_unions_disjoint_cast_targets() {
        let (file_schema, metadata) = write_id_struct_file();
        let schema_descr = metadata.file_metadata().schema_descr();

        let narrow = |name: &str, dt: DataType| -> Arc<dyn PhysicalExpr> {
            Arc::new(CastExpr::new(
                Arc::new(PhysicalColumn::new("s", 1)),
                DataType::Struct(vec![Arc::new(Field::new(name, dt, true))].into()),
                None,
            ))
        };
        let exprs = vec![
            narrow("value", DataType::Int32),
            narrow("label", DataType::Utf8),
        ];

        let read_plan = build_projection_read_plan(exprs, &file_schema, schema_descr);

        assert_eq!(
            read_plan.projection_mask,
            ProjectionMask::leaves(schema_descr, [1, 2]),
            "the union must serve both casts without reading `s.pad`"
        );
        let s_field = read_plan.projected_schema.field_with_name("s").unwrap();
        assert_eq!(
            s_field.data_type(),
            &DataType::Struct(
                vec![
                    Arc::new(Field::new("value", DataType::Int32, false)),
                    Arc::new(Field::new("label", DataType::Utf8, false)),
                ]
                .into()
            )
        );
    }

    /// Overlapping cast targets deduplicate their shared leaves.
    #[test]
    fn build_projection_read_plan_unions_overlapping_cast_targets() {
        let (file_schema, metadata) = write_id_struct_file();
        let schema_descr = metadata.file_metadata().schema_descr();

        let exprs = vec![
            cast_to_struct("s", 1, vec![("value", DataType::Int32)]),
            cast_to_struct(
                "s",
                1,
                vec![("value", DataType::Int32), ("label", DataType::Utf8)],
            ),
        ];
        let read_plan = build_projection_read_plan(exprs, &file_schema, schema_descr);

        assert_eq!(
            read_plan.projection_mask,
            ProjectionMask::leaves(schema_descr, [1, 2])
        );
    }

    /// The struct fast-path gate looks at the *projected* columns, not at
    /// every field of the file schema: projecting only `id` produces the same
    /// root-level plan it would for a schema with no struct in it at all.
    #[test]
    fn build_projection_read_plan_ignores_unprojected_struct_columns() {
        let (file_schema, metadata) = write_id_struct_file();
        let schema_descr = metadata.file_metadata().schema_descr();

        // Not a bare column, so the all-plain-columns fast path does not apply.
        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(CastExpr::new(
            Arc::new(PhysicalColumn::new("id", 0)),
            DataType::Int64,
            None,
        ))];

        let read_plan = build_projection_read_plan(exprs, &file_schema, schema_descr);

        assert_eq!(
            read_plan.projection_mask,
            ProjectionMask::roots(schema_descr, [0])
        );
        assert_eq!(read_plan.projected_schema.fields().len(), 1);
    }

    /// A root reached by both a narrowing cast and a disjoint `get_field`
    /// access shares the union of their leaves.
    #[test]
    fn build_projection_read_plan_unions_cast_and_get_field_on_one_root() {
        let (file_schema, metadata) = write_id_struct_file();
        let schema_descr = metadata.file_metadata().schema_descr();

        let narrow = DataType::Struct(
            vec![Arc::new(Field::new("value", DataType::Int32, true))].into(),
        );
        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(CastExpr::new(
                Arc::new(PhysicalColumn::new("s", 1)),
                narrow,
                None,
            )),
            logical2physical(
                &get_field().call(vec![
                    col("s"),
                    Expr::Literal(ScalarValue::Utf8(Some("label".to_string())), None),
                ]),
                &file_schema,
            ),
        ];

        let read_plan = build_projection_read_plan(exprs, &file_schema, schema_descr);

        let expected_mask = ProjectionMask::leaves(schema_descr, [1, 2]);
        assert_eq!(read_plan.projection_mask, expected_mask);

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
    }

    fn access(root: usize, path: &[&str]) -> StructFieldAccess {
        StructFieldAccess {
            root_index: root,
            field_path: path.iter().map(|&s| s.to_string()).collect(),
        }
    }

    #[test]
    fn struct_access_tree_from_empty_input_has_no_roots() {
        let tree = StructAccessTree::from_accesses(&[]);
        assert!(tree.roots.is_empty());
    }

    #[test]
    fn struct_access_tree_groups_paths_by_root() {
        let accesses = [access(0, &["a"]), access(2, &["x"]), access(2, &["y"])];
        let tree = StructAccessTree::from_accesses(&accesses);

        assert_eq!(tree.roots.keys().copied().collect::<Vec<_>>(), vec![0, 2]);
        let root0 = tree.root(0).unwrap();
        assert!(root0.children.contains_key("a"));
        assert!(root0.children["a"].selected_here);

        let root2 = tree.root(2).unwrap();
        assert_eq!(
            root2.children.keys().copied().collect::<Vec<_>>(),
            vec!["x", "y"],
        );
    }

    #[test]
    fn struct_access_tree_shared_prefix_collapses_into_one_node() {
        let accesses = [access(0, &["outer", "a"]), access(0, &["outer", "b"])];
        let tree = StructAccessTree::from_accesses(&accesses);

        let root = tree.root(0).unwrap();
        assert!(!root.selected_here);

        let outer = &root.children["outer"];
        // `outer` itself was never the terminal of an access path.
        assert!(!outer.selected_here);
        // Both leaves below share the single `outer` node.
        assert_eq!(
            outer.children.keys().copied().collect::<Vec<_>>(),
            vec!["a", "b"],
        );
        assert!(outer.children["a"].selected_here);
        assert!(outer.children["b"].selected_here);
    }

    #[test]
    fn struct_access_tree_records_both_shallow_and_deep_selection() {
        // `s['outer']` (whole subtree) and `s['outer']['a']` (specific leaf)
        // both recorded. Consumers honor the shallower selection at walk time;
        // the builder simply records both `selected_here` flags.
        let accesses = [access(0, &["outer"]), access(0, &["outer", "a"])];
        let tree = StructAccessTree::from_accesses(&accesses);

        let outer = &tree.root(0).unwrap().children["outer"];
        assert!(outer.selected_here);
        assert!(outer.children["a"].selected_here);
    }

    /// `prune_struct_type` must honor `selected_here` on the input node
    /// itself, not only on its children — symmetric with `leaf_under_tree`.
    /// Without this guard, a node with `selected_here = true` and no
    /// children produces an empty struct (silent drift from the leaf set).
    #[test]
    fn prune_struct_type_returns_full_type_when_node_is_selected_here() {
        let node = StructAccessNode {
            selected_here: true,
            ..Default::default()
        };

        let s_type = DataType::Struct(
            vec![
                Arc::new(Field::new("outer", DataType::Int32, false)),
                Arc::new(Field::new("other", DataType::Int32, false)),
            ]
            .into(),
        );

        let pruned = prune_struct_type(&s_type, &node);

        assert_eq!(
            pruned, s_type,
            "selected_here on the input node must preserve the full type"
        );
    }

    /// Same guard, but for the case where `selected_here` is set on an
    /// intermediate node that also has children — e.g. both `s['outer']`
    /// and `s['outer']['a']` are recorded. The shallower terminal must
    /// keep the entire `outer` subtree, ignoring the deeper child entry.
    #[test]
    fn prune_struct_type_shallow_selection_subsumes_deeper_children() {
        let accesses = [access(0, &["outer"]), access(0, &["outer", "a"])];
        let tree = StructAccessTree::from_accesses(&accesses);

        let outer_type = DataType::Struct(
            vec![
                Arc::new(Field::new("a", DataType::Int32, false)),
                Arc::new(Field::new("b", DataType::Int32, false)),
            ]
            .into(),
        );

        let outer_node = &tree.root(0).unwrap().children["outer"];
        let pruned = prune_struct_type(&outer_type, outer_node);

        assert_eq!(
            pruned, outer_type,
            "shallow selected_here must preserve the whole subtree, \
             not narrow to the deeper child"
        );
    }

    /// Mixed whole-root and nested access.
    /// Projecting `s` (whole) alongside `get_field(s, 'outer', 'a')` (nested)
    /// must preserve the full `s` struct type AND include all `s` leaves in
    /// the projection mask. The nested access does not narrow the whole-root
    /// reference — `regular_indices` wins over the access tree for that root.
    #[test]
    fn projection_whole_root_plus_nested_access_keeps_full_struct() {
        // Schema: s (Struct{outer: Struct{a, b}})
        // Parquet leaves: s.outer.a=0, s.outer.b=1
        let outer_fields: Fields = vec![
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Field::new("b", DataType::Int32, false)),
        ]
        .into();
        let s_fields: Fields = vec![Arc::new(Field::new(
            "outer",
            DataType::Struct(outer_fields.clone()),
            false,
        ))]
        .into();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(s_fields.clone()),
            false,
        )]));

        let outer_arr = StructArray::new(
            outer_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as _,
                Arc::new(Int32Array::from(vec![3, 4])) as _,
            ],
            None,
        );
        let s_arr =
            StructArray::new(s_fields.clone(), vec![Arc::new(outer_arr) as _], None);
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(s_arr)]).unwrap();

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

        // Column("s") (whole struct) + get_field(s, 'outer', 'a') (nested access).
        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(PhysicalColumn::new("s", 0)),
            logical2physical(
                &get_field().call(vec![
                    col("s"),
                    Expr::Literal(ScalarValue::Utf8(Some("outer".to_string())), None),
                    Expr::Literal(ScalarValue::Utf8(Some("a".to_string())), None),
                ]),
                &file_schema,
            ),
        ];

        let read_plan = build_projection_read_plan(exprs, &file_schema, schema_descr);

        // `s` must keep its full nested type — NOT narrowed to Struct{outer: Struct{a}}.
        let s_field = read_plan.projected_schema.field_with_name("s").unwrap();
        assert_eq!(
            s_field.data_type(),
            &DataType::Struct(s_fields),
            "whole-root reference must preserve the full nested struct type \
             even when a nested access is also recorded"
        );

        // All `s` leaves must be in the projection mask (s.outer.a AND s.outer.b).
        let expected_mask = ProjectionMask::leaves(schema_descr, [0, 1]);
        assert_eq!(
            read_plan.projection_mask, expected_mask,
            "whole-root reference must select every leaf under the root"
        );
    }
}
