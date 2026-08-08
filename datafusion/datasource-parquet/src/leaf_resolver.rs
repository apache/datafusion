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

//! O(1) resolution of an Arrow field name to its Parquet leaf column index.

use arrow::datatypes::Schema;
use datafusion_common::HashMap;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::schema::types::SchemaDescriptor;

/// Resolves an Arrow field name to the index of the matching Parquet leaf
/// column in O(1), after an O(N) build.
///
/// This exists because [`parquet::arrow::parquet_column`] — and therefore
/// [`StatisticsConverter::try_new`], which calls it — is O(N) in the number of
/// columns: it does a linear `Fields::find` over the Arrow schema (twice, in
/// the `try_new` case) plus a linear scan for the first leaf descended from the
/// matched root. Callers that resolve *every* field of a file, as
/// per-file statistics collection does, therefore end up O(N²) per file.
/// Hoisting the two lookups into maps built once per file makes that O(N).
///
/// # Semantics
///
/// Deliberately identical to [`parquet::arrow::parquet_column`], including its
/// tie-breaking rules — see [`Self::leaf_index`]. Any divergence would silently
/// attribute one column's statistics to another, so the equivalence is pinned
/// by a differential test against `parquet_column` itself.
pub(crate) struct LeafResolver<'a> {
    /// The Arrow schema the names are resolved against.
    schema: &'a Schema,
    /// Arrow field name to its root field index. First occurrence wins, to
    /// match `Fields::find`.
    name_to_root: HashMap<&'a str, usize>,
    /// Parquet root field index to the index of its first leaf column, or
    /// `None` for a root with no leaves (an empty group).
    root_to_first_leaf: Vec<Option<usize>>,
}

impl<'a> LeafResolver<'a> {
    /// Build the lookup maps. O(number of Arrow fields + number of Parquet
    /// leaf columns).
    pub(crate) fn new(parquet_schema: &SchemaDescriptor, schema: &'a Schema) -> Self {
        let fields = schema.fields();
        let mut name_to_root = HashMap::with_capacity(fields.len());
        for (idx, field) in fields.iter().enumerate() {
            // `Fields::find` returns the *first* field with a given name, so
            // earlier entries must win over later duplicates.
            name_to_root.entry(field.name().as_str()).or_insert(idx);
        }

        let num_roots = parquet_schema.root_schema().get_fields().len();
        let mut root_to_first_leaf = vec![None; num_roots];
        // Iterate in reverse so that the lowest leaf index for each root is
        // the value left behind — `parquet_column` takes the first match.
        for leaf in (0..parquet_schema.columns().len()).rev() {
            let root = parquet_schema.get_column_root_idx(leaf);
            if root < num_roots {
                root_to_first_leaf[root] = Some(leaf);
            }
        }

        Self {
            schema,
            name_to_root,
            root_to_first_leaf,
        }
    }

    /// The Parquet leaf column index for `name`, or `None` when there isn't
    /// one.
    ///
    /// Returns `None` in exactly the cases [`parquet::arrow::parquet_column`]
    /// does:
    /// * `name` is not in the Arrow schema,
    /// * the matched field is nested (a struct/list/map may span anywhere from
    ///   one to three Parquet levels, which this mapping does not model),
    /// * the matched root has no leaf columns.
    pub(crate) fn leaf_index(&self, name: &str) -> Option<usize> {
        let root = *self.name_to_root.get(name)?;
        if self.schema.field(root).data_type().is_nested() {
            return None;
        }
        self.root_to_first_leaf.get(root).copied().flatten()
    }

    /// Build a [`StatisticsConverter`] for `name` without the O(N) name
    /// lookups [`StatisticsConverter::try_new`] performs.
    ///
    /// Falls back to `try_new` when [`Self::leaf_index`] finds no leaf, both to
    /// reproduce its exact error for an unknown column and because
    /// [`StatisticsConverter::from_column_index`] cannot express "this column
    /// has no Parquet leaf". The fallback is O(N), so nested-heavy schemas keep
    /// the quadratic term; flat schemas — the wide-schema case this targets —
    /// never take it.
    pub(crate) fn converter(
        &self,
        name: &str,
        parquet_schema: &'a SchemaDescriptor,
    ) -> parquet::errors::Result<StatisticsConverter<'a>> {
        match self.leaf_index(name) {
            Some(leaf) => {
                // `leaf_index` returning `Some` guarantees the name is present.
                let root = self.name_to_root[name];
                StatisticsConverter::from_column_index(
                    leaf,
                    self.schema.field(root),
                    parquet_schema,
                )
            }
            None => StatisticsConverter::try_new(name, self.schema, parquet_schema),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Fields};
    use parquet::arrow::{ArrowSchemaConverter, parquet_column};
    use std::sync::Arc;

    /// A schema exercising every branch the resolver has to agree with
    /// `parquet_column` on: plain primitives, a nested struct, a nested list,
    /// and duplicate field names.
    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
            Field::new(
                "s",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int32, true),
                    Field::new("y", DataType::Int32, true),
                ])),
                true,
            ),
            Field::new(
                "l",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
            Field::new("c", DataType::Float64, true),
            // Duplicate of "a": `Fields::find` resolves "a" to index 0, so this
            // field is unreachable by name and the resolver must agree.
            Field::new("a", DataType::Int64, true),
        ])
    }

    /// The resolver must return exactly what `parquet_column` returns, for
    /// every field name in the schema and for names that are absent.
    #[test]
    fn matches_parquet_column() {
        let schema = test_schema();
        let descr = ArrowSchemaConverter::new().convert(&schema).unwrap();
        let resolver = LeafResolver::new(&descr, &schema);

        let mut names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        // Names that are not in the Arrow schema at all.
        names.extend(["missing", "", "A"]);

        for name in names {
            let expected = parquet_column(&descr, &schema, name).map(|(idx, _)| idx);
            assert_eq!(
                resolver.leaf_index(name),
                expected,
                "leaf_index disagreed with parquet_column for {name:?}"
            );
        }
    }

    /// Nested fields and absent names must not resolve to a leaf.
    #[test]
    fn nested_and_missing_have_no_leaf() {
        let schema = test_schema();
        let descr = ArrowSchemaConverter::new().convert(&schema).unwrap();
        let resolver = LeafResolver::new(&descr, &schema);

        assert_eq!(resolver.leaf_index("s"), None, "struct must not resolve");
        assert_eq!(resolver.leaf_index("l"), None, "list must not resolve");
        assert_eq!(resolver.leaf_index("missing"), None);
    }

    /// Primitive fields resolve, and the duplicate name resolves to the first
    /// occurrence rather than the last.
    #[test]
    fn duplicate_name_resolves_to_first_occurrence() {
        let schema = test_schema();
        let descr = ArrowSchemaConverter::new().convert(&schema).unwrap();
        let resolver = LeafResolver::new(&descr, &schema);

        // "a" is roots 0 and 5; root 0's leaf is parquet column 0.
        assert_eq!(resolver.leaf_index("a"), Some(0));
        assert_eq!(resolver.leaf_index("b"), Some(1));
        // "s" contributes two leaves and "l" one, so "c" is the sixth leaf.
        assert_eq!(resolver.leaf_index("c"), Some(5));
    }

    /// A converter built via the resolver must target the same Parquet column
    /// as one built by `try_new`.
    #[test]
    fn converter_matches_try_new() {
        let schema = test_schema();
        let descr = ArrowSchemaConverter::new().convert(&schema).unwrap();
        let resolver = LeafResolver::new(&descr, &schema);

        for name in ["a", "b", "c", "s", "l"] {
            let expected = StatisticsConverter::try_new(name, &schema, &descr).unwrap();
            let actual = resolver.converter(name, &descr).unwrap();
            assert_eq!(
                actual.parquet_column_index(),
                expected.parquet_column_index(),
                "converter targeted a different column for {name:?}"
            );
            assert_eq!(actual.arrow_field(), expected.arrow_field());
        }
    }

    /// An unknown column must produce the same error `try_new` produces.
    #[test]
    fn converter_reports_unknown_column() {
        let schema = test_schema();
        let descr = ArrowSchemaConverter::new().convert(&schema).unwrap();
        let resolver = LeafResolver::new(&descr, &schema);

        let expected = StatisticsConverter::try_new("missing", &schema, &descr)
            .expect_err("try_new should reject an unknown column");
        let actual = resolver
            .converter("missing", &descr)
            .expect_err("resolver should reject an unknown column");
        assert_eq!(actual.to_string(), expected.to_string());
    }

    /// An empty Arrow schema must not panic and must resolve nothing.
    #[test]
    fn empty_schema() {
        let schema = Schema::empty();
        let descr = ArrowSchemaConverter::new().convert(&schema).unwrap();
        let resolver = LeafResolver::new(&descr, &schema);
        assert_eq!(resolver.leaf_index("anything"), None);
    }
}
