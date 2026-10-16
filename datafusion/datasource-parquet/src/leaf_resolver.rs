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

/// Resolves Arrow field names to Parquet leaf column indices in O(1), after
/// an O(N) build.
///
/// [`parquet::arrow::parquet_column`], and [`StatisticsConverter::try_new`]
/// which calls it, is O(N) per lookup, so resolving every field of a file
/// with it is O(N²). This resolver builds the lookup maps once per file
/// instead.
///
/// Results match `parquet_column` exactly, including its tie-breaks (see
/// [`Self::leaf_index`]). A mismatch would attribute one column's statistics
/// to another, so the tests compare the two directly.
pub(crate) struct LeafResolver<'a> {
    parquet_schema: &'a SchemaDescriptor,
    schema: &'a Schema,
    /// Arrow field name to root field index. The first field with a name
    /// wins, as in `Fields::find`.
    name_to_root: HashMap<&'a str, usize>,
    /// Parquet root index to its lowest leaf index, or `None` for a root
    /// with no leaves (an empty group).
    root_to_first_leaf: Vec<Option<usize>>,
}

impl<'a> LeafResolver<'a> {
    pub(crate) fn new(parquet_schema: &'a SchemaDescriptor, schema: &'a Schema) -> Self {
        let fields = schema.fields();
        let mut name_to_root = HashMap::with_capacity(fields.len());
        for (idx, field) in fields.iter().enumerate() {
            name_to_root.entry(field.name().as_str()).or_insert(idx);
        }

        let num_roots = parquet_schema.root_schema().get_fields().len();
        let mut root_to_first_leaf = vec![None; num_roots];
        // Iterate in reverse so the lowest leaf index for each root is kept.
        for leaf in (0..parquet_schema.columns().len()).rev() {
            let root = parquet_schema.get_column_root_idx(leaf);
            if root < num_roots {
                root_to_first_leaf[root] = Some(leaf);
            }
        }

        Self {
            parquet_schema,
            schema,
            name_to_root,
            root_to_first_leaf,
        }
    }

    /// The Parquet leaf column index for `name`.
    ///
    /// Returns `None` in the same cases as [`parquet::arrow::parquet_column`]:
    /// * `name` is not in the Arrow schema,
    /// * the matched field is nested,
    /// * the matched root has no Parquet leaf columns.
    pub(crate) fn leaf_index(&self, name: &str) -> Option<usize> {
        self.resolve(name).map(|(_, leaf)| leaf)
    }

    /// Build a [`StatisticsConverter`] for `name`.
    ///
    /// When `name` has no leaf this falls back to
    /// [`StatisticsConverter::try_new`], which is O(N). The fallback gives the
    /// same error as `try_new` for an unknown column, and is needed because
    /// [`StatisticsConverter::from_column_index`] cannot express a column with
    /// no Parquet leaf. Flat schemas never take it.
    pub(crate) fn converter(
        &self,
        name: &str,
    ) -> parquet::errors::Result<StatisticsConverter<'a>> {
        match self.resolve(name) {
            Some((root, leaf)) => StatisticsConverter::from_column_index(
                leaf,
                self.schema.field(root),
                self.parquet_schema,
            ),
            None => StatisticsConverter::try_new(name, self.schema, self.parquet_schema),
        }
    }

    /// The Arrow root index and Parquet leaf index for `name`.
    fn resolve(&self, name: &str) -> Option<(usize, usize)> {
        let root = *self.name_to_root.get(name)?;
        if self.schema.field(root).data_type().is_nested() {
            return None;
        }
        let leaf = self.root_to_first_leaf.get(root).copied().flatten()?;
        Some((root, leaf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Fields};
    use parquet::arrow::{ArrowSchemaConverter, parquet_column};
    use parquet::schema::parser::parse_message_type;
    use std::sync::Arc;

    /// Primitives, a nested struct, a nested list, and a duplicate field name
    /// with a different type from the first.
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
            // Unreachable by name: "a" resolves to index 0.
            Field::new("a", DataType::Utf8, true),
        ])
    }

    fn descriptor(message: &str) -> SchemaDescriptor {
        SchemaDescriptor::new(Arc::new(parse_message_type(message).unwrap()))
    }

    /// Asserts `leaf_index` and `converter` agree with `parquet_column` and
    /// `try_new` for every field name in `schema`, plus some absent names.
    fn assert_matches_upstream(descr: &SchemaDescriptor, schema: &Schema) {
        let resolver = LeafResolver::new(descr, schema);

        let mut names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        names.extend(["missing", "", "A"]);

        for name in names {
            let expected = parquet_column(descr, schema, name).map(|(idx, _)| idx);
            assert_eq!(
                resolver.leaf_index(name),
                expected,
                "leaf_index disagreed with parquet_column for {name:?}"
            );

            match (
                resolver.converter(name),
                StatisticsConverter::try_new(name, schema, descr),
            ) {
                (Ok(actual), Ok(expected)) => {
                    assert_eq!(
                        actual.parquet_column_index(),
                        expected.parquet_column_index(),
                        "converter targeted a different column for {name:?}"
                    );
                    assert_eq!(
                        actual.arrow_field(),
                        expected.arrow_field(),
                        "converter used a different Arrow field for {name:?}"
                    );
                }
                (Err(actual), Err(expected)) => {
                    assert_eq!(actual.to_string(), expected.to_string());
                }
                (actual, expected) => panic!(
                    "converter for {name:?} returned {:?}, try_new returned {:?}",
                    actual.map(|c| c.parquet_column_index()),
                    expected.map(|c| c.parquet_column_index()),
                ),
            }
        }
    }

    #[test]
    fn matches_upstream() {
        let schema = test_schema();
        let descr = ArrowSchemaConverter::new().convert(&schema).unwrap();
        assert_matches_upstream(&descr, &schema);
    }

    #[test]
    fn resolves_expected_leaves() {
        let schema = test_schema();
        let descr = ArrowSchemaConverter::new().convert(&schema).unwrap();
        let resolver = LeafResolver::new(&descr, &schema);

        assert_eq!(resolver.leaf_index("a"), Some(0));
        assert_eq!(resolver.leaf_index("b"), Some(1));
        // "s" has two leaves and "l" one, so "c" is leaf 5.
        assert_eq!(resolver.leaf_index("c"), Some(5));
        assert_eq!(resolver.leaf_index("s"), None);
        assert_eq!(resolver.leaf_index("l"), None);
        assert_eq!(resolver.leaf_index("missing"), None);
    }

    /// The converter for a duplicated name must use the first field's type.
    #[test]
    fn duplicate_name_uses_first_field() {
        let schema = test_schema();
        let descr = ArrowSchemaConverter::new().convert(&schema).unwrap();
        let resolver = LeafResolver::new(&descr, &schema);

        let converter = resolver.converter("a").unwrap();
        assert_eq!(converter.arrow_field().data_type(), &DataType::Int64);
    }

    /// A root with no leaves must resolve to no leaf, even when the Arrow
    /// field at that index is a primitive.
    #[test]
    fn empty_group_root() {
        let descr = descriptor(
            "message schema {
                required int64 a;
                optional group g {}
                required int64 c;
            }",
        );
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("g", DataType::Int64, true),
            Field::new("c", DataType::Int64, false),
        ]);
        assert_eq!(LeafResolver::new(&descr, &schema).leaf_index("g"), None);
        assert_matches_upstream(&descr, &schema);
    }

    /// A primitive Arrow field over a multi-leaf Parquet root must resolve to
    /// the root's lowest leaf.
    #[test]
    fn multi_leaf_root_uses_lowest_leaf() {
        let descr = descriptor(
            "message schema {
                required int64 a;
                optional group g {
                    required int64 x;
                    required int64 y;
                }
            }",
        );
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("g", DataType::Int64, true),
        ]);
        assert_eq!(LeafResolver::new(&descr, &schema).leaf_index("g"), Some(1));
        assert_matches_upstream(&descr, &schema);
    }

    /// Arrow fields beyond the last Parquet root must resolve to no leaf.
    #[test]
    fn more_arrow_fields_than_parquet_roots() {
        let descr = descriptor("message schema { required int64 a; }");
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, true),
        ]);
        assert_eq!(LeafResolver::new(&descr, &schema).leaf_index("b"), None);
        assert_matches_upstream(&descr, &schema);
    }

    #[test]
    fn empty_schema() {
        let schema = Schema::empty();
        let descr = ArrowSchemaConverter::new().convert(&schema).unwrap();
        assert_eq!(
            LeafResolver::new(&descr, &schema).leaf_index("anything"),
            None
        );
        assert_matches_upstream(&descr, &schema);
    }
}
