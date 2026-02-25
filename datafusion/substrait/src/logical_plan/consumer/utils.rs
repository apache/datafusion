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

use crate::logical_plan::consumer::SubstraitConsumer;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit, UnionFields};
use datafusion::common::{
    DFSchema, DFSchemaRef, exec_err, not_impl_err, substrait_datafusion_err,
    substrait_err,
};
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::{Cast, Expr, ExprSchemable};
use datafusion::sql::TableReference;
use std::collections::HashSet;
use std::sync::Arc;
use substrait::proto::SortField;
use substrait::proto::sort_field::SortDirection;
use substrait::proto::sort_field::SortKind::{ComparisonFunctionReference, Direction};

// Substrait PrecisionTimestampTz indicates that the timestamp is relative to UTC, which
// is the same as the expectation for any non-empty timezone in DF, so any non-empty timezone
// results in correct points on the timeline, and we pick UTC as a reasonable default.
// However, DF uses the timezone also for some arithmetic and display purposes (see e.g.
// https://github.com/apache/arrow-rs/blob/ee5694078c86c8201549654246900a4232d531a9/arrow-cast/src/cast/mod.rs#L1749).
pub(super) const DEFAULT_TIMEZONE: &str = "UTC";

pub(super) fn next_struct_field_name(
    column_idx: usize,
    dfs_names: &[String],
    name_idx: &mut usize,
) -> datafusion::common::Result<String> {
    if dfs_names.is_empty() {
        // If names are not given, create dummy names
        // c0, c1, ... align with e.g. SqlToRel::create_named_struct
        Ok(format!("c{column_idx}"))
    } else {
        let name = dfs_names.get(*name_idx).cloned().ok_or_else(|| {
            substrait_datafusion_err!("Named schema must contain names for all fields")
        })?;
        *name_idx += 1;
        Ok(name)
    }
}

/// Traverse through the field, renaming the provided field itself and all its inner struct fields.
pub fn rename_field(
    field: &Field,
    dfs_names: &Vec<String>,
    unnamed_field_suffix: usize, // If Substrait doesn't provide a name, we'll use this "c{unnamed_field_suffix}"
    name_idx: &mut usize,        // Index into dfs_names
) -> datafusion::common::Result<Field> {
    let name = next_struct_field_name(unnamed_field_suffix, dfs_names, name_idx)?;
    rename_fields_data_type(field.clone().with_name(name), dfs_names, name_idx)
}

/// Rename the field's data type but not the field itself.
pub fn rename_fields_data_type(
    field: Field,
    dfs_names: &Vec<String>,
    name_idx: &mut usize, // Index into dfs_names
) -> datafusion::common::Result<Field> {
    let dt = rename_data_type(field.data_type(), dfs_names, name_idx)?;
    Ok(field.with_data_type(dt))
}

/// Traverse through the data type (incl. lists/maps/etc), renaming all inner struct fields.
pub fn rename_data_type(
    data_type: &DataType,
    dfs_names: &Vec<String>,
    name_idx: &mut usize, // Index into dfs_names
) -> datafusion::common::Result<DataType> {
    match data_type {
        DataType::Struct(children) => {
            let children = children
                .iter()
                .enumerate()
                .map(|(field_idx, f)| {
                    rename_field(f.as_ref(), dfs_names, field_idx, name_idx)
                })
                .collect::<datafusion::common::Result<_>>()?;
            Ok(DataType::Struct(children))
        }
        DataType::List(inner) => Ok(DataType::List(Arc::new(rename_fields_data_type(
            inner.as_ref().to_owned(),
            dfs_names,
            name_idx,
        )?))),
        DataType::LargeList(inner) => Ok(DataType::LargeList(Arc::new(
            rename_fields_data_type(inner.as_ref().to_owned(), dfs_names, name_idx)?,
        ))),
        DataType::ListView(inner) => Ok(DataType::ListView(Arc::new(
            rename_fields_data_type(inner.as_ref().to_owned(), dfs_names, name_idx)?,
        ))),
        DataType::LargeListView(inner) => Ok(DataType::LargeListView(Arc::new(
            rename_fields_data_type(inner.as_ref().to_owned(), dfs_names, name_idx)?,
        ))),
        DataType::FixedSizeList(inner, len) => Ok(DataType::FixedSizeList(
            Arc::new(rename_fields_data_type(
                inner.as_ref().to_owned(),
                dfs_names,
                name_idx,
            )?),
            *len,
        )),
        DataType::Map(entries, sorted) => {
            let entries_data_type = match entries.data_type() {
                DataType::Struct(fields) => {
                    // This should be two fields, normally "key" and "value", but not guaranteed
                    let fields = fields
                        .iter()
                        .map(|f| {
                            rename_fields_data_type(
                                f.as_ref().to_owned(),
                                dfs_names,
                                name_idx,
                            )
                        })
                        .collect::<datafusion::common::Result<_>>()?;
                    Ok(DataType::Struct(fields))
                }
                _ => exec_err!("Expected map type to contain an inner struct type"),
            }?;
            Ok(DataType::Map(
                Arc::new(
                    entries
                        .as_ref()
                        .to_owned()
                        .with_data_type(entries_data_type),
                ),
                *sorted,
            ))
        }
        DataType::Dictionary(key_type, value_type) => {
            // Dicts probably shouldn't contain structs, but support them just in case one does
            Ok(DataType::Dictionary(
                Box::new(rename_data_type(key_type, dfs_names, name_idx)?),
                Box::new(rename_data_type(value_type, dfs_names, name_idx)?),
            ))
        }
        DataType::RunEndEncoded(run_ends_field, values_field) => {
            // At least the run_ends_field shouldn't contain names (since it should be i16/i32/i64),
            // but we'll try renaming its datatype just in case.
            let run_ends_field = rename_fields_data_type(
                run_ends_field.as_ref().clone(),
                dfs_names,
                name_idx,
            )?;
            let values_field = rename_fields_data_type(
                values_field.as_ref().clone(),
                dfs_names,
                name_idx,
            )?;

            Ok(DataType::RunEndEncoded(
                Arc::new(run_ends_field),
                Arc::new(values_field),
            ))
        }
        DataType::Union(fields, mode) => {
            let fields = fields
                .iter()
                .map(|(i, f)| {
                    Ok((
                        i,
                        Arc::new(rename_fields_data_type(
                            f.as_ref().clone(),
                            dfs_names,
                            name_idx,
                        )?),
                    ))
                })
                .collect::<datafusion::common::Result<UnionFields>>()?;
            Ok(DataType::Union(fields, *mode))
        }
        // Explicitly listing the rest (which can not contain inner fields needing renaming)
        // to ensure we're exhaustive
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Timestamp(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => Ok(data_type.clone()),
    }
}

/// Produce a version of the given schema with names matching the given list of names.
/// Substrait doesn't deal with column (incl. nested struct field) names within the schema,
/// but it does give us the list of expected names at the end of the plan, so we use this
/// to rename the schema to match the expected names.
pub(super) fn make_renamed_schema(
    schema: &DFSchemaRef,
    dfs_names: &Vec<String>,
) -> datafusion::common::Result<DFSchema> {
    let mut name_idx = 0;

    let (qualifiers, fields): (_, Vec<Field>) = schema
        .iter()
        .enumerate()
        .map(|(field_idx, (q, f))| {
            let renamed_f =
                rename_field(f.as_ref(), dfs_names, field_idx, &mut name_idx)?;
            Ok((q.cloned(), renamed_f))
        })
        .collect::<datafusion::common::Result<Vec<_>>>()?
        .into_iter()
        .unzip();

    if name_idx != dfs_names.len() {
        return substrait_err!(
            "Names list must match exactly to nested schema, but found {} uses for {} names",
            name_idx,
            dfs_names.len()
        );
    }

    DFSchema::from_field_specific_qualified_schema(
        qualifiers,
        &Arc::new(Schema::new(fields)),
    )
}

/// Ensure the expressions have the right name(s) according to the new schema.
/// This includes the top-level (column) name, which will be renamed through aliasing if needed,
/// as well as nested names (if the expression produces any struct types), which will be renamed
/// through casting if needed.
pub(super) fn rename_expressions(
    exprs: impl IntoIterator<Item = Expr>,
    input_schema: &DFSchema,
    new_schema_fields: &[Arc<Field>],
) -> datafusion::common::Result<Vec<Expr>> {
    exprs
        .into_iter()
        .zip(new_schema_fields)
        .map(|(old_expr, new_field)| {
            // Check if type (i.e. nested struct field names) match, use Cast to rename if needed
            let new_expr = if &old_expr.get_type(input_schema)? != new_field.data_type() {
                Expr::Cast(Cast::new(
                    Box::new(old_expr),
                    new_field.data_type().to_owned(),
                ))
            } else {
                old_expr
            };
            // Alias column if needed to fix the top-level name
            match &new_expr {
                // If expr is a column reference, alias_if_changed would cause an aliasing if the old expr has a qualifier
                Expr::Column(c) if &c.name == new_field.name() => Ok(new_expr),
                _ => new_expr.alias_if_changed(new_field.name().to_owned()),
            }
        })
        .collect()
}

/// Ensures that the given Substrait schema is compatible with the schema as given by DataFusion
///
/// This means:
/// 1. All fields present in the Substrait schema are present in the DataFusion schema. The
///    DataFusion schema may have MORE fields, but not the other way around.
/// 2. All fields are compatible. See [`ensure_field_compatibility`] for details
pub(super) fn ensure_schema_compatibility(
    table_schema: &DFSchema,
    substrait_schema: DFSchema,
) -> datafusion::common::Result<()> {
    substrait_schema
        .strip_qualifiers()
        .fields()
        .iter()
        .try_for_each(|substrait_field| {
            let df_field =
                table_schema.field_with_unqualified_name(substrait_field.name())?;
            ensure_field_compatibility(df_field, substrait_field)
        })
}

/// Ensures that the given Substrait field is compatible with the given DataFusion field
///
/// A field is compatible between Substrait and DataFusion if:
/// 1. They have logically equivalent types.
/// 2. They have the same nullability OR the Substrait field is nullable and the DataFusion fields
///    is not nullable.
///
/// If a Substrait field is not nullable, the Substrait plan may be built around assuming it is not
/// nullable. As such if DataFusion has that field as nullable the plan should be rejected.
fn ensure_field_compatibility(
    datafusion_field: &Field,
    substrait_field: &Field,
) -> datafusion::common::Result<()> {
    if !DFSchema::datatype_is_logically_equal(
        datafusion_field.data_type(),
        substrait_field.data_type(),
    ) {
        return substrait_err!(
            "Field '{}' in Substrait schema has a different type ({}) than the corresponding field in the table schema ({}).",
            substrait_field.name(),
            substrait_field.data_type(),
            datafusion_field.data_type()
        );
    }

    if !compatible_nullabilities(
        datafusion_field.is_nullable(),
        substrait_field.is_nullable(),
    ) {
        // TODO: from_substrait_struct_type needs to be updated to set the nullability correctly. It defaults to true for now.
        return substrait_err!(
            "Field '{}' is nullable in the DataFusion schema but not nullable in the Substrait schema.",
            substrait_field.name()
        );
    }
    Ok(())
}

/// Returns true if the DataFusion and Substrait nullabilities are compatible, false otherwise
fn compatible_nullabilities(
    datafusion_nullability: bool,
    substrait_nullability: bool,
) -> bool {
    // DataFusion and Substrait have the same nullability
    (datafusion_nullability == substrait_nullability)
        // DataFusion is not nullable and Substrait is nullable
        || (!datafusion_nullability && substrait_nullability)
}

pub(super) struct NameTracker {
    /// Tracks seen schema names (from expr.schema_name()).
    /// Used to detect duplicates that would fail validate_unique_names.
    seen_schema_names: HashSet<String>,
    /// Tracks column names that have been seen with a qualifier.
    /// Used to detect ambiguous references (qualified + unqualified with same name).
    qualified_names: HashSet<String>,
    /// Tracks column names that have been seen without a qualifier.
    /// Used to detect ambiguous references.
    unqualified_names: HashSet<String>,
}

impl NameTracker {
    pub(super) fn new() -> Self {
        NameTracker {
            seen_schema_names: HashSet::default(),
            qualified_names: HashSet::default(),
            unqualified_names: HashSet::default(),
        }
    }

    /// Check if the expression would cause a conflict either in:
    /// 1. validate_unique_names (duplicate schema_name)
    /// 2. DFSchema::check_names (ambiguous reference)
    fn would_conflict(&self, expr: &Expr) -> bool {
        let (qualifier, name) = expr.qualified_name();
        let schema_name = expr.schema_name().to_string();
        self.would_conflict_inner((qualifier, &name), &schema_name)
    }

    fn would_conflict_inner(
        &self,
        qualified_name: (Option<TableReference>, &str),
        schema_name: &str,
    ) -> bool {
        // Check for duplicate schema_name (would fail validate_unique_names)
        if self.seen_schema_names.contains(schema_name) {
            return true;
        }

        // Check for ambiguous reference (would fail DFSchema::check_names)
        // This happens when a qualified field and unqualified field have the same name
        let (qualifier, name) = qualified_name;
        match qualifier {
            Some(_) => {
                // Adding a qualified name - conflicts if unqualified version exists
                self.unqualified_names.contains(name)
            }
            None => {
                // Adding an unqualified name - conflicts if qualified version exists
                self.qualified_names.contains(name)
            }
        }
    }

    fn insert(&mut self, expr: &Expr) {
        let schema_name = expr.schema_name().to_string();
        self.seen_schema_names.insert(schema_name);

        let (qualifier, name) = expr.qualified_name();
        match qualifier {
            Some(_) => {
                self.qualified_names.insert(name);
            }
            None => {
                self.unqualified_names.insert(name);
            }
        }
    }

    pub(super) fn get_uniquely_named_expr(
        &mut self,
        expr: Expr,
    ) -> datafusion::common::Result<Expr> {
        if !self.would_conflict(&expr) {
            self.insert(&expr);
            return Ok(expr);
        }

        // Name collision - need to generate a unique alias
        let schema_name = expr.schema_name().to_string();
        let mut counter = 0;
        let candidate_name = loop {
            let candidate_name = format!("{schema_name}__temp__{counter}");
            // .alias always produces an unqualified name so check for conflicts accordingly.
            if !self.would_conflict_inner((None, &candidate_name), &candidate_name) {
                break candidate_name;
            }
            counter += 1;
        };
        let candidate_expr = expr.alias(&candidate_name);
        self.insert(&candidate_expr);
        Ok(candidate_expr)
    }
}

/// Convert Substrait Sorts to DataFusion Exprs
pub async fn from_substrait_sorts(
    consumer: &impl SubstraitConsumer,
    substrait_sorts: &Vec<SortField>,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Vec<Sort>> {
    let mut sorts: Vec<Sort> = vec![];
    for s in substrait_sorts {
        let expr = consumer
            .consume_expression(s.expr.as_ref().unwrap(), input_schema)
            .await?;
        let asc_nullfirst = match &s.sort_kind {
            Some(k) => match k {
                Direction(d) => {
                    let Ok(direction) = SortDirection::try_from(*d) else {
                        return not_impl_err!(
                            "Unsupported Substrait SortDirection value {d}"
                        );
                    };

                    match direction {
                        SortDirection::AscNullsFirst => Ok((true, true)),
                        SortDirection::AscNullsLast => Ok((true, false)),
                        SortDirection::DescNullsFirst => Ok((false, true)),
                        SortDirection::DescNullsLast => Ok((false, false)),
                        SortDirection::Clustered => not_impl_err!(
                            "Sort with direction clustered is not yet supported"
                        ),
                        SortDirection::Unspecified => {
                            not_impl_err!("Unspecified sort direction is invalid")
                        }
                    }
                }
                ComparisonFunctionReference(_) => not_impl_err!(
                    "Sort using comparison function reference is not supported"
                ),
            },
            None => not_impl_err!("Sort without sort kind is invalid"),
        };
        let (asc, nulls_first) = asc_nullfirst.unwrap();
        sorts.push(Sort {
            expr,
            asc,
            nulls_first,
        });
    }
    Ok(sorts)
}

pub(crate) fn from_substrait_precision(
    precision: i32,
    type_name: &str,
) -> datafusion::common::Result<TimeUnit> {
    match precision {
        0 => Ok(TimeUnit::Second),
        3 => Ok(TimeUnit::Millisecond),
        6 => Ok(TimeUnit::Microsecond),
        9 => Ok(TimeUnit::Nanosecond),
        precision => {
            not_impl_err!("Unsupported Substrait precision {precision}, for {type_name}")
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::{NameTracker, make_renamed_schema};
    use crate::extensions::Extensions;
    use crate::logical_plan::consumer::DefaultSubstraitConsumer;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::DFSchema;
    use datafusion::error::Result;
    use datafusion::execution::SessionState;
    use datafusion::logical_expr::{Expr, col};
    use datafusion::prelude::SessionContext;
    use datafusion::sql::TableReference;
    use std::collections::HashMap;
    use std::sync::{Arc, LazyLock};

    pub(crate) static TEST_SESSION_STATE: LazyLock<SessionState> =
        LazyLock::new(|| SessionContext::default().state());
    pub(crate) static TEST_EXTENSIONS: LazyLock<Extensions> =
        LazyLock::new(Extensions::default);
    pub(crate) fn test_consumer() -> DefaultSubstraitConsumer<'static> {
        let extensions = &TEST_EXTENSIONS;
        let state = &TEST_SESSION_STATE;
        DefaultSubstraitConsumer::new(extensions, state)
    }

    #[tokio::test]
    async fn rename_schema() -> Result<()> {
        let table_ref = TableReference::bare("test");
        let fields = vec![
            (
                Some(table_ref.clone()),
                Arc::new(Field::new("0", DataType::Int32, false)),
            ),
            (
                Some(table_ref.clone()),
                Arc::new(Field::new_struct(
                    "1",
                    vec![
                        Field::new("2", DataType::Int32, false),
                        Field::new_struct(
                            "3",
                            vec![Field::new("4", DataType::Int32, false)],
                            false,
                        ),
                    ],
                    false,
                )),
            ),
            (
                Some(table_ref.clone()),
                Arc::new(Field::new_list(
                    "5",
                    Arc::new(Field::new_struct(
                        "item",
                        vec![Field::new("6", DataType::Int32, false)],
                        false,
                    )),
                    false,
                )),
            ),
            (
                Some(table_ref.clone()),
                Arc::new(Field::new_large_list(
                    "7",
                    Arc::new(Field::new_struct(
                        "item",
                        vec![Field::new("8", DataType::Int32, false)],
                        false,
                    )),
                    false,
                )),
            ),
            (
                Some(table_ref.clone()),
                Arc::new(Field::new_map(
                    "9",
                    "entries",
                    Arc::new(Field::new_struct(
                        "keys",
                        vec![Field::new("10", DataType::Int32, false)],
                        false,
                    )),
                    Arc::new(Field::new_struct(
                        "values",
                        vec![Field::new("11", DataType::Int32, false)],
                        false,
                    )),
                    false,
                    false,
                )),
            ),
        ];

        let schema = Arc::new(DFSchema::new_with_metadata(fields, HashMap::default())?);
        let dfs_names = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
            "f".to_string(),
            "g".to_string(),
            "h".to_string(),
            "i".to_string(),
            "j".to_string(),
            "k".to_string(),
            "l".to_string(),
        ];
        let renamed_schema = make_renamed_schema(&schema, &dfs_names)?;

        assert_eq!(renamed_schema.fields().len(), 5);
        assert_eq!(
            renamed_schema.field(0),
            &Arc::new(Field::new("a", DataType::Int32, false))
        );
        assert_eq!(
            renamed_schema.field(1),
            &Arc::new(Field::new_struct(
                "b",
                vec![
                    Field::new("c", DataType::Int32, false),
                    Field::new_struct(
                        "d",
                        vec![Field::new("e", DataType::Int32, false)],
                        false,
                    )
                ],
                false,
            ))
        );
        assert_eq!(
            renamed_schema.field(2),
            &Arc::new(Field::new_list(
                "f",
                Arc::new(Field::new_struct(
                    "item",
                    vec![Field::new("g", DataType::Int32, false)],
                    false,
                )),
                false,
            ))
        );
        assert_eq!(
            renamed_schema.field(3),
            &Arc::new(Field::new_large_list(
                "h",
                Arc::new(Field::new_struct(
                    "item",
                    vec![Field::new("i", DataType::Int32, false)],
                    false,
                )),
                false,
            ))
        );
        assert_eq!(
            renamed_schema.field(4),
            &Arc::new(Field::new_map(
                "j",
                "entries",
                Arc::new(Field::new_struct(
                    "keys",
                    vec![Field::new("k", DataType::Int32, false)],
                    false,
                )),
                Arc::new(Field::new_struct(
                    "values",
                    vec![Field::new("l", DataType::Int32, false)],
                    false,
                )),
                false,
                false,
            ))
        );
        Ok(())
    }

    #[test]
    fn name_tracker_unique_names_pass_through() -> Result<()> {
        let mut tracker = NameTracker::new();

        // First expression should pass through unchanged
        let expr1 = col("a");
        let result1 = tracker.get_uniquely_named_expr(expr1.clone())?;
        assert_eq!(result1, col("a"));

        // Different name should also pass through unchanged
        let expr2 = col("b");
        let result2 = tracker.get_uniquely_named_expr(expr2)?;
        assert_eq!(result2, col("b"));

        Ok(())
    }

    #[test]
    fn name_tracker_duplicate_schema_name_gets_alias() -> Result<()> {
        let mut tracker = NameTracker::new();

        // First expression with name "a"
        let expr1 = col("a");
        let result1 = tracker.get_uniquely_named_expr(expr1)?;
        assert_eq!(result1, col("a"));

        // Second expression with same name "a" should get aliased
        let expr2 = col("a");
        let result2 = tracker.get_uniquely_named_expr(expr2)?;
        assert_eq!(result2, col("a").alias("a__temp__0"));

        // Third expression with same name "a" should get a different alias
        let expr3 = col("a");
        let result3 = tracker.get_uniquely_named_expr(expr3)?;
        assert_eq!(result3, col("a").alias("a__temp__1"));

        Ok(())
    }

    #[test]
    fn name_tracker_qualified_then_unqualified_conflicts() -> Result<()> {
        let mut tracker = NameTracker::new();

        // First: qualified column "table.a"
        let qualified_col =
            Expr::Column(datafusion::common::Column::new(Some("table"), "a"));
        let result1 = tracker.get_uniquely_named_expr(qualified_col)?;
        assert_eq!(
            result1,
            Expr::Column(datafusion::common::Column::new(Some("table"), "a"))
        );

        // Second: unqualified column "a" - should conflict (ambiguous reference)
        let unqualified_col = col("a");
        let result2 = tracker.get_uniquely_named_expr(unqualified_col)?;
        // Should be aliased to avoid ambiguous reference
        assert_eq!(result2, col("a").alias("a__temp__0"));

        Ok(())
    }

    #[test]
    fn name_tracker_unqualified_then_qualified_conflicts() -> Result<()> {
        let mut tracker = NameTracker::new();

        // First: unqualified column "a"
        let unqualified_col = col("a");
        let result1 = tracker.get_uniquely_named_expr(unqualified_col)?;
        assert_eq!(result1, col("a"));

        // Second: qualified column "table.a" - should conflict (ambiguous reference)
        let qualified_col =
            Expr::Column(datafusion::common::Column::new(Some("table"), "a"));
        let result2 = tracker.get_uniquely_named_expr(qualified_col)?;
        // Should be aliased to avoid ambiguous reference
        assert_eq!(
            result2,
            Expr::Column(datafusion::common::Column::new(Some("table"), "a"))
                .alias("table.a__temp__0")
        );

        Ok(())
    }

    #[test]
    fn name_tracker_different_qualifiers_no_conflict() -> Result<()> {
        let mut tracker = NameTracker::new();

        // First: qualified column "table1.a"
        let col1 = Expr::Column(datafusion::common::Column::new(Some("table1"), "a"));
        let result1 = tracker.get_uniquely_named_expr(col1.clone())?;
        assert_eq!(result1, col1);

        // Second: qualified column "table2.a" - different qualifier, different schema_name
        // so should NOT conflict
        let col2 = Expr::Column(datafusion::common::Column::new(Some("table2"), "a"));
        let result2 = tracker.get_uniquely_named_expr(col2.clone())?;
        assert_eq!(result2, col2);

        Ok(())
    }

    #[test]
    fn name_tracker_aliased_expressions() -> Result<()> {
        let mut tracker = NameTracker::new();

        // First: col("x").alias("result")
        let expr1 = col("x").alias("result");
        let result1 = tracker.get_uniquely_named_expr(expr1.clone())?;
        assert_eq!(result1, col("x").alias("result"));

        // Second: col("y").alias("result") - same alias name, should conflict
        let expr2 = col("y").alias("result");
        let result2 = tracker.get_uniquely_named_expr(expr2)?;
        assert_eq!(result2, col("y").alias("result").alias("result__temp__0"));

        Ok(())
    }
}
