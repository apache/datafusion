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
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema};
use datafusion::common::{
    not_impl_err, substrait_datafusion_err, substrait_err, DFSchema, DFSchemaRef,
    TableReference,
};
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::{Cast, Expr, ExprSchemable, LogicalPlanBuilder};
use std::collections::HashSet;
use std::sync::Arc;
use substrait::proto::sort_field::SortDirection;
use substrait::proto::sort_field::SortKind::{ComparisonFunctionReference, Direction};
use substrait::proto::SortField;

// Substrait PrecisionTimestampTz indicates that the timestamp is relative to UTC, which
// is the same as the expectation for any non-empty timezone in DF, so any non-empty timezone
// results in correct points on the timeline, and we pick UTC as a reasonable default.
// However, DF uses the timezone also for some arithmetic and display purposes (see e.g.
// https://github.com/apache/arrow-rs/blob/ee5694078c86c8201549654246900a4232d531a9/arrow-cast/src/cast/mod.rs#L1749).
pub(super) const DEFAULT_TIMEZONE: &str = "UTC";

/// (Re)qualify the sides of a join if needed, i.e. if the columns from one side would otherwise
/// conflict with the columns from the other.
/// Substrait doesn't currently allow specifying aliases, neither for columns nor for tables. For
/// Substrait the names don't matter since it only refers to columns by indices, however DataFusion
/// requires columns to be uniquely identifiable, in some places (see e.g. DFSchema::check_names).
pub(super) fn requalify_sides_if_needed(
    left: LogicalPlanBuilder,
    right: LogicalPlanBuilder,
) -> datafusion::common::Result<(LogicalPlanBuilder, LogicalPlanBuilder)> {
    let left_cols = left.schema().columns();
    let right_cols = right.schema().columns();
    if left_cols.iter().any(|l| {
        right_cols.iter().any(|r| {
            l == r || (l.name == r.name && (l.relation.is_none() || r.relation.is_none()))
        })
    }) {
        // These names have no connection to the original plan, but they'll make the columns
        // (mostly) unique.
        Ok((
            left.alias(TableReference::bare("left"))?,
            right.alias(TableReference::bare("right"))?,
        ))
    } else {
        Ok((left, right))
    }
}

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

pub(super) fn rename_field(
    field: &Field,
    dfs_names: &Vec<String>,
    unnamed_field_suffix: usize, // If Substrait doesn't provide a name, we'll use this "c{unnamed_field_suffix}"
    name_idx: &mut usize,        // Index into dfs_names
    rename_self: bool, // Some fields (e.g. list items) don't have names in Substrait and this will be false to keep old name
) -> datafusion::common::Result<Field> {
    let name = if rename_self {
        next_struct_field_name(unnamed_field_suffix, dfs_names, name_idx)?
    } else {
        field.name().to_string()
    };
    match field.data_type() {
        DataType::Struct(children) => {
            let children = children
                .iter()
                .enumerate()
                .map(|(child_idx, f)| {
                    rename_field(
                        f.as_ref(),
                        dfs_names,
                        child_idx,
                        name_idx,
                        /*rename_self=*/ true,
                    )
                })
                .collect::<datafusion::common::Result<_>>()?;
            Ok(field
                .to_owned()
                .with_name(name)
                .with_data_type(DataType::Struct(children)))
        }
        DataType::List(inner) => {
            let renamed_inner = rename_field(
                inner.as_ref(),
                dfs_names,
                0,
                name_idx,
                /*rename_self=*/ false,
            )?;
            Ok(field
                .to_owned()
                .with_data_type(DataType::List(FieldRef::new(renamed_inner)))
                .with_name(name))
        }
        DataType::LargeList(inner) => {
            let renamed_inner = rename_field(
                inner.as_ref(),
                dfs_names,
                0,
                name_idx,
                /*rename_self= */ false,
            )?;
            Ok(field
                .to_owned()
                .with_data_type(DataType::LargeList(FieldRef::new(renamed_inner)))
                .with_name(name))
        }
        DataType::Map(inner, sorted) => match inner.data_type() {
            DataType::Struct(key_and_value) if key_and_value.len() == 2 => {
                let renamed_keys = rename_field(
                    key_and_value[0].as_ref(),
                    dfs_names,
                    0,
                    name_idx,
                    /*rename_self=*/ false,
                )?;
                let renamed_values = rename_field(
                    key_and_value[1].as_ref(),
                    dfs_names,
                    0,
                    name_idx,
                    /*rename_self=*/ false,
                )?;
                Ok(field
                    .to_owned()
                    .with_data_type(DataType::Map(
                        Arc::new(Field::new(
                            inner.name(),
                            DataType::Struct(Fields::from(vec![
                                renamed_keys,
                                renamed_values,
                            ])),
                            inner.is_nullable(),
                        )),
                        *sorted,
                    ))
                    .with_name(name))
            }
            _ => substrait_err!("Map fields must contain a Struct with exactly 2 fields"),
        },
        _ => Ok(field.to_owned().with_name(name)),
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
            let renamed_f = rename_field(
                f.as_ref(),
                dfs_names,
                field_idx,
                &mut name_idx,
                /*rename_self=*/ true,
            )?;
            Ok((q.cloned(), renamed_f))
        })
        .collect::<datafusion::common::Result<Vec<_>>>()?
        .into_iter()
        .unzip();

    if name_idx != dfs_names.len() {
        return substrait_err!(
            "Names list must match exactly to nested schema, but found {} uses for {} names",
            name_idx,
            dfs_names.len());
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
    seen_names: HashSet<String>,
}

pub(super) enum NameTrackerStatus {
    NeverSeen,
    SeenBefore,
}

impl NameTracker {
    pub(super) fn new() -> Self {
        NameTracker {
            seen_names: HashSet::default(),
        }
    }
    pub(super) fn get_unique_name(
        &mut self,
        name: String,
    ) -> (String, NameTrackerStatus) {
        match self.seen_names.insert(name.clone()) {
            true => (name, NameTrackerStatus::NeverSeen),
            false => {
                let mut counter = 0;
                loop {
                    let candidate_name = format!("{name}__temp__{counter}");
                    if self.seen_names.insert(candidate_name.clone()) {
                        return (candidate_name, NameTrackerStatus::SeenBefore);
                    }
                    counter += 1;
                }
            }
        }
    }

    pub(super) fn get_uniquely_named_expr(
        &mut self,
        expr: Expr,
    ) -> datafusion::common::Result<Expr> {
        match self.get_unique_name(expr.name_for_alias()?) {
            (_, NameTrackerStatus::NeverSeen) => Ok(expr),
            (name, NameTrackerStatus::SeenBefore) => Ok(expr.alias(name)),
        }
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

#[cfg(test)]
pub(crate) mod tests {
    use super::make_renamed_schema;
    use crate::extensions::Extensions;
    use crate::logical_plan::consumer::DefaultSubstraitConsumer;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::DFSchema;
    use datafusion::error::Result;
    use datafusion::execution::SessionState;
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
                Arc::new(Field::new_map(
                    "7",
                    "entries",
                    Arc::new(Field::new_struct(
                        "keys",
                        vec![Field::new("8", DataType::Int32, false)],
                        false,
                    )),
                    Arc::new(Field::new_struct(
                        "values",
                        vec![Field::new("9", DataType::Int32, false)],
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
        ];
        let renamed_schema = make_renamed_schema(&schema, &dfs_names)?;

        assert_eq!(renamed_schema.fields().len(), 4);
        assert_eq!(
            *renamed_schema.field(0),
            Field::new("a", DataType::Int32, false)
        );
        assert_eq!(
            *renamed_schema.field(1),
            Field::new_struct(
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
            )
        );
        assert_eq!(
            *renamed_schema.field(2),
            Field::new_list(
                "f",
                Arc::new(Field::new_struct(
                    "item",
                    vec![Field::new("g", DataType::Int32, false)],
                    false,
                )),
                false,
            )
        );
        assert_eq!(
            *renamed_schema.field(3),
            Field::new_map(
                "h",
                "entries",
                Arc::new(Field::new_struct(
                    "keys",
                    vec![Field::new("i", DataType::Int32, false)],
                    false,
                )),
                Arc::new(Field::new_struct(
                    "values",
                    vec![Field::new("j", DataType::Int32, false)],
                    false,
                )),
                false,
                false,
            )
        );
        Ok(())
    }
}
