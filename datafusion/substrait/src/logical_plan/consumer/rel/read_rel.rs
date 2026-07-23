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
use crate::logical_plan::consumer::from_substrait_literal;
use crate::logical_plan::consumer::from_substrait_named_struct;
use crate::logical_plan::consumer::utils::{
    ensure_field_compatibility, ensure_schema_compatibility, rename_expressions,
};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::{
    Column, DFSchema, DFSchemaRef, TableReference, not_impl_err, plan_datafusion_err,
    plan_err, substrait_datafusion_err, substrait_err,
};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::utils::split_conjunction_owned;
use datafusion::logical_expr::{
    Cast, EmptyRelation, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Values,
};
use std::sync::Arc;
use substrait::proto::expression::MaskExpression;
use substrait::proto::read_rel::ReadType;
use substrait::proto::read_rel::local_files::file_or_files::PathType::UriFile;
use substrait::proto::{Expression, ReadRel};
use url::Url;

#[expect(deprecated)]
pub async fn from_read_rel(
    consumer: &impl SubstraitConsumer,
    read: &ReadRel,
) -> datafusion::common::Result<LogicalPlan> {
    async fn read_with_schema(
        consumer: &impl SubstraitConsumer,
        table_ref: TableReference,
        schema: DFSchema,
        projection: &Option<MaskExpression>,
        filter: &Option<Box<Expression>>,
    ) -> datafusion::common::Result<LogicalPlan> {
        let schema = schema.replace_qualifier(table_ref.clone());

        let filters = if let Some(f) = filter {
            let filter_expr = consumer.consume_expression(f, &schema).await?;
            split_conjunction_owned(filter_expr)
        } else {
            vec![]
        };

        let provider = match consumer.resolve_table_ref(&table_ref).await? {
            Some(ref provider) => Arc::clone(provider),
            _ => return plan_err!("No table named '{table_ref}'"),
        };
        let unqualified_schema = schema.clone().strip_qualifiers();
        let scan_projection = if arrow_schema_matches_by_position(
            provider.schema().as_ref(),
            &unqualified_schema,
        ) {
            ensure_arrow_schema_compatibility_by_position(
                provider.schema().as_ref(),
                &unqualified_schema,
            )?;
            projection_indices(&schema, projection)?
        } else {
            None
        };
        let projected_schema = scan_projection
            .is_some()
            .then(|| apply_masking(schema.clone(), projection))
            .transpose()?;

        let plan = LogicalPlanBuilder::scan_with_filters(
            table_ref,
            provider_as_source(Arc::clone(&provider)),
            scan_projection,
            filters,
        )?
        .build()?;

        if let Some(projected_schema) = projected_schema {
            ensure_schema_compatibility_by_position(plan.schema(), &projected_schema)?;
            return Ok(plan);
        }

        let schema_matches_by_position =
            plan.schema().logically_equivalent_names_and_types(&schema);
        if schema_matches_by_position {
            ensure_schema_compatibility_by_position(plan.schema(), &schema)?;
        } else {
            if schema_has_duplicate_field_names(&schema) {
                return not_impl_err!(
                    "ReadRel schemas with duplicate field names must match by position"
                );
            }
            ensure_schema_compatibility(plan.schema(), schema.clone())?;
        }

        apply_read_rel_projection(
            plan,
            schema,
            projection,
            schema_matches_by_position,
            false,
        )
    }

    let named_struct = read.base_schema.as_ref().ok_or_else(|| {
        substrait_datafusion_err!("No base schema provided for Read Relation")
    })?;

    let substrait_schema = from_substrait_named_struct(consumer, named_struct)?;

    match &read.read_type {
        Some(ReadType::NamedTable(nt)) => {
            let table_reference = match nt.names.len() {
                0 => {
                    return plan_err!("No table name found in NamedTable");
                }
                1 => TableReference::Bare {
                    table: nt.names[0].clone().into(),
                },
                2 => TableReference::Partial {
                    schema: nt.names[0].clone().into(),
                    table: nt.names[1].clone().into(),
                },
                _ => TableReference::Full {
                    catalog: nt.names[0].clone().into(),
                    schema: nt.names[1].clone().into(),
                    table: nt.names[2].clone().into(),
                },
            };

            read_with_schema(
                consumer,
                table_reference,
                substrait_schema,
                &read.projection,
                &read.filter,
            )
            .await
        }
        Some(ReadType::VirtualTable(vt)) => {
            let plan = if vt.values.is_empty() && vt.expressions.is_empty() {
                LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: DFSchemaRef::new(substrait_schema.clone()),
                })
            } else {
                // Check for produce_one_row pattern in both old (values) and new (expressions) formats.
                // A VirtualTable with exactly one row containing only empty/default fields represents
                // an EmptyRelation with produce_one_row=true. This pattern is used for queries without
                // a FROM clause (e.g., "SELECT 1 AS one") where a single phantom row is needed to
                // provide a context for evaluating scalar expressions. This is conceptually similar to
                // the SQL "DUAL" table (see: https://en.wikipedia.org/wiki/DUAL_table) which some
                // databases provide as a single-row source for selecting constant expressions when no
                // real table is present.
                let is_produce_one_row = (vt.values.len() == 1
                    && vt.expressions.is_empty()
                    && substrait_schema.fields().is_empty()
                    && vt.values[0].fields.is_empty())
                    || (vt.expressions.len() == 1
                        && vt.values.is_empty()
                        && substrait_schema.fields().is_empty()
                        && vt.expressions[0].fields.is_empty());

                if is_produce_one_row {
                    LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: true,
                        schema: DFSchemaRef::new(substrait_schema.clone()),
                    })
                } else {
                    let values = if !vt.expressions.is_empty() {
                        let mut exprs = vec![];
                        for row in &vt.expressions {
                            let mut row_exprs = vec![];
                            for expression in &row.fields {
                                let expr = consumer
                                    .consume_expression(expression, &substrait_schema)
                                    .await?;
                                row_exprs.push(expr);
                            }
                            // For expressions, validate against top-level schema fields, not nested names
                            if row_exprs.len() != substrait_schema.fields().len() {
                                return substrait_err!(
                                    "Field count mismatch: expected {} fields but found {} in virtual table row",
                                    substrait_schema.fields().len(),
                                    row_exprs.len()
                                );
                            }
                            exprs.push(row_exprs);
                        }
                        exprs
                    } else {
                        convert_literal_rows(consumer, vt, named_struct)?
                    };

                    LogicalPlan::Values(Values {
                        schema: DFSchemaRef::new(substrait_schema.clone()),
                        values,
                    })
                }
            };

            let plan = normalize_values_to_schema(plan)?;

            apply_read_rel_filter_and_projection(
                consumer,
                plan,
                substrait_schema,
                &read.projection,
                &read.filter,
            )
            .await
        }
        Some(ReadType::LocalFiles(lf)) => {
            fn extract_filename(name: &str) -> Option<String> {
                let corrected_url =
                    if name.starts_with("file://") && !name.starts_with("file:///") {
                        name.replacen("file://", "file:///", 1)
                    } else {
                        name.to_string()
                    };

                Url::parse(&corrected_url).ok().and_then(|url| {
                    let path = url.path();
                    std::path::Path::new(path)
                        .file_name()
                        .map(|filename| filename.to_string_lossy().to_string())
                })
            }

            // we could use the file name to check the original table provider
            // TODO: currently does not support multiple local files
            let filename: Option<String> =
                lf.items.first().and_then(|x| match x.path_type.as_ref() {
                    Some(UriFile(name)) => extract_filename(name),
                    _ => None,
                });

            if lf.items.len() > 1 || filename.is_none() {
                return not_impl_err!("Only single file reads are supported");
            }
            let name = filename.unwrap();
            // directly use unwrap here since we could determine it is a valid one
            let table_reference = TableReference::Bare { table: name.into() };

            read_with_schema(
                consumer,
                table_reference,
                substrait_schema,
                &read.projection,
                &read.filter,
            )
            .await
        }
        _ => {
            not_impl_err!("Unsupported Readtype: {:?}", read.read_type)
        }
    }
}

async fn apply_read_rel_filter_and_projection(
    consumer: &impl SubstraitConsumer,
    plan: LogicalPlan,
    substrait_schema: DFSchema,
    projection: &Option<MaskExpression>,
    filter: &Option<Box<Expression>>,
) -> datafusion::common::Result<LogicalPlan> {
    let schema_matches_by_position = plan
        .schema()
        .logically_equivalent_names_and_types(&substrait_schema);

    if schema_matches_by_position {
        ensure_schema_compatibility_by_position(plan.schema(), &substrait_schema)?;
    } else {
        if schema_has_duplicate_field_names(&substrait_schema) {
            return not_impl_err!(
                "ReadRel schemas with duplicate field names must match by position"
            );
        }
        ensure_schema_compatibility(plan.schema(), substrait_schema.clone())?;
    }

    let filter_uses_ordinal_aliases = filter.is_some()
        && schema_matches_by_position
        && schema_has_duplicate_field_names(&substrait_schema);
    let (plan, filter_schema) = if filter_uses_ordinal_aliases {
        let filter_schema = ordinal_read_filter_schema(&substrait_schema)?;
        (
            apply_schema_by_position(plan, &filter_schema)?,
            filter_schema,
        )
    } else {
        (plan, substrait_schema.clone())
    };

    let plan = if let Some(f) = filter {
        let filter_expr = consumer.consume_expression(f, &filter_schema).await?;
        LogicalPlanBuilder::from(plan)
            .filter(filter_expr)?
            .build()?
    } else {
        plan
    };

    apply_read_rel_projection(
        plan,
        substrait_schema,
        projection,
        schema_matches_by_position,
        filter_uses_ordinal_aliases,
    )
}

fn apply_read_rel_projection(
    plan: LogicalPlan,
    substrait_schema: DFSchema,
    projection: &Option<MaskExpression>,
    schema_matches_by_position: bool,
    force_aliases: bool,
) -> datafusion::common::Result<LogicalPlan> {
    let projection_indices = if schema_matches_by_position {
        projection_indices(&substrait_schema, projection)?
    } else {
        None
    };
    let schema = apply_masking(substrait_schema, projection)?;
    if force_aliases && schema_has_duplicate_field_names(&schema) {
        return not_impl_err!(
            "ReadRel filters over duplicate field names require a projection with unique field names"
        );
    }

    match projection_indices {
        Some(column_indices) if force_aliases => {
            apply_projection_with_indices_and_aliases(plan, &schema, column_indices)
        }
        Some(column_indices) => {
            apply_projection_with_indices(plan, &schema, column_indices)
        }
        None if force_aliases => {
            let column_indices = (0..schema.fields().len()).collect();
            apply_projection_with_indices_and_aliases(plan, &schema, column_indices)
        }
        None => apply_projection(plan, &schema),
    }
}

fn ensure_schema_compatibility_by_position(
    table_schema: &DFSchema,
    substrait_schema: &DFSchema,
) -> datafusion::common::Result<()> {
    table_schema
        .fields()
        .iter()
        .zip(substrait_schema.fields())
        .try_for_each(|(datafusion_field, substrait_field)| {
            ensure_field_compatibility(datafusion_field, substrait_field)
        })
}

fn ensure_arrow_schema_compatibility_by_position(
    table_schema: &Schema,
    substrait_schema: &DFSchema,
) -> datafusion::common::Result<()> {
    if table_schema.fields().len() != substrait_schema.fields().len() {
        return substrait_err!(
            "ReadRel schema has {} fields but table schema has {} fields",
            substrait_schema.fields().len(),
            table_schema.fields().len()
        );
    }

    table_schema
        .fields()
        .iter()
        .zip(substrait_schema.fields())
        .try_for_each(|(datafusion_field, substrait_field)| {
            ensure_field_compatibility(datafusion_field.as_ref(), substrait_field)
        })
}

fn arrow_schema_matches_by_position(
    table_schema: &Schema,
    substrait_schema: &DFSchema,
) -> bool {
    if table_schema.fields().len() != substrait_schema.fields().len() {
        return false;
    }

    table_schema
        .fields()
        .iter()
        .zip(substrait_schema.fields())
        .all(|(table_field, substrait_field)| {
            table_field.name() == substrait_field.name()
                && DFSchema::datatype_is_logically_equal(
                    table_field.data_type(),
                    substrait_field.data_type(),
                )
        })
}

fn ordinal_read_filter_schema(schema: &DFSchema) -> datafusion::common::Result<DFSchema> {
    let fields = schema
        .iter()
        .enumerate()
        .map(|(index, (_qualifier, field))| {
            (
                Some(TableReference::Bare {
                    table: format!("__datafusion_substrait_read_{index}").into(),
                }),
                Arc::clone(field),
            )
        })
        .collect();

    DFSchema::new_with_metadata(fields, schema.metadata().clone())
}

fn apply_schema_by_position(
    plan: LogicalPlan,
    schema: &DFSchema,
) -> datafusion::common::Result<LogicalPlan> {
    match plan {
        LogicalPlan::Values(mut values) => {
            values.schema = DFSchemaRef::new(schema.clone());
            Ok(LogicalPlan::Values(values))
        }
        LogicalPlan::EmptyRelation(mut empty) => {
            empty.schema = DFSchemaRef::new(schema.clone());
            Ok(LogicalPlan::EmptyRelation(empty))
        }
        _ => not_impl_err!(
            "ReadRel filters over generic plans with duplicate field names are not supported"
        ),
    }
}

/// Converts Substrait literal rows from a VirtualTable into DataFusion expressions.
///
/// This function processes the deprecated `values` field of VirtualTable, converting
/// each literal value into a `Expr::Literal` while tracking and validating the name
/// indices against the provided named struct schema.
fn convert_literal_rows(
    consumer: &impl SubstraitConsumer,
    vt: &substrait::proto::read_rel::VirtualTable,
    named_struct: &substrait::proto::NamedStruct,
) -> datafusion::common::Result<Vec<Vec<Expr>>> {
    #[expect(deprecated)]
    vt.values
        .iter()
        .map(|row| {
            let mut name_idx = 0;
            let lits = row
                .fields
                .iter()
                .map(|lit| {
                    name_idx += 1; // top-level names are provided through schema
                    Ok(Expr::Literal(from_substrait_literal(
                        consumer,
                        lit,
                        &named_struct.names,
                        &mut name_idx,
                    )?, None))
                })
                .collect::<datafusion::common::Result<_>>()?;
            if name_idx != named_struct.names.len() {
                return substrait_err!(
                    "Names list must match exactly to nested schema, but found {} uses for {} names",
                    name_idx,
                    named_struct.names.len()
                );
            }
            Ok(lits)
        })
        .collect::<datafusion::common::Result<_>>()
}

/// Applies a top-level `ReadRel` projection mask to a schema.
///
/// Nested masks are not supported and return an error. The
/// `maintain_singular_struct` flag does not affect top-level field selection.
/// Projection field indexes must refer to existing top-level fields.
pub fn apply_masking(
    schema: DFSchema,
    mask_expression: &::core::option::Option<MaskExpression>,
) -> datafusion::common::Result<DFSchema> {
    let Some(column_indices) = projection_indices(&schema, mask_expression)? else {
        return Ok(schema);
    };

    let fields = column_indices
        .iter()
        .map(|i| schema.qualified_field(*i))
        .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
        .collect();

    DFSchema::new_with_metadata(fields, schema.metadata().clone())
}

fn projection_indices(
    schema: &DFSchema,
    mask_expression: &::core::option::Option<MaskExpression>,
) -> datafusion::common::Result<Option<Vec<usize>>> {
    match mask_expression {
        Some(MaskExpression { select, .. }) => match &select.as_ref() {
            Some(projection) => projection
                .struct_items
                .iter()
                .map(|item| {
                    if item.child.is_some() {
                        return not_impl_err!(
                            "Nested ReadRel projections are not supported"
                        );
                    }
                    projection_field_index(schema, item.field)
                })
                .collect::<datafusion::common::Result<_>>()
                .map(Some),
            None => Ok(None),
        },
        None => Ok(None),
    }
}

/// This function returns a DataFrame with fields adjusted if necessary in the event that the
/// Substrait schema is a subset of the DataFusion schema.
fn apply_projection(
    plan: LogicalPlan,
    substrait_schema: &DFSchema,
) -> datafusion::common::Result<LogicalPlan> {
    let df_schema = plan.schema();

    if df_schema.logically_equivalent_names_and_types(substrait_schema) {
        return Ok(plan);
    }

    let df_schema = df_schema.to_owned();

    let column_indices: Vec<usize> = substrait_schema
        .clone()
        .strip_qualifiers()
        .fields()
        .iter()
        .map(|substrait_field| {
            unqualified_field_index(&df_schema, substrait_field.name().as_str())
        })
        .collect::<datafusion::common::Result<_>>()?;

    apply_projection_with_indices(plan, substrait_schema, column_indices)
}

fn cast_value_expressions(
    exprs: Vec<Expr>,
    input_schema: &DFSchema,
    fields: &[Arc<Field>],
) -> datafusion::common::Result<Vec<Expr>> {
    if exprs.len() != fields.len() {
        return plan_err!(
            "ReadRel Values projection has {} expressions for {} fields",
            exprs.len(),
            fields.len()
        );
    }

    exprs
        .into_iter()
        .zip(fields)
        .map(|(expr, field)| {
            if &expr.get_type(input_schema)? != field.data_type() {
                Ok(Expr::Cast(Cast::new(
                    Box::new(expr),
                    field.data_type().to_owned(),
                )))
            } else {
                Ok(expr)
            }
        })
        .collect()
}

fn normalize_values_to_schema(
    plan: LogicalPlan,
) -> datafusion::common::Result<LogicalPlan> {
    match plan {
        LogicalPlan::Values(mut values) => {
            let schema = Arc::clone(&values.schema);
            values.values = values
                .values
                .into_iter()
                .map(|exprs| cast_value_expressions(exprs, &schema, schema.fields()))
                .collect::<datafusion::common::Result<_>>()?;
            Ok(LogicalPlan::Values(values))
        }
        _ => Ok(plan),
    }
}

fn apply_projection_with_indices(
    plan: LogicalPlan,
    substrait_schema: &DFSchema,
    column_indices: Vec<usize>,
) -> datafusion::common::Result<LogicalPlan> {
    apply_projection_with_indices_inner(plan, substrait_schema, column_indices, false)
}

fn apply_projection_with_indices_and_aliases(
    plan: LogicalPlan,
    substrait_schema: &DFSchema,
    column_indices: Vec<usize>,
) -> datafusion::common::Result<LogicalPlan> {
    apply_projection_with_indices_inner(plan, substrait_schema, column_indices, true)
}

fn apply_projection_with_indices_inner(
    plan: LogicalPlan,
    substrait_schema: &DFSchema,
    column_indices: Vec<usize>,
    force_aliases: bool,
) -> datafusion::common::Result<LogicalPlan> {
    let df_schema = plan.schema().to_owned();

    match plan {
        LogicalPlan::TableScan(mut scan) => {
            let fields = column_indices
                .iter()
                .map(|i| df_schema.qualified_field(*i))
                .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
                .collect();

            let scan_projection = match scan.projection.as_ref() {
                Some(existing) => Some(
                    column_indices
                        .iter()
                        .map(|i| {
                            existing.get(*i).copied().ok_or_else(|| {
                                plan_datafusion_err!(
                                    "ReadRel projection field index {i} is out of bounds for the TableScan projection"
                                )
                            })
                        })
                        .collect::<datafusion::common::Result<_>>()?,
                ),
                None => Some(column_indices),
            };

            scan.projected_schema = DFSchemaRef::new(DFSchema::new_with_metadata(
                fields,
                df_schema.metadata().clone(),
            )?);
            scan.projection = scan_projection;

            Ok(LogicalPlan::TableScan(scan))
        }
        LogicalPlan::Values(mut values) => {
            values.values = values
                .values
                .into_iter()
                .map(|row| {
                    column_indices
                        .iter()
                        .map(|i| {
                            row.get(*i).cloned().ok_or_else(|| {
                                plan_datafusion_err!(
                                    "ReadRel projection field index {i} is out of bounds for a Values row with {} fields",
                                    row.len()
                                )
                            })
                        })
                        .collect::<datafusion::common::Result<_>>()
                })
                .map(|exprs| {
                    cast_value_expressions(exprs?, &df_schema, substrait_schema.fields())
                })
                .collect::<datafusion::common::Result<_>>()?;
            values.schema = DFSchemaRef::new(substrait_schema.clone());

            Ok(LogicalPlan::Values(values))
        }
        LogicalPlan::EmptyRelation(mut empty) => {
            empty.schema = DFSchemaRef::new(substrait_schema.clone());

            Ok(LogicalPlan::EmptyRelation(empty))
        }
        _ => {
            if schema_has_duplicate_field_names(&df_schema) {
                return not_impl_err!(
                    "ReadRel projections over generic plans with duplicate field names are not supported"
                );
            }

            let exprs: Vec<Expr> = column_indices
                .iter()
                .map(|i| Expr::Column(Column::from(df_schema.qualified_field(*i))))
                .collect();
            let exprs = rename_expressions(exprs, &df_schema, substrait_schema.fields())?;
            let exprs = if force_aliases {
                exprs
                    .into_iter()
                    .zip(substrait_schema.fields())
                    .map(|(expr, field)| expr.alias(field.name().to_owned()))
                    .collect()
            } else {
                exprs
            };

            LogicalPlanBuilder::from(plan).project(exprs)?.build()
        }
    }
}

fn projection_field_index(
    schema: &DFSchema,
    field: i32,
) -> datafusion::common::Result<usize> {
    let index = usize::try_from(field).map_err(|_| {
        substrait_datafusion_err!("Invalid ReadRel projection field index: {field}")
    })?;

    if index >= schema.fields().len() {
        return substrait_err!(
            "ReadRel projection field index {} is out of bounds for schema with {} fields",
            field,
            schema.fields().len()
        );
    }

    Ok(index)
}

fn schema_has_duplicate_field_names(schema: &DFSchema) -> bool {
    schema
        .iter()
        .enumerate()
        .any(|(left_index, (left_qualifier, left_field))| {
            schema
                .iter()
                .skip(left_index + 1)
                .any(|(right_qualifier, right_field)| {
                    left_qualifier == right_qualifier
                        && left_field.name() == right_field.name()
                })
        })
}

fn unqualified_field_index(
    schema: &DFSchema,
    name: &str,
) -> datafusion::common::Result<usize> {
    let matches = schema
        .iter()
        .enumerate()
        .filter(|(_, (_, field))| field.name() == name)
        .map(|(index, (qualifier, _))| (index, qualifier))
        .collect::<Vec<_>>();

    match matches.len() {
        0 => Err(plan_datafusion_err!(
            "No field named '{name}' found in ReadRel input schema"
        )),
        1 => Ok(matches[0].0),
        _ => {
            let unqualified_matches = matches
                .iter()
                .filter(|(_, qualifier)| qualifier.is_none())
                .collect::<Vec<_>>();

            if unqualified_matches.len() == 1 {
                Ok(unqualified_matches[0].0)
            } else {
                Err(plan_datafusion_err!(
                    "Ambiguous field name '{name}' found in ReadRel input schema"
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::Extensions;
    use crate::logical_plan::consumer::utils::tests::{
        TEST_EXTENSIONS, TEST_SESSION_STATE, test_consumer,
    };
    use crate::variation_const::DEFAULT_TYPE_VARIATION_REF;
    use async_trait::async_trait;
    use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::TableProvider;
    use datafusion::common::ScalarValue;
    use datafusion::datasource::MemTable;
    use datafusion::execution::FunctionRegistry;
    use datafusion::logical_expr::Projection;
    use datafusion::prelude::SessionContext;
    use std::ops::Deref;
    use substrait::proto::expression::literal::{LiteralType, Struct as LiteralStruct};
    use substrait::proto::expression::mask_expression::{StructItem, StructSelect};
    use substrait::proto::expression::nested::Struct as ExpressionStruct;
    use substrait::proto::expression::{Literal, RexType};
    use substrait::proto::{NamedStruct, Type, read_rel, r#type};
    struct TableConsumer {
        table: Arc<dyn TableProvider>,
    }

    impl TableConsumer {
        fn new() -> datafusion::common::Result<Self> {
            Self::new_with_bool_columns(&["a"], &[vec![true, false]])
        }

        fn new_with_bool_columns(
            names: &[&str],
            columns: &[Vec<bool>],
        ) -> datafusion::common::Result<Self> {
            let schema = Arc::new(Schema::new(
                names
                    .iter()
                    .map(|name| Field::new(*name, DataType::Boolean, true))
                    .collect::<Vec<_>>(),
            ));
            let columns = columns
                .iter()
                .map(|values| Arc::new(BooleanArray::from(values.clone())) as ArrayRef)
                .collect();
            let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
            let table: Arc<dyn TableProvider> =
                Arc::new(MemTable::try_new(schema, vec![vec![batch]])?);

            Ok(Self { table })
        }
    }

    #[async_trait]
    impl SubstraitConsumer for TableConsumer {
        async fn resolve_table_ref(
            &self,
            _table_ref: &TableReference,
        ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
            Ok(Some(Arc::clone(&self.table)))
        }

        fn get_extensions(&self) -> &Extensions {
            TEST_EXTENSIONS.deref()
        }

        fn get_function_registry(&self) -> &impl FunctionRegistry {
            TEST_SESSION_STATE.deref()
        }
    }

    fn named_table_read_with_bool_fields(table: &str, names: &[&str]) -> ReadRel {
        let names = names
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>();
        let field_count = names.len();

        ReadRel {
            base_schema: Some(NamedStruct {
                names,
                r#struct: Some(r#type::Struct {
                    types: (0..field_count).map(|_| nullable_bool()).collect(),
                    type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                    nullability: r#type::Nullability::Required as i32,
                }),
            }),
            read_type: Some(ReadType::NamedTable(read_rel::NamedTable {
                names: vec![table.to_string()],
                advanced_extension: None,
            })),
            ..Default::default()
        }
    }

    fn local_files_read_with_bool_fields(file_name: &str, names: &[&str]) -> ReadRel {
        let names = names
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>();
        let field_count = names.len();

        ReadRel {
            base_schema: Some(NamedStruct {
                names,
                r#struct: Some(r#type::Struct {
                    types: (0..field_count).map(|_| nullable_bool()).collect(),
                    type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                    nullability: r#type::Nullability::Required as i32,
                }),
            }),
            read_type: Some(ReadType::LocalFiles(read_rel::LocalFiles {
                items: vec![read_rel::local_files::FileOrFiles {
                    path_type: Some(UriFile(format!("file:///{file_name}"))),
                    ..Default::default()
                }],
                advanced_extension: None,
            })),
            ..Default::default()
        }
    }

    fn virtual_table_read(names: &[&str]) -> ReadRel {
        let names = names
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>();
        let field_count = names.len();

        ReadRel {
            base_schema: Some(NamedStruct {
                names,
                r#struct: Some(r#type::Struct {
                    types: (0..field_count).map(|_| nullable_i64()).collect(),
                    type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                    nullability: r#type::Nullability::Required as i32,
                }),
            }),
            read_type: Some(ReadType::VirtualTable(read_rel::VirtualTable {
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    #[expect(deprecated)]
    fn virtual_table_read_with_bool_values(
        names: &[&str],
        rows: &[[bool; 2]],
    ) -> ReadRel {
        let names = names
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>();
        let field_count = names.len();

        ReadRel {
            base_schema: Some(NamedStruct {
                names,
                r#struct: Some(r#type::Struct {
                    types: (0..field_count).map(|_| nullable_bool()).collect(),
                    type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                    nullability: r#type::Nullability::Required as i32,
                }),
            }),
            read_type: Some(ReadType::VirtualTable(read_rel::VirtualTable {
                values: rows
                    .iter()
                    .map(|row| LiteralStruct {
                        fields: row.iter().map(|value| bool_literal(*value)).collect(),
                    })
                    .collect(),
                expressions: vec![],
            })),
            ..Default::default()
        }
    }

    #[expect(deprecated)]
    fn virtual_table_read_with_i32_values_declared_i64(
        name: &str,
        rows: &[i32],
    ) -> ReadRel {
        ReadRel {
            base_schema: Some(NamedStruct {
                names: vec![name.to_string()],
                r#struct: Some(r#type::Struct {
                    types: vec![nullable_i64()],
                    type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                    nullability: r#type::Nullability::Required as i32,
                }),
            }),
            read_type: Some(ReadType::VirtualTable(read_rel::VirtualTable {
                values: rows
                    .iter()
                    .map(|value| LiteralStruct {
                        fields: vec![i32_literal(*value)],
                    })
                    .collect(),
                expressions: vec![],
            })),
            ..Default::default()
        }
    }

    #[expect(deprecated)]
    fn virtual_table_read_with_i32_expressions_declared_i64(
        name: &str,
        rows: &[i32],
    ) -> ReadRel {
        ReadRel {
            base_schema: Some(NamedStruct {
                names: vec![name.to_string()],
                r#struct: Some(r#type::Struct {
                    types: vec![nullable_i64()],
                    type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                    nullability: r#type::Nullability::Required as i32,
                }),
            }),
            read_type: Some(ReadType::VirtualTable(read_rel::VirtualTable {
                values: vec![],
                expressions: rows
                    .iter()
                    .map(|value| ExpressionStruct {
                        fields: vec![i32_expression(*value)],
                    })
                    .collect(),
            })),
            ..Default::default()
        }
    }

    #[expect(deprecated)]
    fn virtual_table_read_with_bool_i32_values_declared_bool_i64(
        rows: &[(bool, i32)],
    ) -> ReadRel {
        ReadRel {
            base_schema: Some(NamedStruct {
                names: vec!["keep".to_string(), "value".to_string()],
                r#struct: Some(r#type::Struct {
                    types: vec![nullable_bool(), nullable_i64()],
                    type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                    nullability: r#type::Nullability::Required as i32,
                }),
            }),
            read_type: Some(ReadType::VirtualTable(read_rel::VirtualTable {
                values: rows
                    .iter()
                    .map(|(keep, value)| LiteralStruct {
                        fields: vec![bool_literal(*keep), i32_literal(*value)],
                    })
                    .collect(),
                expressions: vec![],
            })),
            ..Default::default()
        }
    }

    #[expect(deprecated)]
    fn virtual_table_read_with_bool_expressions(
        names: &[&str],
        rows: &[[bool; 2]],
    ) -> ReadRel {
        let names = names
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>();
        let field_count = names.len();

        ReadRel {
            base_schema: Some(NamedStruct {
                names,
                r#struct: Some(r#type::Struct {
                    types: (0..field_count).map(|_| nullable_bool()).collect(),
                    type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                    nullability: r#type::Nullability::Required as i32,
                }),
            }),
            read_type: Some(ReadType::VirtualTable(read_rel::VirtualTable {
                values: vec![],
                expressions: rows
                    .iter()
                    .map(|row| ExpressionStruct {
                        fields: row.iter().map(|value| bool_expression(*value)).collect(),
                    })
                    .collect(),
            })),
            ..Default::default()
        }
    }

    #[expect(deprecated)]
    fn virtual_table_read_produce_one_row() -> ReadRel {
        ReadRel {
            base_schema: Some(NamedStruct {
                names: vec![],
                r#struct: Some(r#type::Struct {
                    types: vec![],
                    type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                    nullability: r#type::Nullability::Required as i32,
                }),
            }),
            read_type: Some(ReadType::VirtualTable(read_rel::VirtualTable {
                values: vec![LiteralStruct { fields: vec![] }],
                expressions: vec![],
            })),
            ..Default::default()
        }
    }

    #[expect(deprecated)]
    fn virtual_table_expression_read_produce_one_row() -> ReadRel {
        ReadRel {
            base_schema: Some(NamedStruct {
                names: vec![],
                r#struct: Some(r#type::Struct {
                    types: vec![],
                    type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                    nullability: r#type::Nullability::Required as i32,
                }),
            }),
            read_type: Some(ReadType::VirtualTable(read_rel::VirtualTable {
                values: vec![],
                expressions: vec![ExpressionStruct { fields: vec![] }],
            })),
            ..Default::default()
        }
    }

    fn nullable_i64() -> Type {
        Type {
            kind: Some(r#type::Kind::I64(r#type::I64 {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability: r#type::Nullability::Nullable as i32,
            })),
        }
    }

    fn nullable_bool() -> Type {
        Type {
            kind: Some(r#type::Kind::Bool(r#type::Boolean {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability: r#type::Nullability::Nullable as i32,
            })),
        }
    }

    fn required_bool() -> Type {
        Type {
            kind: Some(r#type::Kind::Bool(r#type::Boolean {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability: r#type::Nullability::Required as i32,
            })),
        }
    }

    fn projection(fields: &[i32]) -> MaskExpression {
        MaskExpression {
            select: Some(StructSelect {
                struct_items: fields
                    .iter()
                    .map(|field| StructItem {
                        field: *field,
                        child: None,
                    })
                    .collect(),
            }),
            maintain_singular_struct: false,
        }
    }

    fn nested_projection(field: i32) -> MaskExpression {
        MaskExpression {
            select: Some(StructSelect {
                struct_items: vec![StructItem {
                    field,
                    child: Some(substrait::proto::expression::mask_expression::Select {
                        r#type: None,
                    }),
                }],
            }),
            maintain_singular_struct: false,
        }
    }

    fn maintain_singular_projection(fields: &[i32]) -> MaskExpression {
        MaskExpression {
            maintain_singular_struct: true,
            ..projection(fields)
        }
    }

    fn bool_expression(value: bool) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(bool_literal(value))),
        }
    }

    fn i32_expression(value: i32) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(i32_literal(value))),
        }
    }

    fn true_filter() -> Expression {
        bool_expression(true)
    }

    fn false_filter() -> Expression {
        bool_expression(false)
    }

    fn bool_literal(value: bool) -> Literal {
        Literal {
            nullable: false,
            type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
            literal_type: Some(LiteralType::Boolean(value)),
        }
    }

    fn i32_literal(value: i32) -> Literal {
        Literal {
            nullable: false,
            type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
            literal_type: Some(LiteralType::I32(value)),
        }
    }

    fn assert_projection_columns(projection: &Projection, expected: &[&str]) {
        let actual = projection
            .expr
            .iter()
            .map(|expr| match expr {
                Expr::Column(column) => column.name.clone(),
                _ => panic!("expected column projection, got {expr:?}"),
            })
            .collect::<Vec<_>>();
        let expected = expected
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    async fn assert_bool_column(plan: LogicalPlan, expected: &[Option<bool>]) {
        let batches = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let actual = batches
            .iter()
            .flat_map(|batch| {
                assert_eq!(batch.num_columns(), 1);
                let array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                (0..array.len())
                    .map(|row| {
                        if array.is_null(row) {
                            None
                        } else {
                            Some(array.value(row))
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    async fn assert_bool_rows(plan: LogicalPlan, expected: &[Vec<Option<bool>>]) {
        let batches = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let actual = batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows())
                    .map(|row| {
                        (0..batch.num_columns())
                            .map(|column| {
                                let array = batch
                                    .column(column)
                                    .as_any()
                                    .downcast_ref::<BooleanArray>()
                                    .unwrap();
                                if array.is_null(row) {
                                    None
                                } else {
                                    Some(array.value(row))
                                }
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    async fn assert_i64_column(plan: LogicalPlan, expected: &[Option<i64>]) {
        let batches = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let actual = batches
            .iter()
            .flat_map(|batch| {
                assert_eq!(batch.num_columns(), 1);
                let array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (0..array.len())
                    .map(|row| {
                        if array.is_null(row) {
                            None
                        } else {
                            Some(array.value(row))
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    async fn assert_row_count(plan: LogicalPlan, expected: usize) {
        let batches = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let actual = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(actual, expected);
    }

    fn field_reference_filter(field: i32) -> Expression {
        Expression {
            rex_type: Some(RexType::Selection(Box::new(
                substrait::proto::expression::FieldReference {
                    reference_type: Some(
                        substrait::proto::expression::field_reference::ReferenceType::DirectReference(
                            substrait::proto::expression::ReferenceSegment {
                                reference_type: Some(
                                    substrait::proto::expression::reference_segment::ReferenceType::StructField(
                                        Box::new(
                                            substrait::proto::expression::reference_segment::StructField {
                                                field,
                                                child: None,
                                            },
                                        ),
                                    ),
                                ),
                            },
                        ),
                    ),
                    root_type: Some(
                        substrait::proto::expression::field_reference::RootType::RootReference(
                            substrait::proto::expression::field_reference::RootReference {},
                        ),
                    ),
                },
            ))),
        }
    }

    fn bool_schema(names: &[&str]) -> DFSchema {
        DFSchema::new_with_metadata(
            names
                .iter()
                .map(|name| (None, Arc::new(Field::new(*name, DataType::Boolean, true))))
                .collect(),
            Default::default(),
        )
        .unwrap()
    }

    fn assert_unqualified_schema(schema: &DFSchema, expected: &[&str]) {
        let actual = schema
            .iter()
            .map(|(qualifier, field)| {
                assert!(qualifier.is_none(), "unexpected qualifier: {qualifier:?}");
                field.name().as_str()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn apply_masking_projects_fields() {
        let schema = bool_schema(&["a", "b"]);

        let schema = apply_masking(schema, &Some(projection(&[1, 0]))).unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.fields()[0].name(), "b");
        assert_eq!(schema.fields()[1].name(), "a");
    }

    #[test]
    fn apply_masking_projects_fields_with_maintain_singular_struct() {
        let schema = bool_schema(&["a", "b"]);

        let schema =
            apply_masking(schema, &Some(maintain_singular_projection(&[1, 0]))).unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.fields()[0].name(), "b");
        assert_eq!(schema.fields()[1].name(), "a");
    }

    #[test]
    fn apply_masking_rejects_unsupported_masks() {
        let schema = bool_schema(&["a"]);

        for (projection, expected) in [
            (
                projection(&[-1]),
                "Invalid ReadRel projection field index: -1",
            ),
            (
                projection(&[1]),
                "ReadRel projection field index 1 is out of bounds",
            ),
            (
                nested_projection(0),
                "Nested ReadRel projections are not supported",
            ),
        ] {
            let err = apply_masking(schema.clone(), &Some(projection)).unwrap_err();
            assert!(err.to_string().contains(expected), "got: {err}");
        }
    }

    async fn assert_read_rejects_unsupported_projection_masks(read: ReadRel) {
        let consumer = TableConsumer::new().unwrap();

        for (projection, expected) in [
            (
                projection(&[-1]),
                "Invalid ReadRel projection field index: -1",
            ),
            (
                projection(&[1]),
                "ReadRel projection field index 1 is out of bounds",
            ),
            (
                nested_projection(0),
                "Nested ReadRel projections are not supported",
            ),
        ] {
            let mut read = read.clone();
            read.projection = Some(projection);

            let err = from_read_rel(&consumer, &read).await.unwrap_err();
            assert!(err.to_string().contains(expected), "got: {err}");
        }
    }

    #[tokio::test]
    async fn named_table_read_rejects_unsupported_projection_masks() {
        assert_read_rejects_unsupported_projection_masks(
            named_table_read_with_bool_fields("source", &["a"]),
        )
        .await;
    }

    #[tokio::test]
    async fn local_files_read_rejects_unsupported_projection_masks() {
        assert_read_rejects_unsupported_projection_masks(
            local_files_read_with_bool_fields("source", &["a"]),
        )
        .await;
    }

    async fn assert_read_projects_reordered_table_scan(mut read: ReadRel) {
        let consumer = TableConsumer::new_with_bool_columns(
            &["a", "b"],
            &[vec![true, false], vec![false, true]],
        )
        .unwrap();
        read.projection = Some(projection(&[1, 0]));

        let plan = from_read_rel(&consumer, &read).await.unwrap();

        assert_bool_rows(
            plan,
            &[vec![Some(false), Some(true)], vec![Some(true), Some(false)]],
        )
        .await;
    }

    #[tokio::test]
    async fn named_table_read_projects_reordered_table_scan() {
        assert_read_projects_reordered_table_scan(named_table_read_with_bool_fields(
            "source",
            &["a", "b"],
        ))
        .await;
    }

    #[tokio::test]
    async fn local_files_read_projects_reordered_table_scan() {
        assert_read_projects_reordered_table_scan(local_files_read_with_bool_fields(
            "source",
            &["a", "b"],
        ))
        .await;
    }

    async fn assert_read_projects_duplicate_table_scan_column(mut read: ReadRel) {
        let consumer = TableConsumer::new_with_bool_columns(
            &["a", "a"],
            &[vec![true, false], vec![false, true]],
        )
        .unwrap();
        read.projection = Some(projection(&[1]));

        let plan = from_read_rel(&consumer, &read).await.unwrap();

        let LogicalPlan::TableScan(scan) = &plan else {
            panic!("expected TableScan, got {plan:?}");
        };
        assert_eq!(scan.projection.as_deref(), Some(&[1][..]));
        assert_eq!(scan.projected_schema.fields().len(), 1);
        assert_eq!(scan.projected_schema.fields()[0].name(), "a");
    }

    #[tokio::test]
    async fn named_table_read_projects_duplicate_table_scan_column() {
        assert_read_projects_duplicate_table_scan_column(
            named_table_read_with_bool_fields("source", &["a", "a"]),
        )
        .await;
    }

    #[tokio::test]
    async fn local_files_read_projects_duplicate_table_scan_column() {
        assert_read_projects_duplicate_table_scan_column(
            local_files_read_with_bool_fields("source", &["a", "a"]),
        )
        .await;
    }

    async fn assert_read_rejects_incompatible_omitted_projection_field(
        mut read: ReadRel,
    ) {
        let consumer = TableConsumer::new_with_bool_columns(
            &["a", "b"],
            &[vec![true, false], vec![false, true]],
        )
        .unwrap();
        read.base_schema
            .as_mut()
            .unwrap()
            .r#struct
            .as_mut()
            .unwrap()
            .types[0] = required_bool();
        read.filter = Some(Box::new(field_reference_filter(0)));
        read.projection = Some(projection(&[1]));

        let err = from_read_rel(&consumer, &read).await.unwrap_err();

        assert!(
            err.to_string().contains(
                "Field 'a' is nullable in the DataFusion schema but not nullable in the Substrait schema"
            ),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn named_table_read_rejects_incompatible_omitted_projection_field() {
        assert_read_rejects_incompatible_omitted_projection_field(
            named_table_read_with_bool_fields("source", &["a", "b"]),
        )
        .await;
    }

    #[tokio::test]
    async fn local_files_read_rejects_incompatible_omitted_projection_field() {
        assert_read_rejects_incompatible_omitted_projection_field(
            local_files_read_with_bool_fields("source", &["a", "b"]),
        )
        .await;
    }

    #[tokio::test]
    async fn local_files_read_projects_with_maintain_singular_struct() {
        let consumer = TableConsumer::new_with_bool_columns(
            &["a", "b"],
            &[vec![true, false], vec![false, true]],
        )
        .unwrap();
        let mut read = local_files_read_with_bool_fields("source", &["a", "b"]);
        read.projection = Some(maintain_singular_projection(&[1, 0]));

        let plan = from_read_rel(&consumer, &read).await.unwrap();

        assert_bool_rows(
            plan,
            &[vec![Some(false), Some(true)], vec![Some(true), Some(false)]],
        )
        .await;
    }

    #[tokio::test]
    async fn virtual_table_read_applies_filter_and_projection() {
        let consumer = test_consumer();
        let mut read = virtual_table_read(&["a", "b"]);
        read.filter = Some(Box::new(true_filter()));
        read.projection = Some(projection(&[0]));

        let plan = from_read_rel(&consumer, &read).await.unwrap();

        assert_eq!(plan.schema().fields().len(), 1);
        assert_eq!(plan.schema().fields()[0].name(), "a");
        let LogicalPlan::Projection(projection) = plan else {
            panic!("expected Projection, got {plan:?}");
        };
        assert_projection_columns(&projection, &["a"]);
        assert!(matches!(projection.input.as_ref(), LogicalPlan::Filter(_)));
    }

    #[tokio::test]
    async fn virtual_table_values_read_applies_filter_and_projection() {
        let consumer = test_consumer();
        let mut read = virtual_table_read_with_bool_values(
            &["a", "b"],
            &[[true, false], [false, true]],
        );
        read.filter = Some(Box::new(field_reference_filter(0)));
        read.projection = Some(projection(&[1]));

        let plan = from_read_rel(&consumer, &read).await.unwrap();

        assert_eq!(plan.schema().fields().len(), 1);
        assert_eq!(plan.schema().fields()[0].name(), "b");
        let LogicalPlan::Projection(projection) = &plan else {
            panic!("expected Projection, got {plan:?}");
        };
        assert_projection_columns(projection, &["b"]);
        let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
            panic!("expected Filter, got {:?}", projection.input);
        };
        let Expr::Column(column) = &filter.predicate else {
            panic!("expected column filter, got {:?}", filter.predicate);
        };
        assert_eq!(column.name, "a");
        let LogicalPlan::Values(values) = filter.input.as_ref() else {
            panic!("expected Values, got {:?}", filter.input);
        };
        assert_eq!(values.values.len(), 2);
        assert_bool_column(plan, &[Some(false)]).await;
    }

    #[tokio::test]
    async fn virtual_table_projection_casts_values_to_declared_schema() {
        let consumer = test_consumer();
        let mut read = virtual_table_read_with_i32_values_declared_i64("a", &[25, 100]);
        read.projection = Some(projection(&[0]));

        let plan = from_read_rel(&consumer, &read).await.unwrap();

        assert_eq!(plan.schema().fields()[0].data_type(), &DataType::Int64);
        assert_i64_column(plan, &[Some(25), Some(100)]).await;
    }

    #[tokio::test]
    async fn virtual_table_expressions_cast_to_declared_schema() {
        let consumer = test_consumer();
        let read = virtual_table_read_with_i32_expressions_declared_i64("a", &[25, 100]);

        let plan = from_read_rel(&consumer, &read).await.unwrap();

        assert_eq!(plan.schema().fields()[0].data_type(), &DataType::Int64);
        assert_i64_column(plan, &[Some(25), Some(100)]).await;
    }

    #[tokio::test]
    async fn virtual_table_filter_casts_values_to_declared_schema() {
        let consumer = test_consumer();
        let mut read = virtual_table_read_with_bool_i32_values_declared_bool_i64(&[
            (true, 25),
            (false, 100),
        ]);
        read.filter = Some(Box::new(field_reference_filter(0)));
        read.projection = Some(projection(&[1]));

        let plan = from_read_rel(&consumer, &read).await.unwrap();

        assert_eq!(plan.schema().fields()[0].data_type(), &DataType::Int64);
        assert_i64_column(plan, &[Some(25)]).await;
    }

    #[tokio::test]
    async fn virtual_table_read_allows_duplicate_names_without_filter_or_projection() {
        let consumer = test_consumer();
        let read = virtual_table_read_with_bool_values(&["a", "a"], &[[true, false]]);

        let plan = from_read_rel(&consumer, &read).await.unwrap();

        assert_eq!(plan.schema().fields().len(), 2);
        assert_eq!(plan.schema().fields()[0].name(), "a");
        assert_eq!(plan.schema().fields()[1].name(), "a");
        let LogicalPlan::Values(values) = plan else {
            panic!("expected Values, got {plan:?}");
        };
        assert_eq!(values.values.len(), 1);
    }

    #[tokio::test]
    async fn virtual_table_filter_rejects_duplicate_output_names() {
        let consumer = test_consumer();
        let mut read = virtual_table_read_with_bool_values(
            &["a", "a"],
            &[[true, false], [true, true]],
        );
        read.filter = Some(Box::new(field_reference_filter(1)));

        let err = from_read_rel(&consumer, &read).await.unwrap_err();

        assert!(
            err.to_string().contains(
                "ReadRel filters over duplicate field names require a projection with unique field names"
            ),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn virtual_table_filter_and_projection_use_ordinals_with_duplicate_names() {
        let consumer = test_consumer();
        let mut read = virtual_table_read_with_bool_values(
            &["a", "a"],
            &[[false, true], [true, false]],
        );
        read.filter = Some(Box::new(field_reference_filter(1)));
        read.projection = Some(projection(&[0]));

        let plan = from_read_rel(&consumer, &read).await.unwrap();

        assert_unqualified_schema(plan.schema(), &["a"]);
        assert_bool_column(plan, &[Some(false)]).await;
    }

    #[tokio::test]
    async fn virtual_table_empty_filter_and_projection_use_ordinals_with_duplicate_names()
    {
        let consumer = test_consumer();
        let mut read = virtual_table_read_with_bool_values(&["a", "a"], &[]);
        read.filter = Some(Box::new(field_reference_filter(1)));
        read.projection = Some(projection(&[0]));

        let plan = from_read_rel(&consumer, &read).await.unwrap();

        assert_unqualified_schema(plan.schema(), &["a"]);
        assert_row_count(plan, 0).await;
    }

    #[tokio::test]
    async fn virtual_table_projection_uses_ordinal_with_duplicate_names() {
        let consumer = test_consumer();
        let mut read = virtual_table_read_with_bool_values(&["a", "a"], &[[true, false]]);
        read.projection = Some(projection(&[1]));

        let plan = from_read_rel(&consumer, &read).await.unwrap();

        assert_eq!(plan.schema().fields().len(), 1);
        assert_eq!(plan.schema().fields()[0].name(), "a");
        assert_bool_column(plan, &[Some(false)]).await;
    }

    #[tokio::test]
    async fn virtual_table_empty_projection_uses_ordinal_with_duplicate_names() {
        let consumer = test_consumer();
        let mut read = virtual_table_read_with_bool_values(&["a", "a"], &[]);
        read.projection = Some(projection(&[1]));

        let plan = from_read_rel(&consumer, &read).await.unwrap();

        assert_unqualified_schema(plan.schema(), &["a"]);
        assert_row_count(plan, 0).await;
    }

    #[tokio::test]
    async fn virtual_table_projection_reorders_values_by_ordinal() {
        let consumer = test_consumer();
        let mut read = virtual_table_read_with_bool_values(&["a", "b"], &[[true, false]]);
        read.projection = Some(projection(&[1, 0]));

        let plan = from_read_rel(&consumer, &read).await.unwrap();

        assert_eq!(plan.schema().fields().len(), 2);
        assert_eq!(plan.schema().fields()[0].name(), "b");
        assert_eq!(plan.schema().fields()[1].name(), "a");
        assert_bool_rows(plan, &[vec![Some(false), Some(true)]]).await;
    }

    #[tokio::test]
    async fn virtual_table_empty_projection_preserves_row_count() {
        let consumer = test_consumer();
        let mut read = virtual_table_read_with_bool_values(
            &["a", "b"],
            &[[true, false], [false, true]],
        );
        read.projection = Some(projection(&[]));

        let plan = from_read_rel(&consumer, &read).await.unwrap();

        assert_eq!(plan.schema().fields().len(), 0);
        assert_row_count(plan, 2).await;
    }

    #[tokio::test]
    async fn virtual_table_expressions_read_applies_filter_and_projection() {
        let consumer = test_consumer();
        let mut read = virtual_table_read_with_bool_expressions(
            &["a", "b"],
            &[[true, false], [false, true]],
        );
        read.filter = Some(Box::new(field_reference_filter(0)));
        read.projection = Some(projection(&[1]));

        let plan = from_read_rel(&consumer, &read).await.unwrap();

        assert_eq!(plan.schema().fields().len(), 1);
        assert_eq!(plan.schema().fields()[0].name(), "b");
        let LogicalPlan::Projection(projection) = &plan else {
            panic!("expected Projection, got {plan:?}");
        };
        assert_projection_columns(projection, &["b"]);
        let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
            panic!("expected Filter, got {:?}", projection.input);
        };
        let Expr::Column(column) = &filter.predicate else {
            panic!("expected column filter, got {:?}", filter.predicate);
        };
        assert_eq!(column.name, "a");
        let LogicalPlan::Values(values) = filter.input.as_ref() else {
            panic!("expected Values, got {:?}", filter.input);
        };
        assert_eq!(values.values.len(), 2);
        assert_bool_column(plan, &[Some(false)]).await;
    }

    #[tokio::test]
    async fn virtual_table_produce_one_row_applies_filter() {
        let consumer = test_consumer();
        let mut read = virtual_table_read_produce_one_row();
        read.filter = Some(Box::new(false_filter()));

        let plan = from_read_rel(&consumer, &read).await.unwrap();

        assert_row_count(plan.clone(), 0).await;
        let LogicalPlan::Filter(filter) = plan else {
            panic!("expected Filter, got {plan:?}");
        };
        assert_eq!(
            filter.predicate,
            Expr::Literal(ScalarValue::Boolean(Some(false)), None)
        );
        let LogicalPlan::EmptyRelation(empty) = filter.input.as_ref() else {
            panic!("expected EmptyRelation, got {:?}", filter.input);
        };
        assert!(empty.produce_one_row);
    }

    #[tokio::test]
    async fn virtual_table_produce_one_row_keeps_true_filter() {
        let consumer = test_consumer();
        let mut read = virtual_table_read_produce_one_row();
        read.filter = Some(Box::new(true_filter()));

        let plan = from_read_rel(&consumer, &read).await.unwrap();

        assert_row_count(plan, 1).await;
    }

    #[tokio::test]
    async fn virtual_table_expression_produce_one_row_applies_filter() {
        let consumer = test_consumer();
        let mut read = virtual_table_expression_read_produce_one_row();
        read.filter = Some(Box::new(false_filter()));

        let plan = from_read_rel(&consumer, &read).await.unwrap();

        assert_row_count(plan.clone(), 0).await;
        let LogicalPlan::Filter(filter) = plan else {
            panic!("expected Filter, got {plan:?}");
        };
        assert_eq!(
            filter.predicate,
            Expr::Literal(ScalarValue::Boolean(Some(false)), None)
        );
        let LogicalPlan::EmptyRelation(empty) = filter.input.as_ref() else {
            panic!("expected EmptyRelation, got {:?}", filter.input);
        };
        assert!(empty.produce_one_row);
    }
}
