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

use crate::logical_plan::consumer::from_substrait_literal;
use crate::logical_plan::consumer::from_substrait_named_struct;
use crate::logical_plan::consumer::utils::ensure_schema_compatibility;
use crate::logical_plan::consumer::SubstraitConsumer;
use datafusion::common::{
    not_impl_err, plan_err, substrait_datafusion_err, substrait_err, DFSchema,
    DFSchemaRef, TableReference,
};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::utils::split_conjunction_owned;
use datafusion::logical_expr::{
    EmptyRelation, Expr, LogicalPlan, LogicalPlanBuilder, Values,
};
use std::sync::Arc;
use substrait::proto::expression::MaskExpression;
use substrait::proto::read_rel::local_files::file_or_files::PathType::UriFile;
use substrait::proto::read_rel::ReadType;
use substrait::proto::{Expression, ReadRel};
use url::Url;

#[allow(deprecated)]
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

        let plan = {
            let provider = match consumer.resolve_table_ref(&table_ref).await? {
                Some(ref provider) => Arc::clone(provider),
                _ => return plan_err!("No table named '{table_ref}'"),
            };

            LogicalPlanBuilder::scan_with_filters(
                table_ref,
                provider_as_source(Arc::clone(&provider)),
                None,
                filters,
            )?
            .build()?
        };

        ensure_schema_compatibility(plan.schema(), schema.clone())?;

        let schema = apply_masking(schema, projection)?;

        apply_projection(plan, schema)
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
            if vt.values.is_empty() && vt.expressions.is_empty() {
                return Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: DFSchemaRef::new(substrait_schema),
                }));
            }

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
                return Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: true,
                    schema: DFSchemaRef::new(substrait_schema),
                }));
            }

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

            Ok(LogicalPlan::Values(Values {
                schema: DFSchemaRef::new(substrait_schema),
                values,
            }))
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
    #[allow(deprecated)]
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

pub fn apply_masking(
    schema: DFSchema,
    mask_expression: &::core::option::Option<MaskExpression>,
) -> datafusion::common::Result<DFSchema> {
    match mask_expression {
        Some(MaskExpression { select, .. }) => match &select.as_ref() {
            Some(projection) => {
                let column_indices: Vec<usize> = projection
                    .struct_items
                    .iter()
                    .map(|item| item.field as usize)
                    .collect();

                let fields = column_indices
                    .iter()
                    .map(|i| schema.qualified_field(*i))
                    .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
                    .collect();

                Ok(DFSchema::new_with_metadata(
                    fields,
                    schema.metadata().clone(),
                )?)
            }
            None => Ok(schema),
        },
        None => Ok(schema),
    }
}

/// This function returns a DataFrame with fields adjusted if necessary in the event that the
/// Substrait schema is a subset of the DataFusion schema.
fn apply_projection(
    plan: LogicalPlan,
    substrait_schema: DFSchema,
) -> datafusion::common::Result<LogicalPlan> {
    let df_schema = plan.schema();

    if df_schema.logically_equivalent_names_and_types(&substrait_schema) {
        return Ok(plan);
    }

    let df_schema = df_schema.to_owned();

    match plan {
        LogicalPlan::TableScan(mut scan) => {
            let column_indices: Vec<usize> = substrait_schema
                .strip_qualifiers()
                .fields()
                .iter()
                .map(|substrait_field| {
                    Ok(df_schema
                        .index_of_column_by_name(None, substrait_field.name().as_str())
                        .unwrap())
                })
                .collect::<datafusion::common::Result<_>>()?;

            let fields = column_indices
                .iter()
                .map(|i| df_schema.qualified_field(*i))
                .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
                .collect();

            scan.projected_schema = DFSchemaRef::new(DFSchema::new_with_metadata(
                fields,
                df_schema.metadata().clone(),
            )?);
            scan.projection = Some(column_indices);

            Ok(LogicalPlan::TableScan(scan))
        }
        _ => plan_err!("DataFrame passed to apply_projection must be a TableScan"),
    }
}
