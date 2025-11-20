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

use crate::logical_plan::producer::{
    to_substrait_literal, to_substrait_named_struct, SubstraitProducer,
};
use datafusion::catalog::default_table_source::DefaultTableSource;
use datafusion::common::{not_impl_err, substrait_datafusion_err, DFSchema, ToDFSchema};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{EmptyRelation, Expr, TableScan, Values};
use pbjson_types::Any;
use prost::Message;
use std::sync::Arc;
use substrait::proto::expression::literal::Struct;
use substrait::proto::expression::mask_expression::{StructItem, StructSelect};
use substrait::proto::expression::MaskExpression;
use substrait::proto::extensions::AdvancedExtension;
use substrait::proto::read_rel::{NamedTable, ReadType, VirtualTable};
use substrait::proto::rel::RelType;
use substrait::proto::{ReadRel, Rel};

use crate::logical_plan::constants::TABLE_FUNCTION_TYPE_URL;

#[derive(Clone, PartialEq, ::prost::Message)]
struct TableFunctionReadRelExtension {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(message, repeated, tag = "2")]
    pub arguments: Vec<substrait::proto::expression::Literal>,
}

fn table_function_extension_for_scan(
    producer: &mut impl SubstraitProducer,
    scan: &TableScan,
) -> datafusion::common::Result<Option<AdvancedExtension>> {
    let default_source = match scan.source.as_any().downcast_ref::<DefaultTableSource>() {
        Some(source) => source,
        None => return Ok(None),
    };

    // If the table provider exposes function details (e.g. `generate_series`),
    // include a substrait extension with the function name and evaluated
    // literals. This allows consumers to reconstruct table functions during
    // read-rel deserialization.
    let Some(details) = default_source.table_provider.table_function_details() else {
        return Ok(None);
    };
    let arguments = details
        .arguments
        .iter()
        .map(|value| to_substrait_literal(producer, value))
        .collect::<datafusion::common::Result<_>>()?;

    let extension = TableFunctionReadRelExtension {
        name: details.name.to_string(),
        arguments,
    };

    Ok(Some(AdvancedExtension {
        optimization: vec![],
        enhancement: Some(Any {
            type_url: TABLE_FUNCTION_TYPE_URL.to_string(),
            value: extension.encode_to_vec().into(),
        }),
    }))
}

pub fn from_table_scan(
    producer: &mut impl SubstraitProducer,
    scan: &TableScan,
) -> datafusion::common::Result<Box<Rel>> {
    let projection = scan.projection.as_ref().map(|p| {
        p.iter()
            .map(|i| StructItem {
                field: *i as i32,
                child: None,
            })
            .collect()
    });

    let projection = projection.map(|struct_items| MaskExpression {
        select: Some(StructSelect { struct_items }),
        maintain_singular_struct: false,
    });

    let table_schema = scan.source.schema().to_dfschema_ref()?;
    let base_schema = to_substrait_named_struct(producer, &table_schema)?;

    let filter_option = if scan.filters.is_empty() {
        None
    } else {
        let table_schema_qualified = Arc::new(
            DFSchema::try_from_qualified_schema(
                scan.table_name.clone(),
                &(scan.source.schema()),
            )
            .unwrap(),
        );

        let combined_expr = conjunction(scan.filters.clone()).unwrap();
        let filter_expr =
            producer.handle_expr(&combined_expr, &table_schema_qualified)?;
        Some(Box::new(filter_expr))
    };

    let advanced_extension = table_function_extension_for_scan(producer, scan)?;

    Ok(Box::new(Rel {
        rel_type: Some(RelType::Read(Box::new(ReadRel {
            common: None,
            base_schema: Some(base_schema),
            filter: filter_option,
            best_effort_filter: None,
            projection,
            advanced_extension,
            read_type: Some(ReadType::NamedTable(NamedTable {
                names: scan.table_name.to_vec(),
                advanced_extension: None,
            })),
        }))),
    }))
}

pub fn from_empty_relation(
    producer: &mut impl SubstraitProducer,
    e: &EmptyRelation,
) -> datafusion::common::Result<Box<Rel>> {
    if e.produce_one_row {
        return not_impl_err!("Producing a row from empty relation is unsupported");
    }
    #[allow(deprecated)]
    Ok(Box::new(Rel {
        rel_type: Some(RelType::Read(Box::new(ReadRel {
            common: None,
            base_schema: Some(to_substrait_named_struct(producer, &e.schema)?),
            filter: None,
            best_effort_filter: None,
            projection: None,
            advanced_extension: None,
            read_type: Some(ReadType::VirtualTable(VirtualTable {
                values: vec![],
                expressions: vec![],
            })),
        }))),
    }))
}

pub fn from_values(
    producer: &mut impl SubstraitProducer,
    v: &Values,
) -> datafusion::common::Result<Box<Rel>> {
    let values = v
        .values
        .iter()
        .map(|row| {
            let fields = row
                .iter()
                .map(|v| match v {
                    Expr::Literal(sv, _) => to_substrait_literal(producer, sv),
                    Expr::Alias(alias) => match alias.expr.as_ref() {
                        // The schema gives us the names, so we can skip aliases
                        Expr::Literal(sv, _) => to_substrait_literal(producer, sv),
                        _ => Err(substrait_datafusion_err!(
                                    "Only literal types can be aliased in Virtual Tables, got: {}", alias.expr.variant_name()
                                )),
                    },
                    _ => Err(substrait_datafusion_err!(
                                "Only literal types and aliases are supported in Virtual Tables, got: {}", v.variant_name()
                            )),
                })
                .collect::<datafusion::common::Result<_>>()?;
            Ok(Struct { fields })
        })
        .collect::<datafusion::common::Result<_>>()?;
    #[allow(deprecated)]
    Ok(Box::new(Rel {
        rel_type: Some(RelType::Read(Box::new(ReadRel {
            common: None,
            base_schema: Some(to_substrait_named_struct(producer, &v.schema)?),
            filter: None,
            best_effort_filter: None,
            projection: None,
            advanced_extension: None,
            read_type: Some(ReadType::VirtualTable(VirtualTable {
                values,
                expressions: vec![],
            })),
        }))),
    }))
}

#[cfg(test)]
mod tests {
    use super::TableFunctionReadRelExtension;
    use crate::logical_plan::constants::TABLE_FUNCTION_TYPE_URL;
    use crate::logical_plan::producer::to_substrait_plan;
    use datafusion::prelude::SessionContext;
    use prost::Message;
    use substrait::proto::{plan_rel, rel, ReadRel, Rel};

    #[tokio::test]
    async fn captures_generate_series_table_function() {
        let ctx = SessionContext::new();
        let df = ctx
            .sql("select * from generate_series(1, 3)")
            .await
            .expect("failed to plan generate_series");

        let plan = df.into_optimized_plan().expect("optimized plan");
        let substrait_plan =
            to_substrait_plan(&plan, &ctx.state()).expect("serialize to substrait");

        let root_rel = substrait_plan
            .relations
            .first()
            .and_then(|rel| rel.rel_type.as_ref())
            .and_then(|rel_type| match rel_type {
                plan_rel::RelType::Root(root) => root.input.as_ref(),
                _ => None,
            })
            .expect("root rel");

        let read_rel = find_read_rel(root_rel).expect("found read rel");
        let advanced_extension = read_rel
            .advanced_extension
            .as_ref()
            .expect("table function extension present");
        let enhancement = advanced_extension
            .enhancement
            .as_ref()
            .expect("enhancement payload");

        assert_eq!(enhancement.type_url, TABLE_FUNCTION_TYPE_URL);

        let payload = TableFunctionReadRelExtension::decode(&*enhancement.value)
            .expect("decode table function payload");
        assert_eq!(payload.name, "generate_series");

        let literal_values: Vec<Option<i64>> = payload
            .arguments
            .iter()
            .map(|literal| match literal.literal_type.as_ref() {
                Some(substrait::proto::expression::literal::LiteralType::I64(v)) => {
                    Some(*v)
                }
                _ => None,
            })
            .collect();

        assert_eq!(literal_values, vec![Some(1), Some(3), Some(1)]);
    }

    fn find_read_rel(rel: &Rel) -> Option<&ReadRel> {
        match rel.rel_type.as_ref()? {
            rel::RelType::Read(read) => Some(read),
            rel::RelType::Project(project) => project
                .input
                .as_ref()
                .and_then(|input| find_read_rel(input.as_ref())),
            rel::RelType::Filter(filter) => filter
                .input
                .as_ref()
                .and_then(|input| find_read_rel(input.as_ref())),
            _ => None,
        }
    }
}
