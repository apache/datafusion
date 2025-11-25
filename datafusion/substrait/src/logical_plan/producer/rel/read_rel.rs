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
use datafusion::common::{not_impl_err, substrait_datafusion_err, DFSchema, ToDFSchema};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{EmptyRelation, Expr, TableScan, Values};
use std::sync::Arc;
use substrait::proto::expression::literal::Struct as LiteralStruct;
use substrait::proto::expression::mask_expression::{StructItem, StructSelect};
use substrait::proto::expression::nested::Struct as NestedStruct;
use substrait::proto::expression::MaskExpression;
use substrait::proto::read_rel::{NamedTable, ReadType, VirtualTable};
use substrait::proto::rel::RelType;
use substrait::proto::{ReadRel, Rel};

/// Converts rows of literal expressions into Substrait literal structs.
///
/// Each row is expected to contain only `Expr::Literal` or `Expr::Alias` wrapping literals.
/// Aliases are unwrapped and the underlying literal is converted.
fn convert_literal_rows(
    producer: &mut impl SubstraitProducer,
    rows: &[Vec<Expr>],
) -> datafusion::common::Result<Vec<LiteralStruct>> {
    rows.iter()
        .map(|row| {
            let fields = row
                .iter()
                .map(|expr| match expr {
                    Expr::Literal(sv, _) => to_substrait_literal(producer, sv),
                    Expr::Alias(alias) => match alias.expr.as_ref() {
                        // The schema gives us the names, so we can skip aliases
                        Expr::Literal(sv, _) => to_substrait_literal(producer, sv),
                        _ => Err(substrait_datafusion_err!(
                            "Only literal types can be aliased in Virtual Tables, got: {}",
                            alias.expr.variant_name()
                        )),
                    },
                    _ => Err(substrait_datafusion_err!(
                        "Only literal types and aliases are supported in Virtual Tables, got: {}",
                        expr.variant_name()
                    )),
                })
                .collect::<datafusion::common::Result<_>>()?;
            Ok(LiteralStruct { fields })
        })
        .collect()
}

/// Converts rows of arbitrary expressions into Substrait nested structs.
///
/// Validates that each row has the expected schema length and converts each expression
/// using the producer's expression handler.
fn convert_expression_rows(
    producer: &mut impl SubstraitProducer,
    rows: &[Vec<Expr>],
    schema_len: usize,
    empty_schema: &Arc<DFSchema>,
) -> datafusion::common::Result<Vec<NestedStruct>> {
    rows.iter()
        .map(|row| {
            if row.len() != schema_len {
                return Err(substrait_datafusion_err!(
                    "Names list must match exactly to nested schema, but found {} uses for {} names",
                    row.len(),
                    schema_len
                ));
            }

            let fields = row
                .iter()
                .map(|expr| producer.handle_expr(expr, empty_schema))
                .collect::<datafusion::common::Result<_>>()?;
            Ok(NestedStruct { fields })
        })
        .collect()
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

    Ok(Box::new(Rel {
        rel_type: Some(RelType::Read(Box::new(ReadRel {
            common: None,
            base_schema: Some(base_schema),
            filter: filter_option,
            best_effort_filter: None,
            projection,
            advanced_extension: None,
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
    let schema_len = v.schema.fields().len();
    let empty_schema = Arc::new(DFSchema::empty());

    let use_literals = v.values.iter().all(|row| {
        row.iter().all(|expr| match expr {
            Expr::Literal(_, _) => true,
            Expr::Alias(alias) => matches!(alias.expr.as_ref(), Expr::Literal(_, _)),
            _ => false,
        })
    });

    let (values, expressions) = if use_literals {
        let values = convert_literal_rows(producer, &v.values)?;
        (values, vec![])
    } else {
        let expressions =
            convert_expression_rows(producer, &v.values, schema_len, &empty_schema)?;
        (vec![], expressions)
    };
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
                expressions,
            })),
        }))),
    }))
}
