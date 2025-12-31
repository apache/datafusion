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

//! Shared utility functions for Substrait logical plan conversion.

use crate::logical_plan::consumer::{SubstraitConsumer, from_substrait_literal};
use crate::logical_plan::producer::{SubstraitProducer, to_substrait_literal};
use datafusion::common::{substrait_datafusion_err, substrait_err};
use datafusion::logical_expr::Expr;
use substrait::proto::expression::literal::Struct as LiteralStruct;

/// Converts Substrait literal rows from a VirtualTable into DataFusion expressions.
///
/// This function processes the deprecated `values` field of VirtualTable, converting
/// each literal value into a `Expr::Literal` while tracking and validating the name
/// indices against the provided named struct schema.
pub fn convert_literal_rows_from_substrait(
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

/// Converts rows of literal expressions into Substrait literal structs.
///
/// Each row is expected to contain only `Expr::Literal` or `Expr::Alias` wrapping literals.
/// Aliases are unwrapped and the underlying literal is converted.
pub fn convert_literal_rows_to_substrait(
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
