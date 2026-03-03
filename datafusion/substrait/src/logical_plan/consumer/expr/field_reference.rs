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
use datafusion::common::{Column, DFSchema, not_impl_err, substrait_err};
use datafusion::logical_expr::Expr;
use std::sync::Arc;
use substrait::proto::expression::FieldReference;
use substrait::proto::expression::field_reference::ReferenceType::DirectReference;
use substrait::proto::expression::field_reference::RootType;
use substrait::proto::expression::reference_segment::ReferenceType::StructField;

pub async fn from_field_reference(
    consumer: &impl SubstraitConsumer,
    field_ref: &FieldReference,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Expr> {
    from_substrait_field_reference(consumer, field_ref, input_schema)
}

pub(crate) fn from_substrait_field_reference(
    consumer: &impl SubstraitConsumer,
    field_ref: &FieldReference,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Expr> {
    match &field_ref.reference_type {
        Some(DirectReference(direct)) => match &direct.reference_type.as_ref() {
            Some(StructField(struct_field)) => {
                if struct_field.child.is_some() {
                    return not_impl_err!(
                        "Direct reference StructField with child is not supported"
                    );
                }
                let field_idx = struct_field.field as usize;
                match &field_ref.root_type {
                    Some(RootType::RootReference(_)) | None => Ok(Expr::Column(
                        Column::from(input_schema.qualified_field(field_idx)),
                    )),
                    Some(RootType::OuterReference(outer_ref)) => {
                        resolve_outer_reference(consumer, outer_ref, field_idx)
                    }
                    Some(RootType::Expression(_)) => not_impl_err!(
                        "Expression root type in field reference is not supported"
                    ),
                }
            }
            _ => not_impl_err!(
                "Direct reference with types other than StructField is not supported"
            ),
        },
        _ => not_impl_err!("unsupported field ref type"),
    }
}

fn resolve_outer_reference(
    consumer: &impl SubstraitConsumer,
    outer_ref: &substrait::proto::expression::field_reference::OuterReference,
    field_idx: usize,
) -> datafusion::common::Result<Expr> {
    let steps_out = outer_ref.steps_out as usize;
    let Some(outer_schema) = consumer.get_outer_schema(steps_out) else {
        return substrait_err!(
            "OuterReference with steps_out={steps_out} \
             but no outer schema is available"
        );
    };
    let (qualifier, field) = outer_schema.qualified_field(field_idx);
    let col = Column::from((qualifier, field));
    Ok(Expr::OuterReferenceColumn(Arc::clone(field), col))
}
