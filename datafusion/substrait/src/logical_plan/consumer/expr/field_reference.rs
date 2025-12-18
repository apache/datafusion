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
use datafusion::common::{Column, DFSchema, not_impl_err};
use datafusion::logical_expr::Expr;
use substrait::proto::expression::FieldReference;
use substrait::proto::expression::field_reference::ReferenceType::DirectReference;
use substrait::proto::expression::reference_segment::ReferenceType::StructField;

pub async fn from_field_reference(
    _consumer: &impl SubstraitConsumer,
    field_ref: &FieldReference,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Expr> {
    from_substrait_field_reference(field_ref, input_schema)
}

pub(crate) fn from_substrait_field_reference(
    field_ref: &FieldReference,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Expr> {
    match &field_ref.reference_type {
        Some(DirectReference(direct)) => match &direct.reference_type.as_ref() {
            Some(StructField(x)) => match &x.child.as_ref() {
                Some(_) => not_impl_err!(
                    "Direct reference StructField with child is not supported"
                ),
                None => Ok(Expr::Column(Column::from(
                    input_schema.qualified_field(x.field as usize),
                ))),
            },
            _ => not_impl_err!(
                "Direct reference with types other than StructField is not supported"
            ),
        },
        _ => not_impl_err!("unsupported field ref type"),
    }
}
