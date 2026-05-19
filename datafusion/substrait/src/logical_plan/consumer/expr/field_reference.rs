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
use substrait::proto::expression::field_reference::{LambdaParameterReference, RootType};
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
                    Some(RootType::LambdaParameterReference(
                        LambdaParameterReference { steps_out },
                    )) => consumer.lambda_variable(*steps_out as usize, field_idx),
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

#[cfg(test)]
mod tests {
    use datafusion::{
        common::{DFSchema, assert_contains},
        prelude::SessionContext,
    };
    use substrait::proto::{
        Type,
        expression::{
            FieldReference, ReferenceSegment,
            field_reference::{self, LambdaParameterReference, RootType},
            reference_segment::{ReferenceType, StructField},
        },
        r#type::{I64, Kind},
    };

    use crate::{
        extensions::Extensions,
        logical_plan::consumer::{
            DefaultSubstraitConsumer, SubstraitConsumer, from_field_reference,
        },
    };

    #[tokio::test]
    async fn test_lambda_variable_invalid_steps_out() {
        let lambda_field_ref = lambda_field_ref(0, 99);

        let extensions = Extensions::default();
        let session_state = SessionContext::new().state();
        let consumer = DefaultSubstraitConsumer::new(&extensions, &session_state);

        let err =
            from_field_reference(&consumer, &lambda_field_ref, DFSchema::empty_ref())
                .await
                .unwrap_err();

        assert_contains!(err.to_string(), "No lambda at 99 steps out, got only 0");
    }

    #[tokio::test]
    async fn test_lambda_variable_invalid_field_idx() {
        let lambda_field_ref = lambda_field_ref(1, 0);

        let extensions = Extensions::default();
        let session_state = SessionContext::new().state();
        let consumer = DefaultSubstraitConsumer::new(&extensions, &session_state);
        let _names = consumer
            .push_lambda_parameters(
                &[Type {
                    kind: Some(Kind::I64(I64::default())),
                }],
                DFSchema::empty_ref(),
            )
            .unwrap();

        let err =
            from_field_reference(&consumer, &lambda_field_ref, DFSchema::empty_ref())
                .await
                .unwrap_err();

        assert_contains!(
            err.to_string(),
            "At lambda 0 steps out, no field at index 1, got only 1"
        );
    }

    fn lambda_field_ref(field: i32, steps_out: u32) -> FieldReference {
        FieldReference {
            reference_type: Some(field_reference::ReferenceType::DirectReference(
                ReferenceSegment {
                    reference_type: Some(ReferenceType::StructField(Box::new(
                        StructField { field, child: None },
                    ))),
                },
            )),
            root_type: Some(RootType::LambdaParameterReference(
                LambdaParameterReference { steps_out },
            )),
        }
    }
}
