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

use crate::logical_plan::producer::SubstraitProducer;
use datafusion::common::{Column, DFSchemaRef, substrait_err};
use datafusion::logical_expr::Expr;
use substrait::proto::Expression;
use substrait::proto::expression::field_reference::{
    OuterReference, ReferenceType, RootReference, RootType,
};
use substrait::proto::expression::{
    FieldReference, ReferenceSegment, RexType, reference_segment,
};

pub fn from_column(
    col: &Column,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let index = schema.index_of_column(col)?;
    substrait_field_ref(index)
}

pub(crate) fn substrait_field_ref(
    index: usize,
) -> datafusion::common::Result<Expression> {
    substrait_field_ref_with_root(index, RootType::RootReference(RootReference {}))
}

fn substrait_field_ref_with_root(
    index: usize,
    root_type: RootType,
) -> datafusion::common::Result<Expression> {
    Ok(Expression {
        rex_type: Some(RexType::Selection(Box::new(FieldReference {
            reference_type: Some(ReferenceType::DirectReference(ReferenceSegment {
                reference_type: Some(reference_segment::ReferenceType::StructField(
                    Box::new(reference_segment::StructField {
                        field: index as i32,
                        child: None,
                    }),
                )),
            })),
            root_type: Some(root_type),
        }))),
    })
}

/// Try to convert an [Expr] to a [FieldReference].
/// Returns `Err` if the [Expr] is not a [Expr::Column].
pub(crate) fn try_to_substrait_field_reference(
    expr: &Expr,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<FieldReference> {
    match expr {
        Expr::Column(col) => {
            let index = schema.index_of_column(col)?;
            Ok(FieldReference {
                reference_type: Some(ReferenceType::DirectReference(ReferenceSegment {
                    reference_type: Some(reference_segment::ReferenceType::StructField(
                        Box::new(reference_segment::StructField {
                            field: index as i32,
                            child: None,
                        }),
                    )),
                })),
                root_type: Some(RootType::RootReference(RootReference {})),
            })
        }
        _ => substrait_err!("Expect a `Column` expr, but found {expr:?}"),
    }
}

/// Convert an outer reference column to a Substrait field reference with an
/// `OuterReference` root type.
///
/// Outer reference columns reference columns from an enclosing query scope in
/// correlated subqueries. The column is resolved against the producer's stack
/// of outer schemas (pushed at each subquery boundary), innermost first, and
/// the resulting `steps_out` records how many query boundaries the reference
/// crosses (`steps_out = 1` is the immediately enclosing query).
pub fn from_outer_reference_column(
    producer: &mut impl SubstraitProducer,
    col: &Column,
) -> datafusion::common::Result<Expression> {
    let mut steps_out = 1;
    while let Some(outer_schema) = producer.get_outer_schema(steps_out) {
        if let Some(index) = outer_schema.maybe_index_of_column(col) {
            return substrait_field_ref_with_root(
                index,
                RootType::OuterReference(OuterReference {
                    steps_out: steps_out as u32,
                }),
            );
        }
        steps_out += 1;
    }
    substrait_err!(
        "Outer reference column '{col}' could not be resolved against any outer \
         query schema. If using a custom SubstraitProducer, ensure it maintains \
         the outer schema stack (push_outer_schema/pop_outer_schema/get_outer_schema)"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Result;

    #[test]
    fn to_field_reference() -> Result<()> {
        let expression = substrait_field_ref(2)?;

        match &expression.rex_type {
            Some(RexType::Selection(field_ref)) => {
                assert_eq!(
                    field_ref
                        .root_type
                        .clone()
                        .expect("root type should be set"),
                    RootType::RootReference(RootReference {})
                );
            }

            _ => panic!("Should not be anything other than field reference"),
        }
        Ok(())
    }
}
