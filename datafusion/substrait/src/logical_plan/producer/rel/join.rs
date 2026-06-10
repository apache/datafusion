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
use datafusion::common::{JoinConstraint, JoinType, NullEquality, not_impl_err};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, Join, Operator};
use datafusion::prelude::binary_expr;
use std::sync::Arc;
use substrait::proto::rel::RelType;
use substrait::proto::{JoinRel, Rel, join_rel};

pub fn from_join(
    producer: &mut impl SubstraitProducer,
    join: &Join,
) -> datafusion::common::Result<Box<Rel>> {
    // only ON constraints are supported right now
    match join.join_constraint {
        JoinConstraint::On => {}
        JoinConstraint::Using => return not_impl_err!("join constraint: `using`"),
    }

    let left = producer.handle_plan(join.left.as_ref())?;
    let right = producer.handle_plan(join.right.as_ref())?;
    let join_type = to_substrait_jointype(join.join_type);

    let join_expr =
        to_substrait_join_expr(join.on.clone(), join.null_equality, join.filter.clone());
    let join_expression = match join_expr {
        Some(expr) => {
            let in_join_schema = Arc::new(join.left.schema().join(join.right.schema())?);
            let expression = producer.handle_expr(&expr, &in_join_schema)?;
            Some(Box::new(expression))
        }
        None => None,
    };

    Ok(Box::new(Rel {
        rel_type: Some(RelType::Join(Box::new(JoinRel {
            common: None,
            left: Some(left),
            right: Some(right),
            r#type: join_type as i32,
            expression: join_expression,
            post_join_filter: None,
            advanced_extension: None,
        }))),
    }))
}

fn to_substrait_join_expr(
    join_on: Vec<(Expr, Expr)>,
    null_equality: NullEquality,
    join_filter: Option<Expr>,
) -> Option<Expr> {
    // Combine join on and filter conditions into a single Boolean expression (#7611)
    let eq_op = match null_equality {
        NullEquality::NullEqualsNothing => Operator::Eq,
        NullEquality::NullEqualsNull => Operator::IsNotDistinctFrom,
    };
    let all_conditions = join_on
        .into_iter()
        .map(|(left, right)| binary_expr(left, eq_op, right))
        .chain(join_filter);
    conjunction(all_conditions)
}

fn to_substrait_jointype(join_type: JoinType) -> join_rel::JoinType {
    match join_type {
        JoinType::Inner => join_rel::JoinType::Inner,
        JoinType::Left => join_rel::JoinType::Left,
        JoinType::Right => join_rel::JoinType::Right,
        JoinType::Full => join_rel::JoinType::Outer,
        JoinType::LeftAnti => join_rel::JoinType::LeftAnti,
        JoinType::LeftSemi => join_rel::JoinType::LeftSemi,
        JoinType::LeftMark => join_rel::JoinType::LeftMark,
        JoinType::RightMark => join_rel::JoinType::RightMark,
        JoinType::RightAnti => join_rel::JoinType::RightAnti,
        JoinType::RightSemi => join_rel::JoinType::RightSemi,
    }
}

#[cfg(test)]
mod tests {
    use crate::logical_plan::producer::{
        DefaultSubstraitProducer, SubstraitProducer, to_substrait_type,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinConstraint, JoinType, NullEquality};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::utils::conjunction;
    use datafusion::logical_expr::{Join, col, table_scan};
    use std::sync::Arc;
    use substrait::proto::expression::{RexType, ScalarFunction};
    use substrait::proto::rel::RelType;
    use substrait::proto::{Expression, JoinRel, Rel, join_rel};

    #[test]
    fn test_from_join() -> datafusion::common::Result<()> {
        let state = SessionStateBuilder::default().build();
        let mut producer = DefaultSubstraitProducer::new(&state);

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);
        let left_scan = table_scan(Some("t1"), &schema, None)?.build()?;
        let right_scan = table_scan(Some("t2"), &schema, None)?.build()?;
        let join = Join::try_new(
            Arc::new(left_scan.clone()),
            Arc::new(right_scan.clone()),
            vec![(col("t1.a"), col("t2.a")), (col("t1.b"), col("t2.b"))],
            Some(col("t1.c").gt(col("t2.c"))),
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
            false,
        )?;
        let join_expr = producer.handle_join(&join)?;

        let in_join_schema = Arc::new(join.left.schema().join(join.right.schema())?);
        let expected_join_expr = conjunction(vec![
            // Join on
            col("t1.a").eq(col("t2.a")),
            col("t1.b").eq(col("t2.b")),
            // Join filter
            col("t1.c").gt(col("t2.c")),
        ])
        .unwrap();
        let expected_join_expression =
            producer.handle_expr(&expected_join_expr, &in_join_schema)?;

        assert_eq!(
            join_expr,
            Box::new(Rel {
                rel_type: Some(RelType::Join(Box::new(JoinRel {
                    common: None,
                    left: Some(producer.handle_plan(&left_scan)?),
                    right: Some(producer.handle_plan(&right_scan)?),
                    r#type: join_rel::JoinType::Inner as i32,
                    expression: Some(Box::new(expected_join_expression.clone())),
                    post_join_filter: None,
                    advanced_extension: None,
                })))
            })
        );

        // Check that the join_expression has the expected output_type
        if let Expression {
            rex_type: Some(RexType::ScalarFunction(ScalarFunction { output_type, .. })),
        } = expected_join_expression
        {
            let expected_type =
                to_substrait_type(&mut producer, &DataType::Boolean, false)?;
            assert_eq!(output_type, Some(expected_type));
        } else {
            panic!("Substrait ScalarFunction expected")
        }

        Ok(())
    }
}
