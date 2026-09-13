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
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::{Case, Expr};
use substrait::proto::Expression;
use substrait::proto::expression::if_then::IfClause;
use substrait::proto::expression::{IfThen, RexType};

pub fn from_case(
    producer: &mut impl SubstraitProducer,
    case: &Case,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let Case {
        expr,
        when_then_expr,
        else_expr,
    } = case;

    // Substrait's `IfThen` has no notion of a base expression: every `IfClause`
    // is a standalone boolean condition. A `CASE <base> WHEN <value> THEN ...`
    // is therefore emitted as `IfClause`s over `<base> = <value>`, the same
    // desugaring `from_between` applies to `BETWEEN`. DataFusion matches a base
    // expression with `=` semantics, so this preserves the plan's meaning,
    // including a `NULL` `<value>` never matching.
    let mut ifs: Vec<IfClause> = Vec::with_capacity(when_then_expr.len());
    for (when, then) in when_then_expr {
        let condition = match expr {
            Some(base) => {
                let eq = Expr::eq(*base.clone(), *when.clone());
                producer.handle_expr(&eq, schema)?
            }
            None => producer.handle_expr(when, schema)?,
        };
        ifs.push(IfClause {
            r#if: Some(condition),
            then: Some(producer.handle_expr(then, schema)?),
        });
    }

    // Parse outer `else`
    let r#else: Option<Box<Expression>> = match else_expr {
        Some(e) => Some(Box::new(producer.handle_expr(e, schema)?)),
        None => None,
    };

    Ok(Expression {
        rex_type: Some(RexType::IfThen(Box::new(IfThen { ifs, r#else }))),
    })
}
