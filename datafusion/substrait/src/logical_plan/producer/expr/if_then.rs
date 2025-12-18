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
use datafusion::logical_expr::Case;
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
    let mut ifs: Vec<IfClause> = vec![];
    // Parse base
    if let Some(e) = expr {
        // Base expression exists
        ifs.push(IfClause {
            r#if: Some(producer.handle_expr(e, schema)?),
            then: None,
        });
    }
    // Parse `when`s
    for (r#if, then) in when_then_expr {
        ifs.push(IfClause {
            r#if: Some(producer.handle_expr(r#if, schema)?),
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
