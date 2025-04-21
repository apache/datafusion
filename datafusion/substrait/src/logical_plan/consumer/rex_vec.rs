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

use super::SubstraitConsumer;
use datafusion::common::DFSchema;
use datafusion::logical_expr::Expr;
use substrait::proto::Expression;

/// Convert Substrait Expressions to DataFusion Exprs
pub async fn from_substrait_rex_vec(
    consumer: &impl SubstraitConsumer,
    exprs: &Vec<Expression>,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Vec<Expr>> {
    let mut expressions: Vec<Expr> = vec![];
    for expr in exprs {
        let expression = consumer.consume_expression(expr, input_schema).await?;
        expressions.push(expression);
    }
    Ok(expressions)
}
