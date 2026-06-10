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
use async_recursion::async_recursion;
use datafusion::common::{DFSchema, DFSchemaRef, not_impl_err};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, lit};
use substrait::proto::{FetchRel, fetch_rel};

#[async_recursion]
pub async fn from_fetch_rel(
    consumer: &impl SubstraitConsumer,
    fetch: &FetchRel,
) -> datafusion::common::Result<LogicalPlan> {
    if let Some(input) = fetch.input.as_ref() {
        let input = LogicalPlanBuilder::from(consumer.consume_rel(input).await?);
        let empty_schema = DFSchemaRef::new(DFSchema::empty());
        let offset = match &fetch.offset_mode {
            #[expect(deprecated)]
            Some(fetch_rel::OffsetMode::Offset(offset)) => Some(lit(*offset)),
            Some(fetch_rel::OffsetMode::OffsetExpr(expr)) => {
                Some(consumer.consume_expression(expr, &empty_schema).await?)
            }
            None => None,
        };
        let count = match &fetch.count_mode {
            #[expect(deprecated)]
            Some(fetch_rel::CountMode::Count(count)) => {
                // -1 means that ALL records should be returned, equivalent to None
                (*count != -1).then(|| lit(*count))
            }
            Some(fetch_rel::CountMode::CountExpr(expr)) => {
                Some(consumer.consume_expression(expr, &empty_schema).await?)
            }
            None => None,
        };
        input.limit_by_expr(offset, count)?.build()
    } else {
        not_impl_err!("Fetch without an input is not valid")
    }
}
