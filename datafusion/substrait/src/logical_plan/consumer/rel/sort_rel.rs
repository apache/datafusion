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

use crate::logical_plan::consumer::{SubstraitConsumer, from_substrait_sorts};
use datafusion::common::not_impl_err;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use substrait::proto::SortRel;

pub async fn from_sort_rel(
    consumer: &impl SubstraitConsumer,
    sort: &SortRel,
) -> datafusion::common::Result<LogicalPlan> {
    if let Some(input) = sort.input.as_ref() {
        let input = LogicalPlanBuilder::from(consumer.consume_rel(input).await?);
        let sorts = from_substrait_sorts(consumer, &sort.sorts, input.schema()).await?;
        input.sort(sorts)?.build()
    } else {
        not_impl_err!("Sort without an input is not valid")
    }
}
