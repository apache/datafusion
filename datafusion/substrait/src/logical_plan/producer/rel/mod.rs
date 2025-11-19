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

mod aggregate_rel;
mod exchange_rel;
mod fetch_rel;
mod filter_rel;
mod join;
mod project_rel;
mod read_rel;
mod set_rel;
mod sort_rel;

pub use aggregate_rel::*;
pub use exchange_rel::*;
pub use fetch_rel::*;
pub use filter_rel::*;
pub use join::*;
pub use project_rel::*;
pub use read_rel::*;
pub use set_rel::*;
pub use sort_rel::*;

use crate::logical_plan::producer::SubstraitProducer;
use datafusion::common::not_impl_err;
use datafusion::logical_expr::LogicalPlan;
use substrait::proto::Rel;

pub fn to_substrait_rel(
    producer: &mut impl SubstraitProducer,
    plan: &LogicalPlan,
) -> datafusion::common::Result<Box<Rel>> {
    match plan {
        LogicalPlan::Projection(plan) => producer.handle_projection(plan),
        LogicalPlan::Filter(plan) => producer.handle_filter(plan),
        LogicalPlan::Window(plan) => producer.handle_window(plan),
        LogicalPlan::Aggregate(plan) => producer.handle_aggregate(plan),
        LogicalPlan::Sort(plan) => producer.handle_sort(plan),
        LogicalPlan::Join(plan) => producer.handle_join(plan),
        LogicalPlan::Repartition(plan) => producer.handle_repartition(plan),
        LogicalPlan::Union(plan) => producer.handle_union(plan),
        LogicalPlan::TableScan(plan) => producer.handle_table_scan(plan),
        LogicalPlan::EmptyRelation(plan) => producer.handle_empty_relation(plan),
        LogicalPlan::Subquery(plan) => not_impl_err!("Unsupported plan type: {plan:?}")?,
        LogicalPlan::SubqueryAlias(plan) => producer.handle_subquery_alias(plan),
        LogicalPlan::Limit(plan) => producer.handle_limit(plan),
        LogicalPlan::Statement(plan) => not_impl_err!("Unsupported plan type: {plan:?}")?,
        LogicalPlan::Values(plan) => producer.handle_values(plan),
        LogicalPlan::Explain(plan) => not_impl_err!("Unsupported plan type: {plan:?}")?,
        LogicalPlan::Analyze(plan) => not_impl_err!("Unsupported plan type: {plan:?}")?,
        LogicalPlan::Extension(plan) => producer.handle_extension(plan),
        LogicalPlan::Distinct(plan) => producer.handle_distinct(plan),
        LogicalPlan::Dml(plan) => not_impl_err!("Unsupported plan type: {plan:?}")?,
        LogicalPlan::Ddl(plan) => not_impl_err!("Unsupported plan type: {plan:?}")?,
        LogicalPlan::Copy(plan) => not_impl_err!("Unsupported plan type: {plan:?}")?,
        LogicalPlan::DescribeTable(plan) => {
            not_impl_err!("Unsupported plan type: {plan:?}")?
        }
        LogicalPlan::Unnest(plan) => not_impl_err!("Unsupported plan type: {plan:?}")?,
        LogicalPlan::RecursiveQuery(plan) => {
            not_impl_err!("Unsupported plan type: {plan:?}")?
        }
        LogicalPlan::LateralBatchedTableFunction(plan) => {
            not_impl_err!("Unsupported plan type: {plan:?}")?
        }
        LogicalPlan::StandaloneBatchedTableFunction(plan) => {
            not_impl_err!("Unsupported plan type: {plan:?}")?
        }
    }
}
