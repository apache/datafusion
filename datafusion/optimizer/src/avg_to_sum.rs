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

//! [`AvgToWeightedAvg`] converts `AVG(x)` to `SUM(x) / COUNT(x)`

use datafusion_common::{Result, tree_node::Transformed};
use datafusion_expr::{Expr, LogicalPlan, Aggregate, col, ExprSchemable};
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams};
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_expr::utils::grouping_set_to_exprlist;
use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

/// Optimizer rule that converts `AVG(x)` to `SUM(x) / COUNT(x)`
///
/// This rule transforms:
/// `SELECT AVG(x) GROUP BY g`
/// into:
/// `SELECT sum_x / count_x FROM (SELECT g, SUM(x) as sum_x, COUNT(x) as count_x FROM t GROUP BY g)`
#[derive(Default, Debug)]
pub struct AvgToSum {}

impl AvgToSum {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for AvgToSum {
    fn name(&self) -> &str {
        "avg_to_weighted_avg"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Aggregate(agg) = plan else {
            return Ok(Transformed::no(plan));
        };

        let mut has_avg = false;
        for expr in &agg.aggr_expr {
            if let Expr::AggregateFunction(AggregateFunction { func, .. }) = expr {
                if func.name() == "avg" {
                    has_avg = true;
                    break;
                }
            }
        }

        if !has_avg {
            return Ok(Transformed::no(LogicalPlan::Aggregate(agg)));
        }

        let Some(registry) = config.function_registry() else {
            return Ok(Transformed::no(LogicalPlan::Aggregate(agg)));
        };

        let sum_func = registry.udaf("sum")?;
        let count_func = registry.udaf("count")?;

        let mut new_aggr_expr = Vec::new();
        let mut avg_info = Vec::new();
        let mut avg_idx = 0;

        for expr in &agg.aggr_expr {
            match expr {
                Expr::AggregateFunction(AggregateFunction {
                    func,
                    params: AggregateFunctionParams {
                        args,
                        distinct,
                        filter,
                        order_by,
                        null_treatment,
                    },
                }) if func.name() == "avg" && !*distinct && filter.is_none() && order_by.is_empty() => {
                    let arg = &args[0];
                    let sum_name = format!("avg_sum_{}", avg_idx);
                    let count_name = format!("avg_count_{}", avg_idx);
                    avg_idx += 1;
                    
                    // Add SUM(arg) to inner aggregation
                    new_aggr_expr.push(Expr::AggregateFunction(AggregateFunction::new_udf(
                        sum_func.clone(),
                        vec![arg.clone()],
                        false,
                        None,
                        vec![],
                        None,
                    )).alias(&sum_name));

                    // Add COUNT(arg) to inner aggregation
                    new_aggr_expr.push(Expr::AggregateFunction(AggregateFunction::new_udf(
                        count_func.clone(),
                        vec![arg.clone()],
                        false,
                        None,
                        vec![],
                        None,
                    )).alias(&count_name));

                    avg_info.push(Some((sum_name, count_name, func.clone(), arg.clone())));
                }
                _ => {
                    // Keep other aggregates as is, but they need to be in the inner agg and projected out
                    let name = expr.schema_name().to_string();
                    new_aggr_expr.push(expr.clone().alias(&name));
                    avg_info.push(None);
                }
            }
        }

        let new_agg = Aggregate::try_new(
            agg.input.clone(),
            agg.group_expr.clone(),
            new_aggr_expr,
        )?;
        let agg_schema = &new_agg.schema;

        let mut projection_expr = Vec::new();

        // Add group by columns to projection
        for group_expr in grouping_set_to_exprlist(&agg.group_expr)? {
            projection_expr.push(group_expr.clone());
        }

        for (expr, info) in agg.aggr_expr.iter().zip(avg_info.into_iter()) {
            if let Some((sum_name, count_name, func, arg)) = info {
                let arg_type = arg.get_type(agg.input.schema())?;
                let avg_return_type = func.return_type(&[arg_type])?;
                
                let sum_col = col(&sum_name).cast_to(&avg_return_type, agg_schema)?;
                let count_col = col(&count_name).cast_to(&avg_return_type, agg_schema)?;
                let div = sum_col / count_col;
                
                // If count is 0, return NULL instead of NaN/Inf
                let div = Expr::Case(datafusion_expr::Case {
                    expr: None,
                    when_then_expr: vec![(
                        Box::new(col(&count_name).eq(Expr::Literal(datafusion_common::ScalarValue::Int64(Some(0)), None))),
                        Box::new(Expr::Literal(datafusion_common::ScalarValue::Float64(None), None).cast_to(&avg_return_type, agg_schema)?),
                    )],
                    else_expr: Some(Box::new(div.cast_to(&avg_return_type, agg_schema)?)),
                });

                projection_expr.push(div.alias(expr.schema_name().to_string()));
            } else {
                let name = expr.schema_name().to_string();
                projection_expr.push(col(&name));
            }
        }

        let new_plan = LogicalPlanBuilder::from(LogicalPlan::Aggregate(new_agg))
            .project(projection_expr)?
            .build()?;

        Ok(Transformed::yes(new_plan))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use datafusion_expr::{col, logical_plan::builder::LogicalPlanBuilder, logical_plan::LogicalTableSource};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion_functions_aggregate::average::avg_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_expr::registry::{MemoryFunctionRegistry, FunctionRegistry};
    use datafusion_expr::GroupingSet;
    use crate::OptimizerContext;

    #[test]
    fn test_avg_to_weighted_avg_decimal() -> Result<()> {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Decimal128(25, 2), true),
        ]));
        let table_scan = LogicalPlanBuilder::scan("t", Arc::new(LogicalTableSource::new(schema)), None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![
                Expr::AggregateFunction(AggregateFunction::new_udf(
                    avg_udaf(),
                    vec![col("b")],
                    false,
                    None,
                    vec![],
                    None,
                ))
            ])?
            .build()?;

        let mut registry = MemoryFunctionRegistry::new();
        registry.register_udaf(avg_udaf())?;
        registry.register_udaf(sum_udaf())?;
        registry.register_udaf(count_udaf())?;

        let config = OptimizerContext::default();
        
        struct TestConfig<'a> {
            registry: &'a MemoryFunctionRegistry,
            inner: OptimizerContext,
        }
        impl OptimizerConfig for TestConfig<'_> {
            fn query_execution_start_time(&self) -> chrono::DateTime<chrono::Utc> { self.inner.query_execution_start_time() }
            fn options(&self) -> Arc<datafusion_common::config::ConfigOptions> { self.inner.options() }
            fn function_registry(&self) -> Option<&dyn FunctionRegistry> {
                Some(self.registry)
            }
            fn alias_generator(&self) -> &Arc<datafusion_common::alias::AliasGenerator> {
                self.inner.alias_generator()
            }
        }

        let test_config = TestConfig {
            registry: &registry,
            inner: config,
        };

        let rule = AvgToSum::new();
        let optimized_plan = rule.rewrite(plan, &test_config)?.data;

        // Check that the plan was transformed
        match &optimized_plan {
            LogicalPlan::Projection(proj) => {
                assert_eq!(proj.expr.len(), 2); // a and avg(b)
                // Check that there are casts in the projection
                let avg_expr = &proj.expr[1];
                let display = format!("{:?}", avg_expr);
                assert!(display.contains("Cast"));
            }
            _ => panic!("Expected projection at the top, got {:?}", optimized_plan),
        }

        Ok(())
    }

    #[test]
    fn test_avg_to_weighted_avg_grouping_sets() -> Result<()> {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]));
        let table_scan = LogicalPlanBuilder::scan("t", Arc::new(LogicalTableSource::new(schema)), None)?.build()?;

        let grouping_set = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
            vec![col("a")],
            vec![col("a"), col("b")],
        ]));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![grouping_set], vec![
                Expr::AggregateFunction(AggregateFunction::new_udf(
                    avg_udaf(),
                    vec![col("c")],
                    false,
                    None,
                    vec![],
                    None,
                ))
            ])?
            .build()?;

        let mut registry = MemoryFunctionRegistry::new();
        registry.register_udaf(avg_udaf())?;
        registry.register_udaf(sum_udaf())?;
        registry.register_udaf(count_udaf())?;

        let config = OptimizerContext::default();
        
        struct TestConfig<'a> {
            registry: &'a MemoryFunctionRegistry,
            inner: OptimizerContext,
        }
        impl OptimizerConfig for TestConfig<'_> {
            fn query_execution_start_time(&self) -> chrono::DateTime<chrono::Utc> { self.inner.query_execution_start_time() }
            fn options(&self) -> Arc<datafusion_common::config::ConfigOptions> { self.inner.options() }
            fn function_registry(&self) -> Option<&dyn FunctionRegistry> {
                Some(self.registry)
            }
            fn alias_generator(&self) -> &Arc<datafusion_common::alias::AliasGenerator> {
                self.inner.alias_generator()
            }
        }

        let test_config = TestConfig {
            registry: &registry,
            inner: config,
        };

        let rule = AvgToSum::new();
        let optimized_plan = rule.rewrite(plan, &test_config)?.data;

        // Check that the plan was transformed
        match &optimized_plan {
            LogicalPlan::Projection(proj) => {
                // a, b, and avg(c)
                assert_eq!(proj.expr.len(), 3);
                match &proj.input.as_ref() {
                    LogicalPlan::Aggregate(agg) => {
                        // GroupingSet, SUM(c), COUNT(*)
                        assert_eq!(agg.group_expr.len(), 1);
                        assert_eq!(agg.aggr_expr.len(), 2);
                    }
                    _ => panic!("Expected aggregate input to projection"),
                }
            }
            _ => panic!("Expected projection at the top, got {:?}", optimized_plan),
        }

        Ok(())
    }

    #[test]
    fn test_avg_to_weighted_avg() -> Result<()> {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));
        let table_scan = LogicalPlanBuilder::scan("t", Arc::new(LogicalTableSource::new(schema)), None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![
                Expr::AggregateFunction(AggregateFunction::new_udf(
                    avg_udaf(),
                    vec![col("b")],
                    false,
                    None,
                    vec![],
                    None,
                ))
            ])?
            .build()?;

        let mut registry = MemoryFunctionRegistry::new();
        registry.register_udaf(avg_udaf())?;
        registry.register_udaf(sum_udaf())?;
        registry.register_udaf(count_udaf())?;

        let config = OptimizerContext::default();
        
        struct TestConfig<'a> {
            registry: &'a MemoryFunctionRegistry,
            inner: OptimizerContext,
        }
        impl OptimizerConfig for TestConfig<'_> {
            fn query_execution_start_time(&self) -> chrono::DateTime<chrono::Utc> { self.inner.query_execution_start_time() }
            fn options(&self) -> Arc<datafusion_common::config::ConfigOptions> { self.inner.options() }
            fn function_registry(&self) -> Option<&dyn FunctionRegistry> {
                Some(self.registry)
            }
            fn alias_generator(&self) -> &Arc<datafusion_common::alias::AliasGenerator> {
                self.inner.alias_generator()
            }
        }

        let test_config = TestConfig {
            registry: &registry,
            inner: config,
        };

        let rule = AvgToSum::new();
        let optimized_plan = rule.rewrite(plan, &test_config)?.data;

        // Check that the plan was transformed
        match &optimized_plan {
            LogicalPlan::Projection(proj) => {
                assert_eq!(proj.expr.len(), 2); // a and avg(b)
                match &proj.input.as_ref() {
                    LogicalPlan::Aggregate(agg) => {
                        assert_eq!(agg.aggr_expr.len(), 2); // SUM(b) and COUNT(*)
                    }
                    _ => panic!("Expected aggregate input to projection"),
                }
            }
            _ => panic!("Expected projection at the top, got {:?}", optimized_plan),
        }

        Ok(())
    }
}
