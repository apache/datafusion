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

//! [`CoalesceFirstLast`] coalesces peer `first_value` / `last_value` aggregate
//! expressions that share the same `ORDER BY` key into a single struct-valued
//! aggregate.

use std::collections::HashMap;
use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::Result;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::expr::{
    AggregateFunction, AggregateFunctionParams, NullTreatment, Sort,
};
use datafusion_expr::{
    Aggregate, AggregateUDF, Expr, LogicalPlan, LogicalPlanBuilder, col, lit,
};

use indexmap::IndexMap;

const FIRST_VALUE: &str = "first_value";
const LAST_VALUE: &str = "last_value";
const NAMED_STRUCT: &str = "named_struct";
const GET_FIELD: &str = "get_field";

/// Coalesces peer `first_value` / `last_value` aggregates that share one
/// `ORDER BY` key into a single struct-valued aggregate, with a projection on
/// top to unpack the struct back into the original columns:
///
/// ```text
/// Aggregate: groupBy=[[p]], aggr=[[first_value(a ORDER BY o DESC),
///                                  first_value(b ORDER BY o DESC)]]
/// ```
///
/// becomes
///
/// ```text
/// Projection: p, get_field(wrapped, 'c0'), get_field(wrapped, 'c1')
///   Aggregate: groupBy=[[p]],
///              aggr=[[first_value(named_struct('c0', a, 'c1', b) ORDER BY o DESC) AS wrapped]]
/// ```
///
/// The input is scanned once, not once per expression, and holds one per-group
/// state slot instead of N.
///
/// Off by default (`optimizer.enable_coalesce_first_last`); no-ops if
/// `named_struct` / `get_field` are not registered.
#[derive(Default, Debug)]
pub struct CoalesceFirstLast {}

impl CoalesceFirstLast {
    pub fn new() -> Self {
        Self {}
    }
}

/// `(function name, ORDER BY key, null treatment)` — peers may be coalesced only
/// when all three match.
type BucketKey = (String, Vec<Sort>, Option<NullTreatment>);

struct Coalesceable {
    key: BucketKey,
    func: Arc<AggregateUDF>,
    value: Expr,
}

fn classify(expr: &Expr) -> Option<Coalesceable> {
    let Expr::AggregateFunction(AggregateFunction { func, params }) = expr else {
        return None;
    };
    let name = func.name();
    if name != FIRST_VALUE && name != LAST_VALUE {
        return None;
    }
    let AggregateFunctionParams {
        args,
        distinct,
        filter,
        order_by,
        null_treatment,
    } = params;

    // DISTINCT / FILTER would break the shared scan, and IGNORE NULLS cannot be
    // reproduced through a single struct (it would skip rows where the whole
    // struct is null rather than where the individual value is null).
    if *distinct
        || filter.is_some()
        || args.len() != 1
        || order_by.is_empty()
        || *null_treatment == Some(NullTreatment::IgnoreNulls)
    {
        return None;
    }

    Some(Coalesceable {
        key: (name.to_string(), order_by.clone(), *null_treatment),
        func: Arc::clone(func),
        value: args[0].clone(),
    })
}

impl OptimizerRule for CoalesceFirstLast {
    fn name(&self) -> &str {
        "coalesce_first_last"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !config.options().optimizer.enable_coalesce_first_last {
            return Ok(Transformed::no(plan));
        }

        let LogicalPlan::Aggregate(aggregate) = plan else {
            return Ok(Transformed::no(plan));
        };

        // Grouping sets expand the group columns (plus an internal grouping id)
        // beyond `group_expr.len()`, which the schema slicing below assumes; skip
        // them (consistent with the other cases this rule declines to coalesce).
        if matches!(aggregate.group_expr.as_slice(), [Expr::GroupingSet(_)]) {
            return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
        }

        let classified: Vec<Option<Coalesceable>> =
            aggregate.aggr_expr.iter().map(classify).collect();

        let mut buckets: IndexMap<BucketKey, Vec<usize>> = IndexMap::new();
        for (i, c) in classified.iter().enumerate() {
            if let Some(c) = c {
                buckets.entry(c.key.clone()).or_default().push(i);
            }
        }
        buckets.retain(|_, idxs| idxs.len() >= 2);
        if buckets.is_empty() {
            return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
        }

        // No-op rather than fail if the required scalar functions are missing
        // (e.g. registry not wired, or a reduced function library).
        let Some(registry) = config.function_registry() else {
            return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
        };
        let (Ok(named_struct), Ok(get_field)) =
            (registry.udf(NAMED_STRUCT), registry.udf(GET_FIELD))
        else {
            return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
        };

        let group_len = aggregate.group_expr.len();
        let orig_columns = aggregate.schema.columns();

        // Original aggregate index -> (struct alias, struct field position).
        let mut coalesced_at: HashMap<usize, (String, usize)> = HashMap::new();
        let mut coalesced_aggr_exprs: Vec<Expr> = Vec::with_capacity(buckets.len());

        for (_key, idxs) in &buckets {
            let alias = config.alias_generator().next("__coalesce_first_last");

            let mut struct_args = Vec::with_capacity(idxs.len() * 2);
            for (pos, &i) in idxs.iter().enumerate() {
                let c = classified[i].as_ref().expect("bucket member is classified");
                struct_args.push(lit(format!("c{pos}")));
                struct_args.push(c.value.clone());
                coalesced_at.insert(i, (alias.clone(), pos));
            }

            let first = classified[idxs[0]].as_ref().expect("non-empty bucket");
            let (_, order_by, null_treatment) = &first.key;
            let coalesced = Expr::AggregateFunction(AggregateFunction::new_udf(
                Arc::clone(&first.func),
                vec![named_struct.call(struct_args)],
                /* distinct */ false,
                /* filter */ None,
                order_by.clone(),
                *null_treatment,
            ))
            .alias(alias);
            coalesced_aggr_exprs.push(coalesced);
        }

        // Untouched aggregates keep their original order, then the coalesced ones.
        let mut new_aggr_exprs: Vec<Expr> = aggregate
            .aggr_expr
            .iter()
            .enumerate()
            .filter(|(i, _)| !coalesced_at.contains_key(i))
            .map(|(_, e)| e.clone())
            .collect();
        new_aggr_exprs.extend(coalesced_aggr_exprs);

        let new_aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
            Arc::clone(&aggregate.input),
            aggregate.group_expr.clone(),
            new_aggr_exprs,
        )?);

        // Rebuild the original output schema column-for-column: group columns,
        // then each aggregate column, unpacking the struct where coalesced.
        let mut projection_exprs: Vec<Expr> = Vec::with_capacity(orig_columns.len());
        for column in &orig_columns[..group_len] {
            projection_exprs.push(Expr::Column(column.clone()));
        }
        for (i, column) in orig_columns[group_len..].iter().enumerate() {
            if let Some((alias, pos)) = coalesced_at.get(&i) {
                let field =
                    get_field.call(vec![col(alias.as_str()), lit(format!("c{pos}"))]);
                projection_exprs.push(field.alias(column.name().to_string()));
            } else {
                projection_exprs.push(Expr::Column(column.clone()));
            }
        }

        let projection = LogicalPlanBuilder::from(new_aggregate)
            .project(projection_exprs)?
            .build()?;

        Ok(Transformed::yes(projection))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::OptimizerContext;
    use crate::assert_optimized_plan_eq_snapshot;

    use arrow::datatypes::{DataType, Field, Schema};
    use chrono::{DateTime, Utc};
    use datafusion_common::alias::AliasGenerator;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::expr::GroupingSet;
    use datafusion_expr::logical_plan::table_scan;
    use datafusion_expr::registry::{FunctionRegistry, MemoryFunctionRegistry};
    use datafusion_functions_aggregate::expr_fn::count;
    use datafusion_functions_aggregate::first_last::{first_value_udaf, last_value_udaf};

    /// An [`OptimizerConfig`] that enables the rule and exposes a registry with
    /// `named_struct` / `get_field`, so the rewrite can be exercised in isolation.
    #[derive(Debug)]
    struct TestConfig {
        options: Arc<ConfigOptions>,
        alias_generator: Arc<AliasGenerator>,
        registry: MemoryFunctionRegistry,
        has_registry: bool,
    }

    impl TestConfig {
        fn new() -> Self {
            Self::build(true)
        }

        fn without_registry() -> Self {
            Self::build(false)
        }

        fn build(has_registry: bool) -> Self {
            let mut registry = MemoryFunctionRegistry::new();
            datafusion_functions::register_all(&mut registry).unwrap();
            let mut options = ConfigOptions::default();
            options.optimizer.enable_coalesce_first_last = true;
            Self {
                options: Arc::new(options),
                alias_generator: Arc::new(AliasGenerator::new()),
                registry,
                has_registry,
            }
        }
    }

    impl OptimizerConfig for TestConfig {
        fn query_execution_start_time(&self) -> Option<DateTime<Utc>> {
            None
        }

        fn alias_generator(&self) -> &Arc<AliasGenerator> {
            &self.alias_generator
        }

        fn options(&self) -> Arc<ConfigOptions> {
            Arc::clone(&self.options)
        }

        fn function_registry(&self) -> Option<&dyn FunctionRegistry> {
            self.has_registry
                .then_some(&self.registry as &dyn FunctionRegistry)
        }
    }

    macro_rules! assert_coalesced {
        ($config:expr, $plan:expr, @ $expected:literal $(,)?) => {{
            let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
                vec![Arc::new(CoalesceFirstLast::new())];
            assert_optimized_plan_eq_snapshot!($config, rules, $plan, @ $expected,)
        }};
    }

    /// Scan with a partition key `p`, two value columns `a` / `b`, and an
    /// ordering column `o`.
    fn scan() -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("p", DataType::UInt32, false),
            Field::new("a", DataType::UInt32, true),
            Field::new("b", DataType::UInt32, true),
            Field::new("o", DataType::UInt32, true),
        ]);
        table_scan(Some("t"), &schema, None)?.build()
    }

    fn order_by_o() -> Vec<Sort> {
        vec![Sort::new(col("o"), false, false)]
    }

    fn first_value(arg: Expr, order_by: Vec<Sort>, distinct: bool) -> Expr {
        Expr::AggregateFunction(AggregateFunction::new_udf(
            first_value_udaf(),
            vec![arg],
            distinct,
            None,
            order_by,
            None,
        ))
    }

    fn last_value(arg: Expr, order_by: Vec<Sort>) -> Expr {
        Expr::AggregateFunction(AggregateFunction::new_udf(
            last_value_udaf(),
            vec![arg],
            false,
            None,
            order_by,
            None,
        ))
    }

    #[test]
    fn coalesces_peer_first_values() -> Result<()> {
        let plan = LogicalPlanBuilder::from(scan()?)
            .aggregate(
                vec![col("p")],
                vec![
                    first_value(col("a"), order_by_o(), false),
                    first_value(col("b"), order_by_o(), false),
                ],
            )?
            .build()?;
        assert_coalesced!(TestConfig::new(), plan, @ r#"
        Projection: t.p, get_field(__coalesce_first_last_1, Utf8("c0")) AS first_value(t.a) ORDER BY [t.o DESC NULLS LAST], get_field(__coalesce_first_last_1, Utf8("c1")) AS first_value(t.b) ORDER BY [t.o DESC NULLS LAST]
          Aggregate: groupBy=[[t.p]], aggr=[[first_value(named_struct(Utf8("c0"), t.a, Utf8("c1"), t.b)) ORDER BY [t.o DESC NULLS LAST] AS __coalesce_first_last_1]]
            TableScan: t
        "#)
    }

    #[test]
    fn preserves_non_coalesced_aggregate() -> Result<()> {
        let plan = LogicalPlanBuilder::from(scan()?)
            .aggregate(
                vec![col("p")],
                vec![
                    count(col("a")),
                    first_value(col("a"), order_by_o(), false),
                    first_value(col("b"), order_by_o(), false),
                ],
            )?
            .build()?;
        assert_coalesced!(TestConfig::new(), plan, @ r#"
        Projection: t.p, count(t.a), get_field(__coalesce_first_last_1, Utf8("c0")) AS first_value(t.a) ORDER BY [t.o DESC NULLS LAST], get_field(__coalesce_first_last_1, Utf8("c1")) AS first_value(t.b) ORDER BY [t.o DESC NULLS LAST]
          Aggregate: groupBy=[[t.p]], aggr=[[count(t.a), first_value(named_struct(Utf8("c0"), t.a, Utf8("c1"), t.b)) ORDER BY [t.o DESC NULLS LAST] AS __coalesce_first_last_1]]
            TableScan: t
        "#)
    }

    #[test]
    fn coalesces_peer_last_values() -> Result<()> {
        let plan = LogicalPlanBuilder::from(scan()?)
            .aggregate(
                vec![col("p")],
                vec![
                    last_value(col("a"), order_by_o()),
                    last_value(col("b"), order_by_o()),
                ],
            )?
            .build()?;
        assert_coalesced!(TestConfig::new(), plan, @ r#"
        Projection: t.p, get_field(__coalesce_first_last_1, Utf8("c0")) AS last_value(t.a) ORDER BY [t.o DESC NULLS LAST], get_field(__coalesce_first_last_1, Utf8("c1")) AS last_value(t.b) ORDER BY [t.o DESC NULLS LAST]
          Aggregate: groupBy=[[t.p]], aggr=[[last_value(named_struct(Utf8("c0"), t.a, Utf8("c1"), t.b)) ORDER BY [t.o DESC NULLS LAST] AS __coalesce_first_last_1]]
            TableScan: t
        "#)
    }

    #[test]
    fn no_op_single_first_value() -> Result<()> {
        let plan = LogicalPlanBuilder::from(scan()?)
            .aggregate(
                vec![col("p")],
                vec![first_value(col("a"), order_by_o(), false)],
            )?
            .build()?;
        assert_coalesced!(TestConfig::new(), plan, @ "
        Aggregate: groupBy=[[t.p]], aggr=[[first_value(t.a) ORDER BY [t.o DESC NULLS LAST]]]
          TableScan: t
        ")
    }

    #[test]
    fn no_op_mixed_first_and_last() -> Result<()> {
        let plan = LogicalPlanBuilder::from(scan()?)
            .aggregate(
                vec![col("p")],
                vec![
                    first_value(col("a"), order_by_o(), false),
                    last_value(col("b"), order_by_o()),
                ],
            )?
            .build()?;
        assert_coalesced!(TestConfig::new(), plan, @ "
        Aggregate: groupBy=[[t.p]], aggr=[[first_value(t.a) ORDER BY [t.o DESC NULLS LAST], last_value(t.b) ORDER BY [t.o DESC NULLS LAST]]]
          TableScan: t
        ")
    }

    #[test]
    fn no_op_distinct() -> Result<()> {
        let plan = LogicalPlanBuilder::from(scan()?)
            .aggregate(
                vec![col("p")],
                vec![
                    first_value(col("a"), order_by_o(), true),
                    first_value(col("b"), order_by_o(), true),
                ],
            )?
            .build()?;
        assert_coalesced!(TestConfig::new(), plan, @ "
        Aggregate: groupBy=[[t.p]], aggr=[[first_value(DISTINCT t.a) ORDER BY [t.o DESC NULLS LAST], first_value(DISTINCT t.b) ORDER BY [t.o DESC NULLS LAST]]]
          TableScan: t
        ")
    }

    #[test]
    fn no_op_when_disabled() -> Result<()> {
        let plan = LogicalPlanBuilder::from(scan()?)
            .aggregate(
                vec![col("p")],
                vec![
                    first_value(col("a"), order_by_o(), false),
                    first_value(col("b"), order_by_o(), false),
                ],
            )?
            .build()?;
        let config = OptimizerContext::new();
        let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
            vec![Arc::new(CoalesceFirstLast::new())];
        assert_optimized_plan_eq_snapshot!(config, rules, plan, @ "
        Aggregate: groupBy=[[t.p]], aggr=[[first_value(t.a) ORDER BY [t.o DESC NULLS LAST], first_value(t.b) ORDER BY [t.o DESC NULLS LAST]]]
          TableScan: t
        ")
    }

    #[test]
    fn no_op_without_registry() -> Result<()> {
        let plan = LogicalPlanBuilder::from(scan()?)
            .aggregate(
                vec![col("p")],
                vec![
                    first_value(col("a"), order_by_o(), false),
                    first_value(col("b"), order_by_o(), false),
                ],
            )?
            .build()?;
        assert_coalesced!(TestConfig::without_registry(), plan, @ "
        Aggregate: groupBy=[[t.p]], aggr=[[first_value(t.a) ORDER BY [t.o DESC NULLS LAST], first_value(t.b) ORDER BY [t.o DESC NULLS LAST]]]
          TableScan: t
        ")
    }

    #[test]
    fn no_op_grouping_set() -> Result<()> {
        let plan = LogicalPlanBuilder::from(scan()?)
            .aggregate(
                vec![Expr::GroupingSet(GroupingSet::Cube(vec![col("p")]))],
                vec![
                    first_value(col("a"), order_by_o(), false),
                    first_value(col("b"), order_by_o(), false),
                ],
            )?
            .build()?;
        assert_coalesced!(TestConfig::new(), plan, @ "
        Aggregate: groupBy=[[CUBE (t.p)]], aggr=[[first_value(t.a) ORDER BY [t.o DESC NULLS LAST], first_value(t.b) ORDER BY [t.o DESC NULLS LAST]]]
          TableScan: t
        ")
    }
}
