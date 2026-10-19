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

//! [`SingleDistinctToGroupBy`] replaces `AGG(DISTINCT ..)` with `AGG(..) GROUP BY ..`

use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::{
    DFSchema, DataFusionError, HashSet, Result, assert_or_internal_err,
    tree_node::Transformed,
};
use datafusion_expr::builder::project;
use datafusion_expr::expr::AggregateFunctionParams;
use datafusion_expr::expr_schema::ExprSchemable;
use datafusion_expr::{
    AggregateUDF, Expr, col,
    expr::AggregateFunction,
    lit,
    logical_plan::{Aggregate, LogicalPlan},
    when,
};

/// single distinct to group by optimizer rule
///  ```text
///    Before:
///    SELECT a, count(DISTINCT b), sum(c)
///    FROM t
///    GROUP BY a
///
///    After:
///    SELECT a, count(alias1), sum(alias2)
///    FROM (
///      SELECT a, b as alias1, sum(c) as alias2
///      FROM t
///      GROUP BY a, b
///    )
///    GROUP BY a
///  ```
///
/// A non-distinct `count` may also appear alongside the distinct aggregate. It
/// is the one supported function whose outer phase is a different function: the
/// inner group-by counts rows per `(group, distinct value)` pair, and the outer
/// phase sums those partial counts.
///
///  ```text
///    Before:
///    SELECT a, count(*), count(DISTINCT b)
///    FROM t
///    GROUP BY a
///
///    After:
///    SELECT a,
///           CASE WHEN sum(alias2) IS NOT NULL THEN sum(alias2) ELSE 0 END,
///           count(alias1)
///    FROM (
///      SELECT a, b as alias1, count(*) as alias2
///      FROM t
///      GROUP BY a, b
///    )
///    GROUP BY a
///  ```
///
/// The `CASE` restores the 0 that `count` reports over an empty input, where a
/// `sum` of no rows is NULL.
///
/// See `rewrite_pays_for_count` for when the `count` is allowed.
#[derive(Default, Debug)]
pub struct SingleDistinctToGroupBy {}

const SINGLE_DISTINCT_ALIAS: &str = "alias1";

impl SingleDistinctToGroupBy {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// The pair of functions used to compute a non-distinct `count` in two phases:
/// `count` identifies the aggregates that need the treatment, `sum` combines
/// the partial counts the inner group-by produces.
///
/// Both are resolved from the session's function registry, so a plan built
/// without one keeps the previous behaviour of bailing out on `count`.
struct CountRollup {
    count: Arc<AggregateUDF>,
    sum: Arc<AggregateUDF>,
}

impl CountRollup {
    fn try_new(config: &dyn OptimizerConfig) -> Option<Self> {
        let registry = config.function_registry()?;
        Some(Self {
            count: registry.udaf("count").ok()?,
            sum: registry.udaf("sum").ok()?,
        })
    }

    /// Whether `func` is the registry's `count`, and so decomposes into
    /// `sum` over per-partition counts.
    fn is_count(&self, func: &AggregateUDF) -> bool {
        self.count.as_ref() == func
    }
}

/// Check whether all aggregate exprs are distinct on a single field and whether
/// rewriting the aggregation into a two-phase GROUP BY is advantageous.
///
/// The decision logic follows this cost and parallelism model:
///
/// Does the query have a GROUP BY?
/// |
/// +- No -> Is there more than 1 partition, and is it not count(DISTINCT tinyint/smallint)?
/// |        +- Yes -> REWRITE  (2.6-5.5x faster: the only way the distinct runs in parallel)
/// |        +- No  -> SKIP     (faster and uses less memory)
/// |
/// +- Yes -> Does the distinct aggregate have its own fast grouped accumulator?
///          +- Yes (count(DISTINCT <integer>)) -> SKIP (faster or a tie, less memory)
///          +- No (sum/avg DISTINCT, floats, decimals, strings, user-defined) -> REWRITE (1.4-20x faster)
///                +- config option to skip it when memory matters more (few groups: 2.4-6x more memory)
fn is_single_distinct_agg(
    aggr_expr: &[Expr],
    group_expr: &[Expr],
    input_schema: &DFSchema,
    count_rollup: Option<&CountRollup>,
    target_partitions: usize,
    prefer_memory: bool,
) -> Result<bool> {
    let mut fields_set = HashSet::new();
    let mut aggregate_count = 0;
    let mut distinct_aggs = vec![];
    for expr in aggr_expr {
        if let Expr::AggregateFunction(AggregateFunction {
            func,
            params:
                AggregateFunctionParams {
                    distinct,
                    args,
                    filter,
                    order_by,
                    null_treatment: _,
                },
        }) = expr
        {
            if filter.is_some() || !order_by.is_empty() {
                return Ok(false);
            }
            aggregate_count += 1;
            if *distinct {
                for e in args {
                    fields_set.insert(e);
                }
                distinct_aggs.push((func, args.as_slice()));
            } else if count_rollup.is_some_and(|rollup| rollup.is_count(func))
                || func.name() == "sum"
                || func.name().eq_ignore_ascii_case("min")
                || func.name().eq_ignore_ascii_case("max")
            {
                // Companion non-distinct aggregates
            } else {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }
    }
    if aggregate_count != aggr_expr.len() || fields_set.len() != 1 {
        return Ok(false);
    }

    // Invariant: Distinct aggregates with DistinctHandling::Unsupported (e.g. approx_median)
    // cannot execute without being rewritten into a GROUP BY first.
    if distinct_aggs.iter().any(|(func, _)| {
        func.distinct_handling() == datafusion_expr::DistinctHandling::Unsupported
    }) {
        return Ok(true);
    }

    let has_group_by = !group_expr.is_empty();

    if !has_group_by {
        // Query has NO GROUP BY:
        // Is there more than 1 partition, and is it not count(DISTINCT tinyint/smallint)?
        //   Yes -> REWRITE (2.6-5.5x faster: the only way the distinct runs in parallel)
        //   No  -> SKIP    (faster and uses less memory)
        let is_multi_partition = target_partitions > 1;
        let is_count_distinct_small_int = distinct_aggs.iter().all(|(func, args)| {
            count_rollup.is_some_and(|rollup| rollup.is_count(func))
                && args.len() == 1
                && matches!(
                    args[0]
                        .get_type(input_schema)
                        .unwrap_or(arrow::datatypes::DataType::Null),
                    arrow::datatypes::DataType::Int8
                        | arrow::datatypes::DataType::UInt8
                        | arrow::datatypes::DataType::Int16
                        | arrow::datatypes::DataType::UInt16
                )
        });

        if is_multi_partition && !is_count_distinct_small_int {
            Ok(true)
        } else {
            Ok(false)
        }
    } else {
        // Query HAS a GROUP BY:
        // Does the distinct aggregate have its own fast grouped accumulator?
        //   Yes (count(DISTINCT <integer>)) -> SKIP (faster or a tie, less memory)
        //   No (sum/avg DISTINCT, floats, decimals, strings, user-defined) -> REWRITE (1.4-20x faster)
        //         +- config option to skip it when memory matters more (few groups: 2.4-6x more memory)
        let has_fast_grouped_accumulator = distinct_aggs.iter().all(|(func, args)| {
            let arg_types: Vec<_> = args
                .iter()
                .map(|arg| {
                    arg.get_type(input_schema)
                        .unwrap_or(arrow::datatypes::DataType::Null)
                })
                .collect();
            func.groups_accumulator_supported_for_types(&arg_types, true) == Some(true)
        });

        if has_fast_grouped_accumulator {
            // Fast native grouped accumulator exists (e.g. count(DISTINCT <integer>)).
            // Single-phase grouping is faster or equivalent, and avoids two-phase memory overhead.
            Ok(false)
        } else if prefer_memory {
            // No fast grouped accumulator, but caller opted in to prefer lower memory over throughput.
            Ok(false)
        } else {
            // No fast grouped accumulator (e.g. sum/avg DISTINCT, Utf8 strings).
            // Rewriting into two-phase GROUP BY is 1.4-20x faster by avoiding GroupsAccumulatorAdapter.
            Ok(true)
        }
    }
}

/// Check if the first expr is [Expr::GroupingSet].
fn contains_grouping_set(expr: &[Expr]) -> bool {
    matches!(expr.first(), Some(Expr::GroupingSet(_)))
}

impl OptimizerRule for SingleDistinctToGroupBy {
    fn name(&self) -> &str {
        "single_distinct_aggregation_to_group_by"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        if !matches!(plan, LogicalPlan::Aggregate(_)) {
            return Ok(Transformed::no(plan));
        }
        let prefer_memory = config
            .options()
            .optimizer
            .single_distinct_aggregation_prefer_memory;
        let target_partitions = config.options().execution.target_partitions;
        let count_rollup = CountRollup::try_new(config);
        match plan {
            LogicalPlan::Aggregate(Aggregate {
                input,
                aggr_expr,
                schema,
                group_expr,
                ..
            }) if !contains_grouping_set(&group_expr)
                && is_single_distinct_agg(
                    &aggr_expr,
                    &group_expr,
                    input.schema(),
                    count_rollup.as_ref(),
                    target_partitions,
                    prefer_memory,
                )? =>
            {
                let group_size = group_expr.len();
                // alias all original group_by exprs
                let (mut inner_group_exprs, out_group_expr_with_alias): (
                    Vec<Expr>,
                    Vec<(Expr, _)>,
                ) = group_expr
                    .into_iter()
                    .enumerate()
                    .map(|(i, group_expr)| {
                        if let Expr::Column(_) = group_expr {
                            // For Column expressions we can use existing expression as is.
                            (group_expr.clone(), (group_expr, None))
                        } else {
                            // For complex expression write is as alias, to be able to refer
                            // if from parent operators successfully.
                            // Consider plan below.
                            //
                            // Aggregate: groupBy=[[group_alias_0]], aggr=[[count(alias1)]] [group_alias_0:Int32, count(alias1):Int64;N]\
                            // --Aggregate: groupBy=[[test.a + Int32(1) AS group_alias_0, test.c AS alias1]], aggr=[[]] [group_alias_0:Int32, alias1:UInt32]\
                            // ----TableScan: test [a:UInt32, b:UInt32, c:UInt32]
                            //
                            // First aggregate(from bottom) refers to `test.a` column.
                            // Second aggregate refers to the `group_alias_0` column, Which is a valid field in the first aggregate.

                            // If we were to write plan above as below without alias
                            //
                            // Aggregate: groupBy=[[test.a + Int32(1)]], aggr=[[count(alias1)]] [group_alias_0:Int32, count(alias1):Int64;N]\
                            // --Aggregate: groupBy=[[test.a + Int32(1), test.c AS alias1]], aggr=[[]] [group_alias_0:Int32, alias1:UInt32]\
                            // ----TableScan: test [a:UInt32, b:UInt32, c:UInt32]
                            //
                            // Second aggregate refers to the `test.a + Int32(1)` expression However, its input do not have `test.a` expression in it.
                            let alias_str = format!("group_alias_{i}");
                            let (qualifier, field) = schema.qualified_field(i);
                            (
                                group_expr.alias(alias_str.clone()),
                                (col(alias_str), Some((qualifier, field.name()))),
                            )
                        }
                    })
                    .unzip();

                // replace the distinct arg with alias
                let mut index = 1;
                let mut group_fields_set = HashSet::new();
                let mut inner_aggr_exprs = vec![];
                // Each aggregate yields the expression the outer `Aggregate`
                // computes and the expression the projection above it selects.
                // They differ only for `count`, whose projection restores the
                // zero that `sum` reports as NULL over an empty input.
                let (outer_aggr_exprs, outer_proj_exprs): (Vec<Expr>, Vec<Expr>) = aggr_expr
                    .into_iter()
                    .map(|aggr_expr| match aggr_expr {
                        Expr::AggregateFunction(AggregateFunction {
                            func,
                            params:
                                AggregateFunctionParams {
                                    mut args,
                                    distinct,
                                    filter,
                                    order_by,
                                    null_treatment,
                                },
                        }) => {
                            if distinct {
                                // `is_single_distinct_agg` checked that every
                                // distinct argument in the plan is the same
                                // expression. A call may still take it more than
                                // once, as `corr(DISTINCT x, x)` does, and keeps
                                // its arity here: its distinct argument tuples
                                // are the distinct values of that expression.
                                assert_or_internal_err!(
                                    args.first()
                                        .is_some_and(|f| args.iter().all(|a| a == f)),
                                    "DISTINCT aggregate arguments should be the same expression"
                                );
                                let arity = args.len();
                                let arg = args.swap_remove(0);

                                if group_fields_set.insert(arg.schema_name().to_string())
                                {
                                    inner_group_exprs
                                        .push(arg.alias(SINGLE_DISTINCT_ALIAS));
                                }
                                let outer =
                                    Expr::AggregateFunction(AggregateFunction::new_udf(
                                        func,
                                        vec![col(SINGLE_DISTINCT_ALIAS); arity],
                                        false, // intentional to remove distinct here
                                        filter,
                                        order_by,
                                        null_treatment,
                                    ));
                                Ok((outer.clone(), outer))
                                // if the aggregate function is not distinct, we need to rewrite it like two phase aggregation
                            } else {
                                index += 1;
                                let alias_str = format!("alias{index}");
                                // `count`'s outer phase is `sum`, over the
                                // partial counts the inner group-by produced.
                                let rollup = count_rollup
                                    .as_ref()
                                    .filter(|rollup| rollup.is_count(&func));
                                let outer_func = match rollup {
                                    Some(rollup) => Arc::clone(&rollup.sum),
                                    None => Arc::clone(&func),
                                };
                                inner_aggr_exprs.push(
                                    Expr::AggregateFunction(AggregateFunction::new_udf(
                                        Arc::clone(&func),
                                        args,
                                        false,
                                        filter,
                                        order_by,
                                        null_treatment,
                                    ))
                                    .alias(&alias_str),
                                );
                                let outer =
                                    Expr::AggregateFunction(AggregateFunction::new_udf(
                                        outer_func,
                                        vec![col(&alias_str)],
                                        false,
                                        None,
                                        vec![],
                                        None,
                                    ));
                                if rollup.is_none() {
                                    return Ok((outer.clone(), outer));
                                }
                                // `sum` of no rows is NULL where `count` is 0.
                                // Restoring the 0 also keeps the column
                                // non-nullable, as `count` had it.
                                let proj =
                                    when(outer.clone().is_not_null(), outer.clone())
                                        .otherwise(lit(0_i64))?;
                                Ok((outer, proj))
                            }
                        }
                        _ => Ok((aggr_expr.clone(), aggr_expr)),
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .unzip();

                // construct the inner AggrPlan
                let inner_agg = LogicalPlan::Aggregate(Aggregate::try_new(
                    input,
                    inner_group_exprs,
                    inner_aggr_exprs,
                )?);

                let outer_group_exprs = out_group_expr_with_alias
                    .iter()
                    .map(|(expr, _)| expr.clone())
                    .collect();

                // so the aggregates are displayed in the same way even after the rewrite
                // this optimizer has two kinds of alias:
                // - group_by aggr
                // - aggr expr
                let alias_expr: Vec<_> = out_group_expr_with_alias
                    .into_iter()
                    .map(|(group_expr, original_name)| match original_name {
                        Some((qualifier, name)) => {
                            group_expr.alias_qualified(qualifier.cloned(), name)
                        }
                        None => group_expr,
                    })
                    .chain(outer_proj_exprs.into_iter().enumerate().map(|(idx, expr)| {
                        let idx = idx + group_size;
                        let (qualifier, field) = schema.qualified_field(idx);
                        expr.alias_qualified(qualifier.cloned(), field.name())
                    }))
                    .collect();

                let outer_aggr = LogicalPlan::Aggregate(Aggregate::try_new(
                    Arc::new(inner_agg),
                    outer_group_exprs,
                    outer_aggr_exprs,
                )?);
                Ok(Transformed::yes(project(outer_aggr, alias_expr)?))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_optimized_plan_eq_display_indent_snapshot;
    use crate::test::*;
    use crate::{Optimizer, OptimizerContext};
    use arrow::datatypes::{DataType, Field, Schema};
    use chrono::{DateTime, Utc};
    use datafusion_common::alias::AliasGenerator;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::ExprFunctionExt;
    use datafusion_expr::expr::GroupingSet;
    use datafusion_expr::function::AccumulatorArgs;
    use datafusion_expr::registry::{FunctionRegistry, MemoryFunctionRegistry};
    use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
    use datafusion_expr::{
        lit,
        logical_plan::builder::{LogicalPlanBuilder, table_scan},
    };
    use datafusion_functions_aggregate::correlation::corr_udaf;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::expr_fn::{count, count_distinct, max, min, sum};
    use datafusion_functions_aggregate::min_max::max_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;

    fn max_distinct(expr: Expr) -> Expr {
        Expr::AggregateFunction(AggregateFunction::new_udf(
            max_udaf(),
            vec![expr],
            true,
            None,
            vec![],
            None,
        ))
    }

    fn corr_distinct(y: Expr, x: Expr) -> Expr {
        Expr::AggregateFunction(AggregateFunction::new_udf(
            corr_udaf(),
            vec![y, x],
            true,
            None,
            vec![],
            None,
        ))
    }

    fn sum_distinct(expr: Expr) -> Expr {
        Expr::AggregateFunction(AggregateFunction::new_udf(
            sum_udaf(),
            vec![expr],
            true,
            None,
            vec![],
            None,
        ))
    }

    /// An aggregate that leaves
    /// [`AggregateUDFImpl::groups_accumulator_supported_for_types`] at its
    /// default, which is what almost every function in the wild does.
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct SilentUdaf {
        signature: Signature,
    }

    impl SilentUdaf {
        fn new() -> Self {
            Self {
                signature: Signature::any(1, Volatility::Immutable),
            }
        }
    }

    impl AggregateUDFImpl for SilentUdaf {
        fn name(&self) -> &str {
            "silent"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::UInt32)
        }

        fn accumulator(
            &self,
            _acc_args: AccumulatorArgs,
        ) -> Result<Box<dyn Accumulator>> {
            unimplemented!("not needed for this test")
        }
    }

    fn silent_distinct(expr: Expr) -> Expr {
        Expr::AggregateFunction(AggregateFunction::new_udf(
            Arc::new(AggregateUDF::from(SilentUdaf::new())),
            vec![expr],
            true,
            None,
            vec![],
            None,
        ))
    }

    fn count_star() -> Expr {
        Expr::AggregateFunction(AggregateFunction::new_udf(
            count_udaf(),
            vec![lit(1_i64)],
            false,
            None,
            vec![],
            None,
        ))
    }

    /// `test` with a `Utf8` `b`, the column the distinct aggregate reads.
    ///
    /// `count(DISTINCT b)` over a string has no specialized
    /// `GroupsAccumulator`, so it is the case a non-distinct `count` may join.
    /// The `UInt32` `b` of [`test_table_scan`] is the case it may not.
    fn test_table_scan_utf8_b() -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::UInt32, false),
        ]);
        table_scan(Some("test"), &schema, None)?.build()
    }

    fn test_table_scan_int8_b() -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::Int8, false),
            Field::new("c", DataType::UInt32, false),
        ]);
        table_scan(Some("test"), &schema, None)?.build()
    }

    /// An [`OptimizerConfig`] that resolves functions the way a session does.
    /// The rule needs `count` and `sum` from the registry to rewrite a
    /// non-distinct `count`.
    #[derive(Debug)]
    struct RegistryOptimizerContext {
        inner: OptimizerContext,
        registry: MemoryFunctionRegistry,
    }

    impl RegistryOptimizerContext {
        fn new() -> Self {
            Self::with_prefer_memory(false)
        }

        /// Build a context that exposes a function registry and optionally turns on
        /// `single_distinct_aggregation_prefer_memory` so tests can verify both sides
        /// of the memory / throughput trade-off.
        fn with_prefer_memory(prefer_memory: bool) -> Self {
            Self::with_options(prefer_memory, 4)
        }

        fn with_options(prefer_memory: bool, target_partitions: usize) -> Self {
            let mut registry = MemoryFunctionRegistry::new();
            registry.register_udaf(count_udaf()).unwrap();
            registry.register_udaf(sum_udaf()).unwrap();

            let mut opts = ConfigOptions::new();
            opts.optimizer.single_distinct_aggregation_prefer_memory = prefer_memory;
            opts.execution.target_partitions = target_partitions;

            Self {
                inner: OptimizerContext::new_with_config_options(Arc::new(opts)),
                registry,
            }
        }
    }

    impl OptimizerConfig for RegistryOptimizerContext {
        fn query_execution_start_time(&self) -> Option<DateTime<Utc>> {
            self.inner.query_execution_start_time()
        }

        fn alias_generator(&self) -> &Arc<AliasGenerator> {
            self.inner.alias_generator()
        }

        fn options(&self) -> Arc<ConfigOptions> {
            self.inner.options()
        }

        fn function_registry(&self) -> Option<&dyn FunctionRegistry> {
            Some(&self.registry)
        }
    }

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let rule: Arc<dyn crate::OptimizerRule + Send + Sync> =
                Arc::new(SingleDistinctToGroupBy::new());
            let optimizer = Optimizer::with_rules(vec![rule]);
            let optimized_plan = optimizer
                .optimize($plan, &RegistryOptimizerContext::new(), |_, _| {})
                .expect("failed to optimize plan");
            insta::assert_snapshot!(optimized_plan.display_indent_schema(), @ $expected);

            Ok::<(), DataFusionError>(())
        }};
    }

    macro_rules! assert_optimized_plan_equal_with_context {
        (
            $ctx:expr,
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let rule: Arc<dyn crate::OptimizerRule + Send + Sync> =
                Arc::new(SingleDistinctToGroupBy::new());
            let optimizer = Optimizer::with_rules(vec![rule]);
            let optimized_plan = optimizer
                .optimize($plan, &$ctx, |_, _| {})
                .expect("failed to optimize plan");
            insta::assert_snapshot!(optimized_plan.display_indent_schema(), @ $expected);

            Ok::<(), DataFusionError>(())
        }};
    }

    #[test]
    fn not_exist_distinct() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![max(col("b"))])?
            .build()?;

        // Do nothing
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[]], aggr=[[max(test.b)]] [max(test.b):UInt32;N]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn single_distinct() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![count_distinct(col("b"))])?
            .build()?;

        // Should work
        assert_optimized_plan_equal!(
            plan,
            @"
        Projection: count(alias1) AS count(DISTINCT test.b) [count(DISTINCT test.b):Int64]
          Aggregate: groupBy=[[]], aggr=[[count(alias1)]] [count(alias1):Int64]
            Aggregate: groupBy=[[test.b AS alias1]], aggr=[[]] [alias1:UInt32]
              TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    // Currently this optimization is disabled for CUBE/ROLLUP/GROUPING SET
    #[test]
    fn single_distinct_and_grouping_set() -> Result<()> {
        let table_scan = test_table_scan()?;

        let grouping_set = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
            vec![col("a")],
            vec![col("b")],
        ]));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![grouping_set], vec![count_distinct(col("c"))])?
            .build()?;

        // Should not be optimized
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[GROUPING SETS ((test.a), (test.b))]], aggr=[[count(DISTINCT test.c)]] [a:UInt32;N, b:UInt32;N, __grouping_id:UInt8, count(DISTINCT test.c):Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    // Currently this optimization is disabled for CUBE/ROLLUP/GROUPING SET
    #[test]
    fn single_distinct_and_cube() -> Result<()> {
        let table_scan = test_table_scan()?;

        let grouping_set = Expr::GroupingSet(GroupingSet::Cube(vec![col("a"), col("b")]));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![grouping_set], vec![count_distinct(col("c"))])?
            .build()?;

        // Should not be optimized
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[CUBE (test.a, test.b)]], aggr=[[count(DISTINCT test.c)]] [a:UInt32;N, b:UInt32;N, __grouping_id:UInt8, count(DISTINCT test.c):Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    // Currently this optimization is disabled for CUBE/ROLLUP/GROUPING SET
    #[test]
    fn single_distinct_and_rollup() -> Result<()> {
        let table_scan = test_table_scan()?;

        let grouping_set =
            Expr::GroupingSet(GroupingSet::Rollup(vec![col("a"), col("b")]));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![grouping_set], vec![count_distinct(col("c"))])?
            .build()?;

        // Should not be optimized
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[ROLLUP (test.a, test.b)]], aggr=[[count(DISTINCT test.c)]] [a:UInt32;N, b:UInt32;N, __grouping_id:UInt8, count(DISTINCT test.c):Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn single_distinct_expr() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![count_distinct(lit(2) * col("b"))])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @"
        Projection: count(alias1) AS count(DISTINCT Int32(2) * test.b) [count(DISTINCT Int32(2) * test.b):Int64]
          Aggregate: groupBy=[[]], aggr=[[count(alias1)]] [count(alias1):Int64]
            Aggregate: groupBy=[[Int32(2) * test.b AS alias1]], aggr=[[]] [alias1:Int64]
              TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn single_distinct_and_groupby() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![count_distinct(col("b"))])?
            .build()?;

        // `count(DISTINCT <integer>)` has a specialized fast grouped accumulator:
        // single-phase grouping is faster or equivalent and avoids two-phase memory overhead.
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.a]], aggr=[[count(DISTINCT test.b)]] [a:UInt32, count(DISTINCT test.b):Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn single_distinct_and_groupby_without_fast_accumulator() -> Result<()> {
        let table_scan = test_table_scan_utf8_b()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![count_distinct(col("b"))])?
            .build()?;

        // `count(DISTINCT <string>)` lacks a specialized fast grouped accumulator:
        // rewrites into two-phase GROUP BY to avoid GroupsAccumulatorAdapter.
        assert_optimized_plan_equal!(
            plan,
            @"
        Projection: test.a, count(alias1) AS count(DISTINCT test.b) [a:UInt32, count(DISTINCT test.b):Int64]
          Aggregate: groupBy=[[test.a]], aggr=[[count(alias1)]] [a:UInt32, count(alias1):Int64]
            Aggregate: groupBy=[[test.a, test.b AS alias1]], aggr=[[]] [a:UInt32, alias1:Utf8]
              TableScan: test [a:UInt32, b:Utf8, c:UInt32]
        "
        )
    }

    #[test]
    fn two_distinct_and_groupby() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![count_distinct(col("b")), count_distinct(col("c"))],
            )?
            .build()?;

        // Do nothing
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.a]], aggr=[[count(DISTINCT test.b), count(DISTINCT test.c)]] [a:UInt32, count(DISTINCT test.b):Int64, count(DISTINCT test.c):Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn single_distinct_repeated_arg_and_groupby() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![corr_distinct(col("b"), col("b"))])?
            .build()?;

        // Should work: the call takes the distinct argument twice, so it keeps
        // both arguments after the rewrite.
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a, corr(alias1,alias1) AS corr(DISTINCT test.b,test.b) [a:UInt32, corr(DISTINCT test.b,test.b):Float64;N]
          Aggregate: groupBy=[[test.a]], aggr=[[corr(alias1, alias1)]] [a:UInt32, corr(alias1,alias1):Float64;N]
            Aggregate: groupBy=[[test.a, test.b AS alias1]], aggr=[[]] [a:UInt32, alias1:UInt32]
              TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn single_distinct_two_args_and_groupby() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![corr_distinct(col("b"), col("c"))])?
            .build()?;

        // Do nothing: the arguments are different expressions, so there is no
        // single field to group by.
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.a]], aggr=[[corr(DISTINCT test.b, test.c)]] [a:UInt32, corr(DISTINCT test.b,test.c):Float64;N]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn one_field_two_distinct_and_groupby() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![count_distinct(col("b")), max_distinct(col("b"))],
            )?
            .build()?;

        // Should work
        assert_optimized_plan_equal!(
            plan,
            @"
        Projection: test.a, count(alias1) AS count(DISTINCT test.b), max(alias1) AS max(DISTINCT test.b) [a:UInt32, count(DISTINCT test.b):Int64, max(DISTINCT test.b):UInt32;N]
          Aggregate: groupBy=[[test.a]], aggr=[[count(alias1), max(alias1)]] [a:UInt32, count(alias1):Int64, max(alias1):UInt32;N]
            Aggregate: groupBy=[[test.a, test.b AS alias1]], aggr=[[]] [a:UInt32, alias1:UInt32]
              TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn distinct_and_common() -> Result<()> {
        let table_scan = test_table_scan_utf8_b()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![count_distinct(col("b")), count(col("c"))],
            )?
            .build()?;

        // Should work: the non-distinct count becomes a sum of the counts the
        // inner group-by produces per (test.a, test.b) pair
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a, count(alias1) AS count(DISTINCT test.b), CASE WHEN sum(alias2) IS NOT NULL THEN sum(alias2) ELSE Int64(0) END AS count(test.c) [a:UInt32, count(DISTINCT test.b):Int64, count(test.c):Int64]
          Aggregate: groupBy=[[test.a]], aggr=[[count(alias1), sum(alias2)]] [a:UInt32, count(alias1):Int64, sum(alias2):Int64;N]
            Aggregate: groupBy=[[test.a, test.b AS alias1]], aggr=[[count(test.c) AS alias2]] [a:UInt32, alias1:Utf8, alias2:Int64]
              TableScan: test [a:UInt32, b:Utf8, c:UInt32]
        "
        )
    }

    #[test]
    fn distinct_and_common_over_a_natively_supported_type() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![count_distinct(col("b")), count(col("c"))],
            )?
            .build()?;

        // Should not work: `count(DISTINCT b)` over a `UInt32` has its own
        // `GroupsAccumulator`, so the rewrite would add an inner group-by
        // without taking anything off `GroupsAccumulatorAdapter`
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.a]], aggr=[[count(DISTINCT test.b), count(test.c)]] [a:UInt32, count(DISTINCT test.b):Int64, count(test.c):Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn distinct_over_a_natively_supported_type_without_a_count() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![count_distinct(col("b")), sum(col("c"))],
            )?
            .build()?;

        // Should work: the gate covers only the `count` this change added, so a
        // plan that already qualified through `sum`, `min` or `max` is rewritten
        // exactly as before
        assert_optimized_plan_equal!(
            plan,
            @"
        Aggregate: groupBy=[[test.a]], aggr=[[count(DISTINCT test.b), sum(test.c)]] [a:UInt32, count(DISTINCT test.b):Int64, sum(test.c):UInt64;N]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn distinct_over_a_function_with_no_opinion_rewrites_by_default() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![silent_distinct(col("b")), count(col("c"))],
            )?
            .build()?;

        // User-defined / no-opinion aggregate lacks a fast grouped accumulator:
        // by default it rewrites into two-phase GROUP BY to avoid GroupsAccumulatorAdapter (1.4-20x faster)
        assert_optimized_plan_equal!(
            plan,
            @"
        Projection: test.a, silent(alias1) AS silent(DISTINCT test.b), CASE WHEN sum(alias2) IS NOT NULL THEN sum(alias2) ELSE Int64(0) END AS count(test.c) [a:UInt32, silent(DISTINCT test.b):UInt32;N, count(test.c):Int64]
          Aggregate: groupBy=[[test.a]], aggr=[[silent(alias1), sum(alias2)]] [a:UInt32, silent(alias1):UInt32;N, sum(alias2):Int64;N]
            Aggregate: groupBy=[[test.a, test.b AS alias1]], aggr=[[count(test.c) AS alias2]] [a:UInt32, alias1:UInt32, alias2:Int64]
              TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn distinct_over_a_function_with_no_opinion_prefer_memory_skips() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![silent_distinct(col("b")), count(col("c"))],
            )?
            .build()?;

        let ctx = RegistryOptimizerContext::with_prefer_memory(true);
        assert_optimized_plan_equal_with_context!(
            ctx,
            plan,
            @r"
        Aggregate: groupBy=[[test.a]], aggr=[[silent(DISTINCT test.b), count(test.c)]] [a:UInt32, silent(DISTINCT test.b):UInt32;N, count(test.c):Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn distinct_sum_and_count_rewrites_by_default() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![sum_distinct(col("b")), count(col("c"))],
            )?
            .build()?;

        // `sum(DISTINCT)` lacks a fast grouped accumulator:
        // by default it rewrites into two-phase GROUP BY to avoid GroupsAccumulatorAdapter (1.4-20x faster)
        assert_optimized_plan_equal!(
            plan,
            @"
        Projection: test.a, sum(alias1) AS sum(DISTINCT test.b), CASE WHEN sum(alias2) IS NOT NULL THEN sum(alias2) ELSE Int64(0) END AS count(test.c) [a:UInt32, sum(DISTINCT test.b):UInt64;N, count(test.c):Int64]
          Aggregate: groupBy=[[test.a]], aggr=[[sum(alias1), sum(alias2)]] [a:UInt32, sum(alias1):UInt64;N, sum(alias2):Int64;N]
            Aggregate: groupBy=[[test.a, test.b AS alias1]], aggr=[[count(test.c) AS alias2]] [a:UInt32, alias1:UInt32, alias2:Int64]
              TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn distinct_sum_and_count_prefer_memory_skips() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![sum_distinct(col("b")), count(col("c"))],
            )?
            .build()?;

        let ctx = RegistryOptimizerContext::with_prefer_memory(true);
        assert_optimized_plan_equal_with_context!(
            ctx,
            plan,
            @r"
        Aggregate: groupBy=[[test.a]], aggr=[[sum(DISTINCT test.b), count(test.c)]] [a:UInt32, sum(DISTINCT test.b):UInt64;N, count(test.c):Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn group_by_with_expr() -> Result<()> {
        let table_scan = test_table_scan_utf8_b().unwrap();

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a") + lit(1)], vec![count_distinct(col("b"))])?
            .build()?;

        // Should work
        assert_optimized_plan_equal!(
            plan,
            @"
        Projection: group_alias_0 AS test.a + Int32(1), count(alias1) AS count(DISTINCT test.b) [test.a + Int32(1):Int64, count(DISTINCT test.b):Int64]
          Aggregate: groupBy=[[group_alias_0]], aggr=[[count(alias1)]] [group_alias_0:Int64, count(alias1):Int64]
            Aggregate: groupBy=[[test.a + Int32(1) AS group_alias_0, test.b AS alias1]], aggr=[[]] [group_alias_0:Int64, alias1:Utf8]
              TableScan: test [a:UInt32, b:Utf8, c:UInt32]
        "
        )
    }

    #[test]
    fn two_distinct_and_one_common() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    sum(col("c")),
                    count_distinct(col("b")),
                    max_distinct(col("b")),
                ],
            )?
            .build()?;

        // `max(DISTINCT b)` lacks a fast grouped accumulator: rewrites into two-phase GROUP BY
        assert_optimized_plan_equal!(
            plan,
            @"
        Projection: test.a, sum(alias2) AS sum(test.c), count(alias1) AS count(DISTINCT test.b), max(alias1) AS max(DISTINCT test.b) [a:UInt32, sum(test.c):UInt64;N, count(DISTINCT test.b):Int64, max(DISTINCT test.b):UInt32;N]
          Aggregate: groupBy=[[test.a]], aggr=[[sum(alias2), count(alias1), max(alias1)]] [a:UInt32, sum(alias2):UInt64;N, count(alias1):Int64, max(alias1):UInt32;N]
            Aggregate: groupBy=[[test.a, test.b AS alias1]], aggr=[[sum(test.c) AS alias2]] [a:UInt32, alias1:UInt32, alias2:UInt64;N]
              TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn no_groupby_multi_partition_rewrites() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![count_distinct(col("b"))])?
            .build()?;

        let ctx = RegistryOptimizerContext::with_options(false, 4);
        assert_optimized_plan_equal_with_context!(
            ctx,
            plan,
            @"
        Projection: count(alias1) AS count(DISTINCT test.b) [count(DISTINCT test.b):Int64]
          Aggregate: groupBy=[[]], aggr=[[count(alias1)]] [count(alias1):Int64]
            Aggregate: groupBy=[[test.b AS alias1]], aggr=[[]] [alias1:UInt32]
              TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn no_groupby_single_partition_skips() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![count_distinct(col("b"))])?
            .build()?;

        let ctx = RegistryOptimizerContext::with_options(false, 1);
        assert_optimized_plan_equal_with_context!(
            ctx,
            plan,
            @r"
        Aggregate: groupBy=[[]], aggr=[[count(DISTINCT test.b)]] [count(DISTINCT test.b):Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn no_groupby_small_int_skips() -> Result<()> {
        let table_scan = test_table_scan_int8_b()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![count_distinct(col("b"))])?
            .build()?;

        let ctx = RegistryOptimizerContext::with_options(false, 4);
        assert_optimized_plan_equal_with_context!(
            ctx,
            plan,
            @r"
        Aggregate: groupBy=[[]], aggr=[[count(DISTINCT test.b)]] [count(DISTINCT test.b):Int64]
          TableScan: test [a:UInt32, b:Int8, c:UInt32]
        "
        )
    }

    #[test]
    fn one_distinct_and_two_common() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![sum(col("c")), max(col("c")), count_distinct(col("b"))],
            )?
            .build()?;

        // Should work
        assert_optimized_plan_equal!(
            plan,
            @"
        Aggregate: groupBy=[[test.a]], aggr=[[sum(test.c), max(test.c), count(DISTINCT test.b)]] [a:UInt32, sum(test.c):UInt64;N, max(test.c):UInt32;N, count(DISTINCT test.b):Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn one_distinct_and_one_common() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("c")],
                vec![min(col("a")), count_distinct(col("b"))],
            )?
            .build()?;

        // Should work
        assert_optimized_plan_equal!(
            plan,
            @"
        Aggregate: groupBy=[[test.c]], aggr=[[min(test.a), count(DISTINCT test.b)]] [c:UInt32, min(test.a):UInt32;N, count(DISTINCT test.b):Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn common_with_filter() -> Result<()> {
        let table_scan = test_table_scan()?;

        // sum(a) FILTER (WHERE a > 5)
        let expr = Expr::AggregateFunction(AggregateFunction::new_udf(
            sum_udaf(),
            vec![col("a")],
            false,
            Some(Box::new(col("a").gt(lit(5)))),
            vec![],
            None,
        ));
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], vec![expr, count_distinct(col("b"))])?
            .build()?;

        // Do nothing
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.c]], aggr=[[sum(test.a) FILTER (WHERE test.a > Int32(5)), count(DISTINCT test.b)]] [c:UInt32, sum(test.a) FILTER (WHERE test.a > Int32(5)):UInt64;N, count(DISTINCT test.b):Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn distinct_with_filter() -> Result<()> {
        let table_scan = test_table_scan()?;

        // count(DISTINCT a) FILTER (WHERE a > 5)
        let expr = count_udaf()
            .call(vec![col("a")])
            .distinct()
            .filter(col("a").gt(lit(5)))
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], vec![sum(col("a")), expr])?
            .build()?;

        // Do nothing
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.c]], aggr=[[sum(test.a), count(DISTINCT test.a) FILTER (WHERE test.a > Int32(5))]] [c:UInt32, sum(test.a):UInt64;N, count(DISTINCT test.a) FILTER (WHERE test.a > Int32(5)):Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn common_with_order_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        // SUM(a ORDER BY a)
        let expr = Expr::AggregateFunction(AggregateFunction::new_udf(
            sum_udaf(),
            vec![col("a")],
            false,
            None,
            vec![col("a").sort(true, false)],
            None,
        ));
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], vec![expr, count_distinct(col("b"))])?
            .build()?;

        // Do nothing
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.c]], aggr=[[sum(test.a) ORDER BY [test.a ASC NULLS LAST], count(DISTINCT test.b)]] [c:UInt32, sum(test.a) ORDER BY [test.a ASC NULLS LAST]:UInt64;N, count(DISTINCT test.b):Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn distinct_with_order_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        // count(DISTINCT a ORDER BY a)
        let expr = count_udaf()
            .call(vec![col("a")])
            .distinct()
            .order_by(vec![col("a").sort(true, false)])
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], vec![sum(col("a")), expr])?
            .build()?;

        // Do nothing
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.c]], aggr=[[sum(test.a), count(DISTINCT test.a) ORDER BY [test.a ASC NULLS LAST]]] [c:UInt32, sum(test.a):UInt64;N, count(DISTINCT test.a) ORDER BY [test.a ASC NULLS LAST]:Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn aggregate_with_filter_and_order_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        // count(DISTINCT a ORDER BY a) FILTER (WHERE a > 5)
        let expr = count_udaf()
            .call(vec![col("a")])
            .distinct()
            .filter(col("a").gt(lit(5)))
            .order_by(vec![col("a").sort(true, false)])
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], vec![sum(col("a")), expr])?
            .build()?;

        // Do nothing
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.c]], aggr=[[sum(test.a), count(DISTINCT test.a) FILTER (WHERE test.a > Int32(5)) ORDER BY [test.a ASC NULLS LAST]]] [c:UInt32, sum(test.a):UInt64;N, count(DISTINCT test.a) FILTER (WHERE test.a > Int32(5)) ORDER BY [test.a ASC NULLS LAST]:Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn count_star_and_distinct_without_groupby() -> Result<()> {
        let table_scan = test_table_scan_utf8_b()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                Vec::<Expr>::new(),
                vec![count_star(), count_distinct(col("b"))],
            )?
            .build()?;

        // Should work. Without a group by the outer aggregate sees no rows at
        // all for an empty input, so the projection has to turn the NULL that
        // `sum` reports there back into the 0 `count` reports.
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: CASE WHEN sum(alias2) IS NOT NULL THEN sum(alias2) ELSE Int64(0) END AS count(Int64(1)), count(alias1) AS count(DISTINCT test.b) [count(Int64(1)):Int64, count(DISTINCT test.b):Int64]
          Aggregate: groupBy=[[]], aggr=[[sum(alias2), count(alias1)]] [sum(alias2):Int64;N, count(alias1):Int64]
            Aggregate: groupBy=[[test.b AS alias1]], aggr=[[count(Int64(1)) AS alias2]] [alias1:Utf8, alias2:Int64]
              TableScan: test [a:UInt32, b:Utf8, c:UInt32]
        "
        )
    }

    #[test]
    fn count_star_min_max_sum_and_distinct_with_groupby() -> Result<()> {
        let table_scan = test_table_scan_utf8_b()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    count_star(),
                    count_distinct(col("b")),
                    min(col("c")),
                    max(col("c")),
                    sum(col("c")),
                ],
            )?
            .build()?;

        // Should work: this is the shape a `count(*)` alongside a
        // `count(DISTINCT ..)`, a min, a max and a sum produces
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a, CASE WHEN sum(alias2) IS NOT NULL THEN sum(alias2) ELSE Int64(0) END AS count(Int64(1)), count(alias1) AS count(DISTINCT test.b), min(alias3) AS min(test.c), max(alias4) AS max(test.c), sum(alias5) AS sum(test.c) [a:UInt32, count(Int64(1)):Int64, count(DISTINCT test.b):Int64, min(test.c):UInt32;N, max(test.c):UInt32;N, sum(test.c):UInt64;N]
          Aggregate: groupBy=[[test.a]], aggr=[[sum(alias2), count(alias1), min(alias3), max(alias4), sum(alias5)]] [a:UInt32, sum(alias2):Int64;N, count(alias1):Int64, min(alias3):UInt32;N, max(alias4):UInt32;N, sum(alias5):UInt64;N]
            Aggregate: groupBy=[[test.a, test.b AS alias1]], aggr=[[count(Int64(1)) AS alias2, min(test.c) AS alias3, max(test.c) AS alias4, sum(test.c) AS alias5]] [a:UInt32, alias1:Utf8, alias2:Int64, alias3:UInt32;N, alias4:UInt32;N, alias5:UInt64;N]
              TableScan: test [a:UInt32, b:Utf8, c:UInt32]
        "
        )
    }

    #[test]
    fn count_with_filter_is_not_rewritten() -> Result<()> {
        let table_scan = test_table_scan()?;

        // count(a) FILTER (WHERE a > 5)
        let expr = count_udaf()
            .call(vec![col("a")])
            .filter(col("a").gt(lit(5)))
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], vec![expr, count_distinct(col("b"))])?
            .build()?;

        // Do nothing: the filter would have to be applied per input row, but
        // the inner aggregate has already collapsed them
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.c]], aggr=[[count(test.a) FILTER (WHERE test.a > Int32(5)), count(DISTINCT test.b)]] [c:UInt32, count(test.a) FILTER (WHERE test.a > Int32(5)):Int64, count(DISTINCT test.b):Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn count_with_order_by_is_not_rewritten() -> Result<()> {
        let table_scan = test_table_scan()?;

        // count(a ORDER BY a)
        let expr = count_udaf()
            .call(vec![col("a")])
            .order_by(vec![col("a").sort(true, false)])
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], vec![expr, count_distinct(col("b"))])?
            .build()?;

        // Do nothing
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.c]], aggr=[[count(test.a) ORDER BY [test.a ASC NULLS LAST], count(DISTINCT test.b)]] [c:UInt32, count(test.a) ORDER BY [test.a ASC NULLS LAST]:Int64, count(DISTINCT test.b):Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn count_without_function_registry_is_not_rewritten() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![count_star(), count_distinct(col("b"))])?
            .build()?;

        // Do nothing: without a registry the rule cannot resolve the `sum` the
        // outer phase needs
        let rule: Arc<dyn OptimizerRule + Send + Sync> =
            Arc::new(SingleDistinctToGroupBy::new());
        assert_optimized_plan_eq_display_indent_snapshot!(
            rule,
            plan,
            @r"
        Aggregate: groupBy=[[test.a]], aggr=[[count(Int64(1)), count(DISTINCT test.b)]] [a:UInt32, count(Int64(1)):Int64, count(DISTINCT test.b):Int64]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        ",
        )
    }

    #[test]
    fn test_unsupported_distinct_handling_is_rewritten() {
        let table_scan = test_table_scan().unwrap();

        // approx_median returns DistinctHandling::Unsupported
        let aggr_expr = Expr::AggregateFunction(AggregateFunction::new_udf(
            datafusion_functions_aggregate::approx_median::approx_median_udaf(),
            vec![col("b")],
            true,
            None,
            vec![],
            None,
        ));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![aggr_expr, sum(col("c"))])
            .unwrap()
            .build()
            .unwrap();

        // The rule should rewrite the plan because approx_median doesn't support DISTINCT natively,
        // so it requires SingleDistinctToGroupBy to deduplicate the input even when accompanied
        // by non-distinct aggregates.
        let rule: Arc<dyn OptimizerRule + Send + Sync> =
            Arc::new(SingleDistinctToGroupBy::new());
        assert_optimized_plan_eq_display_indent_snapshot!(
            rule,
            plan,
            @r"
        Projection: test.a, approx_median(alias1) AS approx_median(DISTINCT test.b), sum(alias2) AS sum(test.c) [a:UInt32, approx_median(DISTINCT test.b):Float64;N, sum(test.c):UInt64;N]
          Aggregate: groupBy=[[test.a]], aggr=[[approx_median(alias1), sum(alias2)]] [a:UInt32, approx_median(alias1):Float64;N, sum(alias2):UInt64;N]
            Aggregate: groupBy=[[test.a, test.b AS alias1]], aggr=[[sum(test.c) AS alias2]] [a:UInt32, alias1:UInt32, alias2:UInt64;N]
              TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        ",
        ).unwrap();
    }

    #[test]
    fn single_distinct_aggregation_prefer_memory_default_rewrites_lone_distinct()
    -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum_distinct(col("b"))])?
            .build()?;

        let ctx = RegistryOptimizerContext::with_prefer_memory(false);
        assert_optimized_plan_equal_with_context!(
            ctx,
            plan,
            @r"
        Projection: test.a, sum(alias1) AS sum(DISTINCT test.b) [a:UInt32, sum(DISTINCT test.b):UInt64;N]
          Aggregate: groupBy=[[test.a]], aggr=[[sum(alias1)]] [a:UInt32, sum(alias1):UInt64;N]
            Aggregate: groupBy=[[test.a, test.b AS alias1]], aggr=[[]] [a:UInt32, alias1:UInt32]
              TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn single_distinct_aggregation_prefer_memory_true_skips_rewrite_for_groups_accumulator()
    -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum_distinct(col("b"))])?
            .build()?;

        let ctx = RegistryOptimizerContext::with_prefer_memory(true);
        assert_optimized_plan_equal_with_context!(
            ctx,
            plan,
            @r"
        Aggregate: groupBy=[[test.a]], aggr=[[sum(DISTINCT test.b)]] [a:UInt32, sum(DISTINCT test.b):UInt64;N]
          TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn single_distinct_aggregation_prefer_memory_true_still_rewrites_unsupported()
    -> Result<()> {
        let table_scan = test_table_scan()?;

        let aggr_expr = Expr::AggregateFunction(AggregateFunction::new_udf(
            datafusion_functions_aggregate::approx_median::approx_median_udaf(),
            vec![col("b")],
            true,
            None,
            vec![],
            None,
        ));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![aggr_expr])?
            .build()?;

        let ctx = RegistryOptimizerContext::with_prefer_memory(true);
        assert_optimized_plan_equal_with_context!(
            ctx,
            plan,
            @r"
        Projection: test.a, approx_median(alias1) AS approx_median(DISTINCT test.b) [a:UInt32, approx_median(DISTINCT test.b):Float64;N]
          Aggregate: groupBy=[[test.a]], aggr=[[approx_median(alias1)]] [a:UInt32, approx_median(alias1):Float64;N]
            Aggregate: groupBy=[[test.a, test.b AS alias1]], aggr=[[]] [a:UInt32, alias1:UInt32]
              TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }
}
