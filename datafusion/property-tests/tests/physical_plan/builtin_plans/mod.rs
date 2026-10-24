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

//! Runs all checks against the `ExecutionPlan`s defined in DataFusion and
//! records the findings in snapshots, one per family of operators: simple
//! operators, aggregates, joins, windows, and sources and sinks.
//!
//! Every plan is a `PlanFactory`: a function that builds the plan from its
//! inputs. The `PlanHarness` generates the inputs, meets the plan's
//! `required_input_ordering` and `input_distribution_requirements`, and
//! checks every case of `Profile::defaults`. With the `extended_tests`
//! feature, the cases of `Profile::extended` are also checked, and recorded
//! in separate `_extended` snapshots. Plans are built the way the physical
//! planner and the optimizer build them. A source or a sink is built from the
//! rows of a generated input, so that it is checked with every layout of the
//! profiles.
//!
//! The snapshots are the list of known violations in the built-in plans. When
//! a plan is fixed, or a new check finds a new problem, a snapshot changes
//! and must be reviewed and updated with `cargo insta review` (or by running
//! the test with `INSTA_UPDATE=always`).
//!
//! Known findings are also listed, with their causes, in
//! `docs/physical_plan/IMPLEMENTATION_STATUS.md`, as are checks allowed for
//! specific plans.

use std::fmt::Write;
use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{
    JoinSide, JoinType, NullEquality, Result, ScalarValue, internal_datafusion_err,
};
use datafusion_expr::{AggregateUDF, Operator};
use datafusion_functions_aggregate::average::avg_udaf;
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal, col};
use datafusion_physical_expr::{
    LexOrdering, Partitioning, PhysicalExpr, PhysicalSortExpr,
};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, LimitOptions, PhysicalGroupBy,
};
use datafusion_physical_plan::buffer::BufferExec;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::coop::CooperativeExec;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter, JoinOn};
use datafusion_physical_plan::joins::{
    AsOfJoinExec, AsOfMatchExpr, CrossJoinExec, HashJoinExec, NestedLoopJoinExec,
    PartitionMode, PiecewiseMergeJoinExec, SortMergeJoinExec, StreamJoinPartitionMode,
    SymmetricHashJoinExec,
};
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_property_tests::physical_plan::fixtures::SourceSpec;
use datafusion_property_tests::physical_plan::harness::{
    PlanFactory, PlanHarness, Profile,
};

mod operators;
mod sources;
mod windows;

type Plan = Arc<dyn ExecutionPlan>;

const FETCH: usize = 10;

/// Check every factory on the cases of `profiles` and render the reports.
///
/// Runs on a current-thread runtime (see the tests) so that execution, and
/// therefore the snapshot, is deterministic. Some plans, such as a
/// partitioned TopK `SortExec`, produce output that depends on how partitions
/// interleave.
async fn audit(factories: Vec<PlanFactory>, profiles: Vec<Profile>) -> Result<String> {
    let harness = PlanHarness::new().with_profiles(profiles);
    let mut output = String::new();
    for factory in factories {
        let report = harness.check_async(&factory).await?;
        writeln!(output, "## {report}\n").unwrap();
    }
    Ok(output)
}

/// A factory for a plan with one input generated from `spec`
fn one_input<F>(name: &str, spec: SourceSpec, create: F) -> PlanFactory
where
    F: Fn(Plan) -> Result<Plan> + Send + Sync + 'static,
{
    PlanFactory::new(name, vec![spec], move |mut inputs| create(inputs.remove(0)))
}

/// `plan.with_fetch(Some(fetch))`, which must return a plan
fn with_fetch(plan: &dyn ExecutionPlan, fetch: usize) -> Result<Plan> {
    let name = plan.name().to_string();
    plan.with_fetch(Some(fetch))
        .ok_or_else(|| internal_datafusion_err!("{name} does not support a fetch"))
}

/// The ascending ordering on `column` of `input`
fn ordering_on(column: &str, input: &Plan) -> Result<LexOrdering> {
    Ok(LexOrdering::new(vec![PhysicalSortExpr::new_default(col(
        column,
        &input.schema(),
    )?)])
    .unwrap())
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Boolean, false),
        Field::new("c", DataType::Utf8, true),
    ]))
}

/// The input of every simple operator: 600 rows of `schema`
fn spec() -> SourceSpec {
    SourceSpec::new(schema()).with_num_rows(600)
}

/// [`spec`], with every partition sorted on `a`
fn sorted_spec() -> Result<SourceSpec> {
    let ordering =
        LexOrdering::new(vec![PhysicalSortExpr::new_default(col("a", &schema())?)])
            .unwrap();
    Ok(spec().with_ordering(ordering))
}

#[expect(deprecated)]
fn coalesce_batches(input: Plan, fetch: Option<usize>) -> Plan {
    use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
    Arc::new(CoalesceBatchesExec::new(input, 8192).with_fetch(fetch))
}

/// The simple operators under test
fn builtin_plans() -> Result<Vec<PlanFactory>> {
    let mut factories = simple_plans()?;
    factories.extend(operators::operator_plans()?);
    Ok(factories)
}

/// Sorts, limits, repartitions, projections, filters and the like
fn simple_plans() -> Result<Vec<PlanFactory>> {
    Ok(vec![
        PlanFactory::new("EmptyExec", vec![], |_| {
            Ok(Arc::new(EmptyExec::new(schema())) as Plan)
        }),
        PlanFactory::new("PlaceholderRowExec", vec![], |_| {
            Ok(Arc::new(PlaceholderRowExec::new(schema())) as Plan)
        }),
        one_input("ProjectionExec", spec(), |input| {
            let a = col("a", &input.schema())?;
            Ok(Arc::new(ProjectionExec::try_new(
                vec![(a, "a".to_string())],
                input,
            )?))
        }),
        one_input("ProjectionExec with a literal", spec(), |input| {
            let a = col("a", &input.schema())?;
            let one = Arc::new(Literal::new(ScalarValue::Int64(Some(1))));
            Ok(Arc::new(ProjectionExec::try_new(
                vec![(a, "a".to_string()), (one, "one".to_string())],
                input,
            )?))
        }),
        one_input("FilterExec", spec(), |input| {
            let b = col("b", &input.schema())?;
            Ok(Arc::new(FilterExec::try_new(b, input)?))
        }),
        one_input("FilterExec with fetch", spec(), |input| {
            let b = col("b", &input.schema())?;
            with_fetch(&FilterExec::try_new(b, input)?, FETCH)
        }),
        // Makes `a` a constant with the value 3 in every partition
        one_input("FilterExec on an equality", spec(), |input| {
            let a = col("a", &input.schema())?;
            let three = Arc::new(Literal::new(ScalarValue::Int32(Some(3))));
            let predicate = Arc::new(BinaryExpr::new(a, Operator::Eq, three));
            Ok(Arc::new(FilterExec::try_new(predicate, input)?))
        })
        .allow(
            "emission_type_holds",
            "the filter keeps one row in 16 and emits batches of 8192 rows \
             (filter.rs:81), so it needs about 131072 rows per input partition \
             before it emits, more than the unbounded input delivers (65536)",
        ),
        one_input("CoalesceBatchesExec", spec(), |input| {
            Ok(coalesce_batches(input, None))
        }),
        one_input("CoalesceBatchesExec with fetch", spec(), |input| {
            Ok(coalesce_batches(input, Some(FETCH)))
        }),
        one_input("CoalescePartitionsExec", spec(), |input| {
            Ok(Arc::new(CoalescePartitionsExec::new(input)))
        }),
        one_input("CoalescePartitionsExec with fetch", spec(), |input| {
            Ok(Arc::new(
                CoalescePartitionsExec::new(input).with_fetch(Some(FETCH)),
            ))
        }),
        one_input("SortExec", spec(), |input| {
            Ok(Arc::new(SortExec::new(ordering_on("a", &input)?, input)))
        }),
        one_input("SortExec with fetch", spec(), |input| {
            Ok(Arc::new(
                SortExec::new(ordering_on("a", &input)?, input).with_fetch(Some(FETCH)),
            ))
        }),
        one_input("SortExec on a nullable column", spec(), |input| {
            Ok(Arc::new(SortExec::new(ordering_on("c", &input)?, input)))
        }),
        one_input("SortExec on sorted input", sorted_spec()?, |input| {
            Ok(Arc::new(SortExec::new(ordering_on("a", &input)?, input)))
        }),
        one_input(
            "SortExec with fetch on sorted input",
            sorted_spec()?,
            |input| {
                Ok(Arc::new(
                    SortExec::new(ordering_on("a", &input)?, input)
                        .with_fetch(Some(FETCH)),
                ))
            },
        ),
        one_input("SortExec with preserve_partitioning", spec(), |input| {
            Ok(Arc::new(
                SortExec::new(ordering_on("a", &input)?, input)
                    .with_preserve_partitioning(true),
            ))
        }),
        one_input(
            "SortExec with preserve_partitioning and fetch",
            spec(),
            |input| {
                Ok(Arc::new(
                    SortExec::new(ordering_on("a", &input)?, input)
                        .with_preserve_partitioning(true)
                        .with_fetch(Some(FETCH)),
                ))
            },
        ),
        one_input("SortPreservingMergeExec", spec(), |input| {
            Ok(Arc::new(SortPreservingMergeExec::new(
                ordering_on("a", &input)?,
                input,
            )))
        }),
        one_input("SortPreservingMergeExec with fetch", spec(), |input| {
            Ok(Arc::new(
                SortPreservingMergeExec::new(ordering_on("a", &input)?, input)
                    .with_fetch(Some(FETCH)),
            ))
        }),
        one_input("RepartitionExec round robin", spec(), |input| {
            Ok(Arc::new(RepartitionExec::try_new(
                input,
                Partitioning::RoundRobinBatch(4),
            )?))
        }),
        one_input("RepartitionExec hash", spec(), |input| {
            let a = col("a", &input.schema())?;
            Ok(Arc::new(RepartitionExec::try_new(
                input,
                Partitioning::Hash(vec![a], 4),
            )?))
        }),
        // Merges the sorted input partitions it sends to each output
        // partition. Only preserves order when the input reports an ordering
        // and has more than one partition.
        one_input(
            "RepartitionExec round robin preserving order",
            sorted_spec()?,
            |input| {
                Ok(Arc::new(
                    RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(4))?
                        .with_preserve_order(),
                ))
            },
        ),
        one_input(
            "RepartitionExec hash preserving order",
            sorted_spec()?,
            |input| {
                let a = col("a", &input.schema())?;
                Ok(Arc::new(
                    RepartitionExec::try_new(input, Partitioning::Hash(vec![a], 4))?
                        .with_preserve_order(),
                ))
            },
        ),
        one_input("GlobalLimitExec", spec(), |input| {
            Ok(Arc::new(GlobalLimitExec::new(input, 5, Some(FETCH))))
        }),
        one_input("GlobalLimitExec without fetch", spec(), |input| {
            Ok(Arc::new(GlobalLimitExec::new(input, 5, None)))
        }),
        one_input("LocalLimitExec", spec(), |input| {
            Ok(Arc::new(LocalLimitExec::new(input, FETCH)))
        }),
        PlanFactory::new("UnionExec", vec![spec(), spec()], UnionExec::try_new),
        one_input("BufferExec", spec(), |input| {
            Ok(Arc::new(BufferExec::new(input, 1024)))
        }),
        one_input("CooperativeExec", spec(), |input| {
            Ok(Arc::new(CooperativeExec::new(input)))
        }),
    ])
}

/// Partitions of the hash repartition between two aggregation phases
const HASH_PARTITIONS: usize = 3;

/// Two grouping keys, one of them nullable, and two value columns
fn aggregate_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int32, false),
        Field::new("h", DataType::Utf8, true),
        Field::new("v", DataType::Int64, true),
        Field::new("f", DataType::Float64, true),
    ]))
}

/// 600 rows of `aggregate_schema`
fn aggregate_spec() -> SourceSpec {
    SourceSpec::new(aggregate_schema()).with_num_rows(600)
}

/// [`aggregate_spec`], with every partition sorted on `g`
fn aggregate_spec_sorted_on_g() -> Result<SourceSpec> {
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(col(
        "g",
        &aggregate_schema(),
    )?)])
    .unwrap();
    Ok(aggregate_spec().with_ordering(ordering))
}

/// Many distinct values, so that few groups tie on `max(v)` and the groups a
/// TopK aggregate keeps are well defined, or so that there are more groups
/// than rows
fn many_values(rows: usize) -> SourceSpec {
    aggregate_spec()
        .with_distinct_values(1000)
        .with_num_rows(rows)
}

/// `udaf(arg) AS alias`, with `arg` a column of `input`
fn aggregate(
    udaf: Arc<AggregateUDF>,
    arg: &str,
    alias: &str,
    input: &Plan,
) -> Result<Arc<AggregateFunctionExpr>> {
    let schema = input.schema();
    Ok(Arc::new(
        AggregateExprBuilder::new(udaf, vec![col(arg, &schema)?])
            .schema(schema)
            .alias(alias)
            .build()?,
    ))
}

/// `count(v), sum(v), min(v), max(v), avg(f), sum(f)`
fn all_aggregates(input: &Plan) -> Result<Vec<Arc<AggregateFunctionExpr>>> {
    Ok(vec![
        aggregate(count_udaf(), "v", "count(v)", input)?,
        aggregate(sum_udaf(), "v", "sum(v)", input)?,
        aggregate(min_udaf(), "v", "min(v)", input)?,
        aggregate(max_udaf(), "v", "max(v)", input)?,
        aggregate(avg_udaf(), "f", "avg(f)", input)?,
        aggregate(sum_udaf(), "f", "sum(f)", input)?,
    ])
}

/// `min(v), max(v)`
fn min_max(input: &Plan) -> Result<Vec<Arc<AggregateFunctionExpr>>> {
    Ok(vec![
        aggregate(min_udaf(), "v", "min(v)", input)?,
        aggregate(max_udaf(), "v", "max(v)", input)?,
    ])
}

/// `GROUP BY` the given columns of `input`
fn group_by(columns: &[&str], input: &Plan) -> Result<PhysicalGroupBy> {
    let schema = input.schema();
    Ok(PhysicalGroupBy::new_single(
        columns
            .iter()
            .map(|name| Ok((col(name, &schema)?, name.to_string())))
            .collect::<Result<_>>()?,
    ))
}

/// No `GROUP BY`, as the physical planner builds it
fn no_group_by() -> PhysicalGroupBy {
    PhysicalGroupBy::new(vec![], vec![], vec![], false)
}

/// `GROUP BY ROLLUP (g, h)`, as the physical planner expands it
fn rollup_g_h(input: &Plan) -> Result<PhysicalGroupBy> {
    let schema = input.schema();
    let null = |data_type: &DataType| -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Literal::new(ScalarValue::try_from(data_type)?)))
    };
    Ok(PhysicalGroupBy::new(
        vec![
            (col("g", &schema)?, "g".to_string()),
            (col("h", &schema)?, "h".to_string()),
        ],
        vec![
            (null(&DataType::Int32)?, "g".to_string()),
            (null(&DataType::Utf8)?, "h".to_string()),
        ],
        vec![vec![true, true], vec![false, true], vec![false, false]],
        true,
    ))
}

/// A one-phase aggregate in `mode` (`Single` or `SinglePartitioned`)
fn single_phase(
    mode: AggregateMode,
    group_by: PhysicalGroupBy,
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    input: Plan,
    limit: Option<LimitOptions>,
) -> Result<Plan> {
    let filters = vec![None; aggr_expr.len()];
    let input_schema = input.schema();
    Ok(Arc::new(
        AggregateExec::try_new(mode, group_by, aggr_expr, filters, input, input_schema)?
            .with_limit_options(limit),
    ))
}

/// How the output of a partial aggregate reaches the final aggregate
#[derive(Debug, Clone, Copy)]
enum Exchange {
    /// `CoalescePartitionsExec` and a `Final` aggregate
    Coalesce,
    /// `RepartitionExec` hashing on the grouping keys, optionally preserving
    /// the order of its input, and a `FinalPartitioned` aggregate
    Hash { preserve_order: bool },
}

/// A two-phase aggregate, as the physical planner builds it and
/// `EnforceDistribution` connects the phases. `limit` is set on both phases,
/// as `TopKAggregation` does. The exchange between the phases is part of the
/// plan under test, not an input.
fn two_phase(
    group_by: PhysicalGroupBy,
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    input: Plan,
    exchange: Exchange,
    limit: Option<LimitOptions>,
) -> Result<Plan> {
    let filters = vec![None; aggr_expr.len()];
    let input_schema = input.schema();
    let partial = AggregateExec::try_new(
        AggregateMode::Partial,
        group_by,
        aggr_expr,
        filters.clone(),
        input,
        Arc::clone(&input_schema),
    )?
    .with_limit_options(limit);
    let final_group_by = partial.group_expr().as_final();
    let final_aggr_expr = partial.aggr_expr().to_vec();
    let (mode, exchanged): (AggregateMode, Plan) = match exchange {
        Exchange::Coalesce => (
            AggregateMode::Final,
            Arc::new(CoalescePartitionsExec::new(Arc::new(partial))),
        ),
        Exchange::Hash { preserve_order } => {
            let keys = partial.output_group_expr();
            let repartition = RepartitionExec::try_new(
                Arc::new(partial),
                Partitioning::Hash(keys, HASH_PARTITIONS),
            )?;
            let repartition = if preserve_order {
                repartition.with_preserve_order()
            } else {
                repartition
            };
            (AggregateMode::FinalPartitioned, Arc::new(repartition))
        }
    };
    Ok(Arc::new(
        AggregateExec::try_new(
            mode,
            final_group_by,
            final_aggr_expr,
            filters,
            exchanged,
            input_schema,
        )?
        .with_limit_options(limit),
    ))
}

/// The aggregates under test
fn aggregate_plans() -> Result<Vec<PlanFactory>> {
    let hash = Exchange::Hash {
        preserve_order: false,
    };
    Ok(vec![
        // SELECT count(v), ... FROM t
        one_input(
            "AggregateExec Partial and Final without GROUP BY",
            aggregate_spec(),
            |input| {
                let aggr_expr = all_aggregates(&input)?;
                two_phase(no_group_by(), aggr_expr, input, Exchange::Coalesce, None)
            },
        ),
        // SELECT min(v), max(v) FROM t: the partial aggregate creates a
        // dynamic filter
        one_input(
            "AggregateExec Partial and Final without GROUP BY, min and max only",
            aggregate_spec(),
            |input| {
                let aggr_expr = min_max(&input)?;
                two_phase(no_group_by(), aggr_expr, input, Exchange::Coalesce, None)
            },
        ),
        // SELECT g, count(v), ... FROM t GROUP BY g, with one target
        // partition
        one_input(
            "AggregateExec Partial and Final",
            aggregate_spec(),
            |input| {
                let group_by = group_by(&["g"], &input)?;
                let aggr_expr = all_aggregates(&input)?;
                two_phase(group_by, aggr_expr, input, Exchange::Coalesce, None)
            },
        ),
        one_input(
            "AggregateExec Partial and FinalPartitioned",
            aggregate_spec(),
            move |input| {
                let group_by = group_by(&["g", "h"], &input)?;
                let aggr_expr = all_aggregates(&input)?;
                two_phase(group_by, aggr_expr, input, hash, None)
            },
        ),
        // On input sorted by the grouping key, both phases run in
        // `InputOrderMode::Sorted`
        one_input(
            "AggregateExec Partial and FinalPartitioned on sorted input",
            aggregate_spec_sorted_on_g()?,
            |input| {
                let group_by = group_by(&["g"], &input)?;
                let aggr_expr = all_aggregates(&input)?;
                let exchange = Exchange::Hash {
                    preserve_order: true,
                };
                two_phase(group_by, aggr_expr, input, exchange, None)
            },
        ),
        one_input("AggregateExec Single", aggregate_spec(), |input| {
            let group_by = group_by(&["g"], &input)?;
            let aggr_expr = all_aggregates(&input)?;
            single_phase(AggregateMode::Single, group_by, aggr_expr, input, None)
        }),
        one_input(
            "AggregateExec Single without GROUP BY",
            aggregate_spec(),
            |input| {
                let aggr_expr = all_aggregates(&input)?;
                single_phase(AggregateMode::Single, no_group_by(), aggr_expr, input, None)
            },
        ),
        one_input(
            "AggregateExec Single on sorted input",
            aggregate_spec_sorted_on_g()?,
            |input| {
                let group_by = group_by(&["g"], &input)?;
                let aggr_expr = all_aggregates(&input)?;
                single_phase(AggregateMode::Single, group_by, aggr_expr, input, None)
            },
        ),
        // Grouping on (h, g) over input sorted on g: `PartiallySorted`
        one_input(
            "AggregateExec Single on partially sorted input",
            aggregate_spec_sorted_on_g()?,
            |input| {
                let group_by = group_by(&["h", "g"], &input)?;
                let aggr_expr = all_aggregates(&input)?;
                single_phase(AggregateMode::Single, group_by, aggr_expr, input, None)
            },
        ),
        one_input(
            "AggregateExec SinglePartitioned",
            aggregate_spec(),
            |input| {
                let group_by = group_by(&["g"], &input)?;
                let aggr_expr = all_aggregates(&input)?;
                single_phase(
                    AggregateMode::SinglePartitioned,
                    group_by,
                    aggr_expr,
                    input,
                    None,
                )
            },
        ),
        // SELECT g, max(v) FROM t GROUP BY g ORDER BY max(v) DESC LIMIT 5,
        // after `TopKAggregation` pushed the limit into both phases
        one_input(
            "AggregateExec Partial and FinalPartitioned with TopK limit",
            many_values(600),
            move |input| {
                let group_by = group_by(&["g"], &input)?;
                let aggr_expr = vec![aggregate(max_udaf(), "v", "max(v)", &input)?];
                let limit = Some(LimitOptions::new_with_order(5, true));
                two_phase(group_by, aggr_expr, input, hash, limit)
            },
        ),
        // SELECT DISTINCT g FROM t LIMIT 5, after `LimitedDistinctAggregation`
        one_input(
            "AggregateExec Single with DISTINCT limit",
            aggregate_spec(),
            |input| {
                let group_by = group_by(&["g"], &input)?;
                let limit = Some(LimitOptions::new(5));
                single_phase(AggregateMode::Single, group_by, vec![], input, limit)
            },
        )
        .allow(
            "batch_boundary_invariance",
            "the limit is a soft limit: the aggregate stops after the input batch in \
             which it has seen 5 groups, so how many and which groups it emits depends \
             on batch boundaries, and only the LIMIT above it makes the result well \
             defined (aggregates/mod.rs:859-864); it reports no fetch, so the check \
             cannot know that",
        ),
        // SELECT g, h, count(v), ... FROM t GROUP BY ROLLUP (g, h), on few
        // rows with many distinct values, so that there are more groups
        // than input rows
        one_input(
            "AggregateExec Partial and FinalPartitioned with ROLLUP",
            many_values(90),
            move |input| {
                let group_by = rollup_g_h(&input)?;
                let aggr_expr = all_aggregates(&input)?;
                two_phase(group_by, aggr_expr, input, hash, None)
            },
        ),
    ])
}

/// Rows of the left input of a join
const LEFT_ROWS: usize = 60;
/// Rows of the right input of a join
const RIGHT_ROWS: usize = 90;

/// A nullable join key and a value. The left key has more distinct values
/// than the right key, so that both sides have rows without a match: left
/// rows with keys the right side does not have, and rows with null keys on
/// both sides.
fn left_spec() -> SourceSpec {
    let schema = Arc::new(Schema::new(vec![
        Field::new("l_k", DataType::Int32, true),
        Field::new("l_v", DataType::Int32, false),
    ]));
    SourceSpec::new(schema)
        .with_distinct_values(12)
        .with_num_rows(LEFT_ROWS)
}

fn right_spec() -> SourceSpec {
    let schema = Arc::new(Schema::new(vec![
        Field::new("r_k", DataType::Int32, true),
        Field::new("r_v", DataType::Int32, false),
    ]));
    SourceSpec::new(schema)
        .with_distinct_values(8)
        .with_num_rows(RIGHT_ROWS)
}

/// A factory for a join of a [`left_spec`] and a [`right_spec`] input
fn join<F>(name: impl Into<String>, create: F) -> PlanFactory
where
    F: Fn(Plan, Plan) -> Result<Plan> + Send + Sync + 'static,
{
    PlanFactory::new(name, vec![left_spec(), right_spec()], move |inputs| {
        let [left, right] = <[Plan; 2]>::try_from(inputs)
            .map_err(|_| internal_datafusion_err!("a join has two inputs"))?;
        create(left, right)
    })
}

/// `(l_k, r_k)`
fn join_on(left: &Plan, right: &Plan) -> Result<JoinOn> {
    Ok(vec![(
        col("l_k", &left.schema())?,
        col("r_k", &right.schema())?,
    )])
}

/// A join filter comparing `left_column` with `right_column` using `op`. Its
/// intermediate schema copies the input fields, with their nullability, as
/// the physical planner does.
fn join_filter(
    left: &Plan,
    left_column: &str,
    op: Operator,
    right: &Plan,
    right_column: &str,
) -> Result<JoinFilter> {
    let (left, right) = (left.schema(), right.schema());
    let left_index = left.index_of(left_column)?;
    let right_index = right.index_of(right_column)?;
    let schema = Arc::new(Schema::new(vec![
        left.field(left_index).clone(),
        right.field(right_index).clone(),
    ]));
    let expression = Arc::new(BinaryExpr::new(
        Arc::new(Column::new(left_column, 0)),
        op,
        Arc::new(Column::new(right_column, 1)),
    ));
    Ok(JoinFilter::new(
        expression,
        vec![
            ColumnIndex {
                index: left_index,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: right_index,
                side: JoinSide::Right,
            },
        ],
        schema,
    ))
}

/// `l_v < r_v`, which keeps about half of the pairs with matching keys
fn value_filter(left: &Plan, right: &Plan) -> Result<JoinFilter> {
    join_filter(left, "l_v", Operator::Lt, right, "r_v")
}

/// Options of a hash join factory
#[derive(Debug, Clone, Copy)]
struct HashJoin {
    mode: PartitionMode,
    join_type: JoinType,
    filter: bool,
    null_equality: NullEquality,
    fetch: Option<usize>,
}

impl HashJoin {
    fn new(mode: PartitionMode, join_type: JoinType) -> Self {
        Self {
            mode,
            join_type,
            filter: false,
            null_equality: NullEquality::NullEqualsNothing,
            fetch: None,
        }
    }

    fn with_filter(mut self) -> Self {
        self.filter = true;
        self
    }

    /// A hash join as the physical planner and `JoinSelection` build it. The
    /// harness puts the build side of `CollectLeft` in one partition, and
    /// hash partitions both inputs of `Partitioned` on the join keys.
    fn factory(self, name: impl Into<String>) -> PlanFactory {
        join(name, move |left, right| {
            let on = join_on(&left, &right)?;
            let filter = self
                .filter
                .then(|| value_filter(&left, &right))
                .transpose()?;
            let join = HashJoinExec::try_new(
                left,
                right,
                on,
                filter,
                &self.join_type,
                None,
                self.mode,
                self.null_equality,
                false,
            )?;
            match self.fetch {
                Some(fetch) => with_fetch(&join, fetch),
                None => Ok(Arc::new(join)),
            }
        })
    }
}

/// A sort merge join. The harness sorts both inputs on the join keys and
/// hash partitions them on the keys, as `EnforceDistribution` and
/// `EnforceSorting` arrange them.
fn sort_merge_join(name: String, join_type: JoinType, filter: bool) -> PlanFactory {
    join(name, move |left, right| {
        let on = join_on(&left, &right)?;
        let filter = filter.then(|| value_filter(&left, &right)).transpose()?;
        Ok(Arc::new(SortMergeJoinExec::try_new(
            left,
            right,
            on,
            filter,
            join_type,
            vec![SortOptions::default()],
            NullEquality::NullEqualsNothing,
        )?))
    })
}

/// A nested loop join with the filter `l_k < r_k`, which is false for null
/// keys. The harness puts the left input in one partition.
fn nested_loop_join(join_type: JoinType) -> PlanFactory {
    join(
        format!("NestedLoopJoinExec {join_type}"),
        move |left, right| {
            let filter = join_filter(&left, "l_k", Operator::Lt, &right, "r_k")?;
            Ok(Arc::new(NestedLoopJoinExec::try_new(
                left,
                right,
                Some(filter),
                &join_type,
                None,
            )?))
        },
    )
}

/// A piecewise merge join on `l_k < r_k`. The harness puts the buffered
/// (left) side in one partition and sorts it as the join requires; right
/// existence joins require neither.
fn piecewise_merge_join(join_type: JoinType) -> PlanFactory {
    join(
        format!("PiecewiseMergeJoinExec {join_type}"),
        move |left, right| {
            let on = (col("l_k", &left.schema())?, col("r_k", &right.schema())?);
            Ok(Arc::new(PiecewiseMergeJoinExec::try_new(
                left,
                right,
                on,
                Operator::Lt,
                join_type,
                HASH_PARTITIONS,
            )?))
        },
    )
}

/// A symmetric hash join, which `JoinSelection` uses when both inputs are
/// unbounded, on finite inputs
fn symmetric_hash_join(
    name: &str,
    mode: StreamJoinPartitionMode,
    join_type: JoinType,
    filter: bool,
) -> PlanFactory {
    join(name, move |left, right| {
        let on = join_on(&left, &right)?;
        let filter = filter.then(|| value_filter(&left, &right)).transpose()?;
        Ok(Arc::new(SymmetricHashJoinExec::try_new(
            left,
            right,
            on,
            filter,
            &join_type,
            NullEquality::NullEqualsNothing,
            None,
            None,
            mode,
        )?))
    })
}

/// `ASOF JOIN ... ON l_k = r_k MATCH_CONDITION (l_v >= r_v)`. The harness
/// sorts both inputs as the join requires, on the equality keys and then on
/// the match expression, and puts the right input in one partition.
fn asof_join() -> PlanFactory {
    join("AsOfJoinExec", |left, right| {
        let on = join_on(&left, &right)?;
        let match_condition = AsOfMatchExpr::new(
            col("l_v", &left.schema())?,
            Operator::GtEq,
            col("r_v", &right.schema())?,
        );
        Ok(Arc::new(AsOfJoinExec::try_new(
            left,
            right,
            on,
            match_condition,
            None,
        )?))
    })
}

/// The joins under test. Join types are chosen to cover the different code
/// paths of each operator rather than every combination: types that emit
/// unmatched rows of the build side at the end, types that emit probe rows
/// as they arrive, existence and mark joins, and filters.
fn join_plans() -> Vec<PlanFactory> {
    use JoinType::*;
    use PartitionMode::{CollectLeft, Partitioned};

    let mut factories = vec![];
    for join_type in [
        Inner, Left, Right, Full, LeftSemi, RightSemi, LeftAnti, RightAnti, LeftMark,
        RightMark,
    ] {
        factories.push(
            HashJoin::new(CollectLeft, join_type)
                .factory(format!("HashJoinExec CollectLeft {join_type}")),
        );
    }
    for join_type in [Inner, Full, RightSemi, LeftAnti] {
        factories.push(
            HashJoin::new(CollectLeft, join_type)
                .with_filter()
                .factory(format!("HashJoinExec CollectLeft {join_type} with filter")),
        );
    }
    let mut nulls_equal = HashJoin::new(CollectLeft, Inner);
    nulls_equal.null_equality = NullEquality::NullEqualsNull;
    factories
        .push(nulls_equal.factory("HashJoinExec CollectLeft Inner with nulls equal"));
    let mut with_fetch = HashJoin::new(CollectLeft, Inner);
    with_fetch.fetch = Some(FETCH);
    factories.push(with_fetch.factory("HashJoinExec CollectLeft Inner with fetch"));
    for join_type in [Inner, Left, Full, RightAnti, LeftMark] {
        factories.push(
            HashJoin::new(Partitioned, join_type)
                .factory(format!("HashJoinExec Partitioned {join_type}")),
        );
    }
    factories.push(
        HashJoin::new(Partitioned, Full)
            .with_filter()
            .factory("HashJoinExec Partitioned Full with filter"),
    );

    for join_type in [Inner, Left, Full, LeftSemi, RightAnti, LeftMark] {
        factories.push(sort_merge_join(
            format!("SortMergeJoinExec {join_type}"),
            join_type,
            false,
        ));
    }
    for join_type in [Inner, Full, LeftAnti] {
        factories.push(sort_merge_join(
            format!("SortMergeJoinExec {join_type} with filter"),
            join_type,
            true,
        ));
    }

    for join_type in [Inner, Left, Full, RightSemi, LeftAnti, RightMark] {
        factories.push(nested_loop_join(join_type));
    }

    factories.push(join("CrossJoinExec", |left, right| {
        Ok(Arc::new(CrossJoinExec::new(left, right)))
    }));

    factories.push(symmetric_hash_join(
        "SymmetricHashJoinExec SinglePartition Inner",
        StreamJoinPartitionMode::SinglePartition,
        Inner,
        false,
    ));
    factories.push(symmetric_hash_join(
        "SymmetricHashJoinExec Partitioned Full with filter",
        StreamJoinPartitionMode::Partitioned,
        Full,
        true,
    ));

    for join_type in [Inner, Full, RightSemi] {
        factories.push(piecewise_merge_join(join_type));
    }

    factories.push(asof_join());
    factories
}

#[tokio::test]
async fn builtin_plan_findings() -> Result<()> {
    let output = audit(builtin_plans()?, Profile::defaults()).await?;
    insta::assert_snapshot!(output);
    Ok(())
}

#[tokio::test]
async fn builtin_aggregate_findings() -> Result<()> {
    let output = audit(aggregate_plans()?, Profile::defaults()).await?;
    insta::assert_snapshot!(output);
    Ok(())
}

#[tokio::test]
async fn builtin_join_findings() -> Result<()> {
    let output = audit(join_plans(), Profile::defaults()).await?;
    insta::assert_snapshot!(output);
    Ok(())
}

#[tokio::test]
async fn builtin_window_findings() -> Result<()> {
    let output = audit(windows::window_plans()?, Profile::defaults()).await?;
    insta::assert_snapshot!(output);
    Ok(())
}

#[tokio::test]
async fn builtin_source_findings() -> Result<()> {
    let output = audit(sources::source_plans()?, Profile::defaults()).await?;
    insta::assert_snapshot!(output);
    Ok(())
}

#[cfg(feature = "extended_tests")]
#[tokio::test]
async fn builtin_plan_findings_extended() -> Result<()> {
    // A factory without inputs has a single case whatever the profiles, which
    // `builtin_plan_findings` records
    let factories = builtin_plans()?
        .into_iter()
        .filter(|factory| !factory.inputs.is_empty())
        .collect();
    let output = audit(factories, Profile::extended()).await?;
    insta::assert_snapshot!(output);
    Ok(())
}

#[cfg(feature = "extended_tests")]
#[tokio::test]
async fn builtin_aggregate_findings_extended() -> Result<()> {
    let output = audit(aggregate_plans()?, Profile::extended()).await?;
    insta::assert_snapshot!(output);
    Ok(())
}

#[cfg(feature = "extended_tests")]
#[tokio::test]
async fn builtin_join_findings_extended() -> Result<()> {
    let output = audit(join_plans(), Profile::extended()).await?;
    insta::assert_snapshot!(output);
    Ok(())
}

#[cfg(feature = "extended_tests")]
#[tokio::test]
async fn builtin_window_findings_extended() -> Result<()> {
    let output = audit(windows::window_plans()?, Profile::extended()).await?;
    insta::assert_snapshot!(output);
    Ok(())
}

#[cfg(feature = "extended_tests")]
#[tokio::test]
async fn builtin_source_findings_extended() -> Result<()> {
    let output = audit(sources::source_plans()?, Profile::extended()).await?;
    insta::assert_snapshot!(output);
    Ok(())
}
