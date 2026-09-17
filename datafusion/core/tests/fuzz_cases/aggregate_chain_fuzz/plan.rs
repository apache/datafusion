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
//! Plan construction for a [`Shape`] over a source.

use super::*;

pub(super) fn sort_expr(schema: &Schema, column: &str) -> PhysicalSortExpr {
    PhysicalSortExpr::new(
        col(column, schema).unwrap(),
        SortOptions {
            descending: false,
            nulls_first: true,
        },
    )
}

/// The ordering the source declares for `order`.
pub(super) fn source_ordering(
    schema: &Schema,
    keys: Keys,
    order: Order,
) -> Option<LexOrdering> {
    let keys = keys.columns();
    let sort_columns: &[&str] = match order {
        Order::Unordered => return None,
        Order::SortedByFirstKey => &keys[..1],
        Order::SortedByAllKeys => keys,
    };
    LexOrdering::new(sort_columns.iter().map(|column| sort_expr(schema, column)))
}

pub(super) fn source(
    partitions: &[Vec<RecordBatch>],
    keys: Keys,
    order: Order,
) -> Arc<dyn ExecutionPlan> {
    let schema = schema();
    let mut memory_source =
        MemorySourceConfig::try_new(partitions, Arc::clone(&schema), None).unwrap();
    if let Some(ordering) = source_ordering(&schema, keys, order) {
        memory_source = memory_source
            .try_with_sort_information(vec![ordering])
            .unwrap();
    }
    DataSourceExec::from_data_source(memory_source)
}

pub(super) fn group_by(schema: &Schema, keys: Keys) -> PhysicalGroupBy {
    PhysicalGroupBy::new_single(
        keys.columns()
            .iter()
            .map(|key| (col(key, schema).unwrap(), key.to_string()))
            .collect(),
    )
}

pub(super) fn aggregates(
    schema: &SchemaRef,
    query: Query,
) -> Vec<Arc<AggregateFunctionExpr>> {
    let value_column = || vec![col("v", schema).unwrap()];
    let build = |builder: AggregateExprBuilder, alias: &str| {
        Arc::new(
            builder
                .schema(Arc::clone(schema))
                .alias(alias)
                .build()
                .unwrap(),
        )
    };
    if query.aggregates == Aggregates::None {
        return vec![];
    }
    if query.aggregates == Aggregates::Max {
        // TopK supports exactly one min/max aggregate over a non-nullable input
        return vec![build(
            AggregateExprBuilder::new(max_udaf(), value_column()),
            "max",
        )];
    }
    vec![
        build(
            AggregateExprBuilder::new(count_udaf(), value_column()),
            "count",
        ),
        build(
            AggregateExprBuilder::new(count_udaf(), value_column()).distinct(),
            "count_distinct",
        ),
        build(AggregateExprBuilder::new(sum_udaf(), value_column()), "sum"),
        // avg has no Int64 groups accumulator; the values are small integers so
        // the Float64 sum stays exact and the result is order-independent.
        build(
            AggregateExprBuilder::new(
                avg_udaf(),
                vec![cast(col("v", schema).unwrap(), schema, DataType::Float64).unwrap()],
            ),
            "avg",
        ),
        build(AggregateExprBuilder::new(min_udaf(), value_column()), "min"),
        build(AggregateExprBuilder::new(max_udaf(), value_column()), "max"),
    ]
}

/// Folds `shape.operators` bottom-up into a plan. The group-by, aggregate
/// expressions and hash keys are rewritten after every aggregate stage so the
/// next stage consumes that stage's output.
pub(super) fn build_plan(
    shape: &Shape,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let input_schema = schema();
    let mut plan = input;
    let mut group_by = group_by(&input_schema, shape.query.keys);
    let mut aggregates = aggregates(&input_schema, shape.query);
    let mut hash_keys: Vec<Arc<dyn PhysicalExpr>> = group_by.input_exprs();

    for operator in shape.chain.operators {
        plan = match operator {
            Aggregate(mode) | TopK(mode) => {
                let limit_options = matches!(operator, TopK(_))
                    .then(|| LimitOptions::new_with_order(TOP_K_LIMIT, true));
                let aggregate = Arc::new(
                    AggregateExec::try_new(
                        *mode,
                        group_by.clone(),
                        aggregates.clone(),
                        vec![None; aggregates.len()],
                        plan,
                        Arc::clone(&input_schema),
                    )
                    .unwrap()
                    .with_limit_options(limit_options),
                );
                group_by = aggregate.group_expr().as_final();
                aggregates = aggregate.aggr_expr().to_vec();
                hash_keys = aggregate.output_group_expr();
                aggregate
            }
            HashRepartition => Arc::new(
                RepartitionExec::try_new(
                    plan,
                    Partitioning::Hash(hash_keys.clone(), PARTITIONS),
                )
                .unwrap(),
            ),
            OrderPreservingHashRepartition => Arc::new(
                RepartitionExec::try_new(
                    plan,
                    Partitioning::Hash(hash_keys.clone(), PARTITIONS),
                )
                .unwrap()
                .with_preserve_order(),
            ),
            CoalescePartitions => Arc::new(CoalescePartitionsExec::new(plan)),
            SortPreservingMerge => {
                let ordering = plan.properties().output_ordering().cloned().unwrap();
                Arc::new(SortPreservingMergeExec::new(ordering, plan))
            }
        };
    }
    plan
}
