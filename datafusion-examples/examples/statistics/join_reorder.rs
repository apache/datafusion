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

//! Plug refined cardinality estimation into the optimizer via the `StatisticsRegistry`.
//!
//! DataFusion's built-in statistics are intentionally simple defaults; the registry
//! is the seam for plugging in more refined estimation (formulas or heuristics
//! suited to your system or data) without changing the core.
//!
//! `(SELECT user_id FROM events WHERE amount < 50 GROUP BY user_id) JOIN dims`: a
//! provider supplies the column stats a catalog knows (`amount` range, `user_id`
//! distinct count); the built-in `FilterStatisticsProvider` then refines the
//! post-filter distinct count with the survival formula
//! `NDV * (1 - (1 - selectivity)^(rows / NDV))` (Yao/Cardenas) to ~32, below `dims`
//! (48), flipping the join build side. Core's simpler `min(NDV, rows)` cap would
//! give 50 (> 48) and keep the other order; the refinement is the point. The
//! ground-truth query prints the true surviving distinct count (below 48),
//! confirming the flip.

use std::sync::Arc;

use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::catalog::MemTable;
use datafusion::common::Result;
use datafusion::common::ScalarValue;
use datafusion::common::stats::Precision;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::operator_statistics::{
    ClosureStatisticsProvider, ExtendedStatistics, StatisticsRegistry, StatisticsResult,
};
use datafusion::physical_plan::statistics::StatisticsArgs;
use datafusion::prelude::*;
use rand::{Rng, SeedableRng, rngs::StdRng};

/// Supplies the catalog-known statistics of the `events` scan that the in-memory
/// table does not carry: the `amount` range (for filter selectivity) and the
/// `user_id` distinct count (for post-filter NDV estimation). Only the base
/// `events` scan (a leaf carrying both columns) is matched; everything else
/// delegates to the rest of the chain.
fn catalog_stats(
    plan: &dyn ExecutionPlan,
    _child_stats: &[ExtendedStatistics],
) -> Result<StatisticsResult> {
    let schema = plan.schema();
    let (Ok(user_id), Ok(amount)) =
        (schema.index_of("user_id"), schema.index_of("amount"))
    else {
        return Ok(StatisticsResult::Delegate);
    };
    if !plan.children().is_empty() {
        return Ok(StatisticsResult::Delegate);
    }
    let mut stats = (*plan.statistics_from_inputs(&[], &StatisticsArgs::new())?).clone();
    stats.column_statistics[amount].min_value =
        Precision::Inexact(ScalarValue::Int32(Some(0)));
    stats.column_statistics[amount].max_value =
        Precision::Inexact(ScalarValue::Int32(Some(999)));
    stats.column_statistics[user_id].distinct_count = Precision::Inexact(100);
    Ok(StatisticsResult::Computed(ExtendedStatistics::new(stats)))
}

fn int_col(values: &[i32]) -> Arc<Int32Array> {
    Arc::new(Int32Array::from_iter_values(values.iter().copied()))
}

fn mem_table(fields: &[(&str, Arc<Int32Array>)]) -> Result<Arc<MemTable>> {
    let schema = Arc::new(Schema::new(
        fields
            .iter()
            .map(|(name, _)| Field::new(*name, DataType::Int32, false))
            .collect::<Vec<_>>(),
    ));
    let cols = fields.iter().map(|(_, col)| Arc::clone(col) as _).collect();
    let batch = RecordBatch::try_new(Arc::clone(&schema), cols)?;
    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}

async fn build_ctx(with_registry: bool) -> Result<SessionContext> {
    let config = SessionConfig::new()
        .with_target_partitions(4)
        .set_bool("datafusion.explain.physical_plan_only", true)
        .set_bool("datafusion.explain.show_statistics", true)
        // Force Partitioned hash joins so statistics alone drive the build side.
        .set_usize(
            "datafusion.optimizer.hash_join_single_partition_threshold",
            1,
        )
        .set_usize(
            "datafusion.optimizer.hash_join_single_partition_threshold_rows",
            1,
        );

    let mut builder = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features();
    if with_registry {
        let mut registry = StatisticsRegistry::default_with_builtin_providers();
        registry.register(Arc::new(ClosureStatisticsProvider::new(catalog_stats)));
        builder = builder.with_statistics_registry(registry);
    }
    let ctx = SessionContext::new_with_state(builder.build());

    let n = 1000i32;
    let user_ids: Vec<i32> = (0..n).map(|v| v % 100).collect();
    // `amount` independent of `user_id` so the filter keeps a representative sample.
    let mut rng = StdRng::seed_from_u64(2024);
    let amounts: Vec<i32> = (0..n).map(|_| rng.random_range(0..1000)).collect();
    ctx.register_table(
        "events",
        mem_table(&[
            ("user_id", int_col(&user_ids)),
            ("amount", int_col(&amounts)),
        ])?,
    )?;
    ctx.register_table(
        "dims",
        mem_table(&[
            ("user_id", int_col(&(0..48).collect::<Vec<_>>())),
            ("label", int_col(&(0..48).collect::<Vec<_>>())),
        ])?,
    )?;
    Ok(ctx)
}

const QUERY: &str = "SELECT e.user_id, d.label \
     FROM (SELECT user_id FROM events WHERE amount < 50 GROUP BY user_id) e \
     JOIN dims d ON e.user_id = d.user_id";

async fn explain(ctx: &SessionContext) -> Result<String> {
    let batches = ctx
        .sql(&format!("EXPLAIN {QUERY}"))
        .await?
        .collect()
        .await?;
    Ok(pretty_format_batches(&batches)?.to_string())
}

pub async fn join_reorder() -> Result<()> {
    let truth_query = "SELECT count(DISTINCT user_id) AS true_distinct_users \
         FROM events WHERE amount < 50";
    println!("-- Ground truth --\n{truth_query}\n");
    let truth = build_ctx(false)
        .await?
        .sql(truth_query)
        .await?
        .collect()
        .await?;
    println!("{}\n", pretty_format_batches(&truth)?);

    println!("-- Query --\n{QUERY}\n");
    println!(
        "A hash join builds its in-memory hash table from one input and probes with\n\
         the other, so the smaller input should be the build side. Default estimation\n\
         sizes the grouped `events` at 1000 rows and builds from `dims`; the\n\
         registry's refined ~32 estimate is below `dims` (48 rows), so it flips the\n\
         build side to `events`. The ground-truth count above (also below 48)\n\
         confirms `events` really is the smaller, cheaper side.\n"
    );
    println!("-- Without the registry (default estimation) --");
    println!("{}\n", explain(&build_ctx(false).await?).await?);
    println!("-- With the registry (built-in refinement) --");
    println!("{}", explain(&build_ctx(true).await?).await?);
    Ok(())
}
