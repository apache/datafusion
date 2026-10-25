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

//! Supply catalog statistics to the optimizer via the `StatisticsRegistry`.
//!
//! The registry is the extension point for statistics the core cannot derive,
//! such as column statistics a catalog knows.
//!
//! `(SELECT user_id FROM events WHERE amount < 50 GROUP BY user_id) JOIN dims`:
//! the in-memory `events` table has no column statistics, so the grouped side is
//! estimated at 1000 rows and `dims` (60 rows) becomes the build side. A provider
//! supplies the `amount` range and the `user_id` distinct count a catalog knows;
//! with them the operators estimate the grouped side at 50 rows, below `dims`,
//! which flips the join build side. The ground-truth query prints the true
//! distinct count (below 60), confirming the flip.

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

/// Matches the base `events` scan: a leaf carrying both `user_id` and `amount`.
fn catalog_matches(plan: &dyn ExecutionPlan) -> bool {
    let schema = plan.schema();
    plan.children().is_empty()
        && schema.index_of("user_id").is_ok()
        && schema.index_of("amount").is_ok()
}

/// Injects the catalog-known `amount` range (for filter selectivity) and
/// `user_id` distinct count (for post-filter NDV) the in-memory table lacks.
fn catalog_stats(
    plan: &dyn ExecutionPlan,
    _child_stats: &[ExtendedStatistics],
) -> Result<StatisticsResult> {
    let schema = plan.schema();
    let user_id = schema.index_of("user_id")?;
    let amount = schema.index_of("amount")?;
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

fn build_ctx(with_registry: bool) -> Result<SessionContext> {
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
        let registry = StatisticsRegistry::with_providers(vec![Arc::new(
            ClosureStatisticsProvider::with_matches(catalog_matches, catalog_stats),
        )]);
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
            ("user_id", int_col(&(0..60).collect::<Vec<_>>())),
            ("label", int_col(&(0..60).collect::<Vec<_>>())),
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
    let truth = build_ctx(false)?.sql(truth_query).await?.collect().await?;
    println!("{}\n", pretty_format_batches(&truth)?);

    println!("-- Query --\n{QUERY}\n");
    println!(
        "A hash join builds its in-memory hash table from one input and probes with\n\
         the other, so the smaller input should be the build side. Default estimation\n\
         sizes the grouped `events` at 1000 rows and builds from `dims`; the\n\
         catalog statistics bring the estimate to 50, below `dims` (60 rows), so the\n\
         build side flips to `events`. The ground-truth count above (also below 60)\n\
         confirms `events` really is the smaller, cheaper side.\n"
    );
    println!("-- Without the registry (default estimation) --");
    println!("{}\n", explain(&build_ctx(false)?).await?);
    println!("-- With the registry (catalog statistics) --");
    println!("{}", explain(&build_ctx(true)?).await?);
    Ok(())
}
