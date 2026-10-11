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

//! Fuzz test for null-aware (`NOT IN`) joins.
//!
//! Scalar and multi-column `NOT IN` / `IN` subqueries, which plan as
//! null-aware hash joins, are compared with `NOT EXISTS` / `EXISTS`
//! formulations of the same three-valued logic, which do not. Random tables
//! with NULLs on both sides are swept across batch sizes and partition counts.
//! Some columns are NOT NULL, so that tuples mix elements that can be NULL with
//! elements that cannot, which the null-aware join reads as scope keys.

use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

#[tokio::test(flavor = "multi_thread")]
async fn null_aware_join_matches_reference() {
    for seed in 0..20 {
        let mut rng = StdRng::seed_from_u64(seed);
        let outer = random_table(&mut rng, &["id", "k", "a", "b", "c"], &["n", "m"]);
        let inner = random_table(&mut rng, &["k", "x", "y", "z"], &["v", "w"]);
        for target_partitions in [1, 4] {
            for batch_size in [1, 3, 8192] {
                let config = SessionConfig::new()
                    .with_target_partitions(target_partitions)
                    .with_batch_size(batch_size)
                    // Keep multi-partition runs from collapsing to a single
                    // partition on these small inputs.
                    .set_usize(
                        "datafusion.optimizer.hash_join_single_partition_threshold",
                        0,
                    )
                    .set_usize(
                        "datafusion.optimizer.hash_join_single_partition_threshold_rows",
                        0,
                    );
                let ctx = SessionContext::new_with_config(config);
                ctx.register_table("o", Arc::clone(&outer) as _).unwrap();
                ctx.register_table("i", Arc::clone(&inner) as _).unwrap();
                for (query, reference) in cases() {
                    let actual = run(&ctx, &query).await;
                    let expected = run(&ctx, &reference).await;
                    assert_eq!(
                        actual, expected,
                        "seed={seed} target_partitions={target_partitions} \
                         batch_size={batch_size}\nquery: {query}\nreference: {reference}"
                    );
                }
            }
        }
    }
}

/// Pairs of (query, reference) that must return the same rows.
fn cases() -> Vec<(String, String)> {
    let mut cases = vec![];
    // (correlation inside the subquery, the same correlation in the reference)
    for (correlation, reference_correlation) in [
        ("", ""),
        (" WHERE i.k = o.k", " AND i.k = o.k"),
        (" WHERE i.k < o.k", " AND i.k < o.k"),
        (
            " WHERE i.k = o.k AND i.z < o.c",
            " AND i.k = o.k AND i.z < o.c",
        ),
    ] {
        // (outer value, subquery output, element-wise equality)
        for (value, output, equal) in [
            ("o.a", "i.x", "o.a = i.x"),
            ("(o.a, o.b)", "i.x, i.y", "o.a = i.x AND o.b = i.y"),
            (
                "(o.a, o.b, o.c)",
                "i.x, i.y, i.z",
                "o.a = i.x AND o.b = i.y AND o.c = i.z",
            ),
            ("(o.a, 1)", "i.x, i.y", "o.a = i.x AND 1 = i.y"),
            // `o.n`, `o.m`, `i.v` and `i.w` are NOT NULL.
            ("(o.a, o.n)", "i.x, i.v", "o.a = i.x AND o.n = i.v"),
            ("(o.n, o.a)", "i.v, i.x", "o.n = i.v AND o.a = i.x"),
            (
                "(o.n, o.a, o.b)",
                "i.v, i.x, i.y",
                "o.n = i.v AND o.a = i.x AND o.b = i.y",
            ),
            (
                "(o.n, o.a, o.m)",
                "i.v, i.x, i.w",
                "o.n = i.v AND o.a = i.x AND o.m = i.w",
            ),
            ("(o.n, o.a)", "i.y, i.x", "o.n = i.y AND o.a = i.x"),
            ("(1, o.a)", "i.v, i.x", "1 = i.v AND o.a = i.x"),
            ("(o.n, o.m)", "i.v, i.w", "o.n = i.v AND o.m = i.w"),
        ] {
            let subquery = format!("SELECT {output} FROM i{correlation}");
            let matched = format!(
                "EXISTS (SELECT 1 FROM i WHERE ({equal}){reference_correlation})"
            );
            let unknown = format!(
                "EXISTS (SELECT 1 FROM i WHERE ({equal}) IS NULL{reference_correlation})"
            );
            let not_in = format!(
                "NOT EXISTS (SELECT 1 FROM i WHERE ({equal}) IS NOT FALSE{reference_correlation})"
            );
            cases.push((
                format!("SELECT o.id FROM o WHERE {value} NOT IN ({subquery})"),
                format!("SELECT o.id FROM o WHERE {not_in}"),
            ));
            cases.push((
                format!("SELECT o.id FROM o WHERE {value} IN ({subquery})"),
                format!("SELECT o.id FROM o WHERE {matched}"),
            ));
            cases.push((
                format!(
                    "SELECT o.id FROM o WHERE {value} NOT IN ({subquery}) OR o.id % 3 = 0"
                ),
                format!("SELECT o.id FROM o WHERE {not_in} OR o.id % 3 = 0"),
            ));
            cases.push((
                format!("SELECT o.id, {value} NOT IN ({subquery}) AS r FROM o"),
                format!(
                    "SELECT o.id, CASE WHEN {matched} THEN false \
                     WHEN {unknown} THEN NULL ELSE true END AS r FROM o"
                ),
            ));
        }
    }
    cases
}

/// A table of `Int32` columns with values in `0..3`, split into up to three
/// partitions: the `nullable` columns, then the `non_nullable` ones. A column
/// named `id` holds unique row ids instead.
fn random_table(
    rng: &mut StdRng,
    nullable: &[&str],
    non_nullable: &[&str],
) -> Arc<MemTable> {
    let columns = nullable
        .iter()
        .map(|name| (*name, true))
        .chain(non_nullable.iter().map(|name| (*name, false)))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, nullable)| Field::new(*name, DataType::Int32, *nullable))
            .collect::<Vec<_>>(),
    ));
    let null_fraction = [0.0, 0.1, 0.3][rng.random_range(0..3)];
    let mut next_id = 0;
    let partitions = (0..rng.random_range(1..4))
        .map(|_| {
            let rows = rng.random_range(0..6);
            let arrays = columns
                .iter()
                .map(|(name, nullable)| {
                    let values: Int32Array = if *name == "id" {
                        (next_id..next_id + rows).map(Some).collect()
                    } else {
                        (0..rows)
                            .map(|_| {
                                (!(*nullable && rng.random_bool(null_fraction)))
                                    .then(|| rng.random_range(0..3))
                            })
                            .collect()
                    };
                    Arc::new(values) as _
                })
                .collect();
            next_id += rows;
            vec![RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap()]
        })
        .collect();
    Arc::new(MemTable::try_new(schema, partitions).unwrap())
}

/// The rows of `sql`, sorted.
async fn run(ctx: &SessionContext, sql: &str) -> Vec<String> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let formatted = pretty_format_batches(&batches).unwrap().to_string();
    let mut rows: Vec<String> = formatted
        .lines()
        .filter(|line| line.starts_with('|'))
        .skip(1)
        .map(String::from)
        .collect();
    rows.sort();
    rows
}
