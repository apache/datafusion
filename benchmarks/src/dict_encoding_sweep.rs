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

//! Standalone harness for comparing `Utf8View` vs `Dictionary<Int32, Utf8>`
//! across operators and cardinalities.
//!
//! Data generation is seeded and deterministic. For dictionary batches, every
//! chunk of `dict_chunk_rows` rows shares one `values` array; subsequent
//! chunks use a fresh values array containing the same `NDV` distinct values.
//!
//! Timing interleaves view/dict iterations to make cache and thermal state
//! symmetric. Statistics reported: median / min / p25 / p75 (nanoseconds).

use std::hint::black_box;
use std::io::Write;
use std::sync::Arc;

use datafusion_common::instant::Instant;

use arrow::array::{
    ArrayRef, DictionaryArray, Int32Array, Int32Builder, Int64Array, PrimitiveArray,
    RecordBatch, StringArray, StringViewBuilder,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::Result;
use futures::StreamExt;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Utf8View,
    Dictionary,
}

impl Encoding {
    pub fn as_str(self) -> &'static str {
        match self {
            Encoding::Utf8View => "utf8view",
            Encoding::Dictionary => "dictionary",
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableSpec {
    pub rows: usize,
    pub ndv: usize,
    pub width_bytes: usize,
    pub null_rate: f64,
    pub batch_size: usize,
    pub dict_chunk_rows: usize,
    pub seed: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct Query {
    pub id: &'static str,
    pub sql: SqlSpec,
}

/// SQL text for a query, either a static string or a function that builds one
/// from the realized distinct values (so filters can target rows that exist).
#[derive(Debug, Clone, Copy)]
pub enum SqlSpec {
    Static(&'static str),
    Dynamic(fn(&[String]) -> String),
}

impl SqlSpec {
    pub fn resolve(self, distinct_values: &[String]) -> String {
        match self {
            SqlSpec::Static(s) => s.to_string(),
            SqlSpec::Dynamic(f) => f(distinct_values),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ChunkInfo {
    pub num_chunks: usize,
    pub values_per_chunk: usize,
    pub mean_distinct_per_batch: usize,
}

/// Reproduce the distinct-values list that `build_batches` uses internally.
/// Call this before `build_batches` and pass the same slice if you need to
/// reference real values from SQL (e.g. for `WHERE k = '...'`).
pub fn distinct_values_for(spec: &TableSpec) -> Vec<String> {
    (0..spec.ndv)
        .map(|i| gen_value(spec.seed, i, spec.width_bytes))
        .collect()
}

/// Three-column table `(k, v, w)` with the requested encoding on `k`.
/// `v` is `Int64`, `w` is a low-cardinality `Int32` (0..16).
pub fn build_batches(
    spec: &TableSpec,
    encoding: Encoding,
    distinct_values: &[String],
) -> Vec<RecordBatch> {
    assert!(spec.ndv >= 1, "ndv must be >= 1");
    assert!(
        spec.ndv <= spec.dict_chunk_rows,
        "ndv ({}) must fit within dict_chunk_rows ({})",
        spec.ndv,
        spec.dict_chunk_rows
    );
    assert!(spec.width_bytes >= 1, "width_bytes must be >= 1");
    assert_eq!(
        distinct_values.len(),
        spec.ndv,
        "distinct_values length ({}) must equal spec.ndv ({})",
        distinct_values.len(),
        spec.ndv
    );

    let schema = Arc::new(schema_for(encoding));
    let mut rng_keys = StdRng::seed_from_u64(spec.seed ^ 0x4B45_5953_4B45_5953);
    let mut rng_nulls = StdRng::seed_from_u64(spec.seed ^ 0xA5A5_A5A5_A5A5_A5A5);
    let mut rng_v = StdRng::seed_from_u64(spec.seed ^ 0x1234_5678_9ABC_DEF0);
    let mut rng_w = StdRng::seed_from_u64(spec.seed ^ 0xDEAD_BEEF_CAFE_BABE);

    let mut batches = Vec::new();
    let mut rows_remaining = spec.rows;

    while rows_remaining > 0 {
        let chunk_rows = rows_remaining.min(spec.dict_chunk_rows);
        // One values array shared across all batches in this chunk.
        let values_array: Arc<StringArray> = Arc::new(StringArray::from(
            distinct_values
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
        ));

        let mut chunk_rows_left = chunk_rows;
        while chunk_rows_left > 0 {
            let batch_rows = chunk_rows_left.min(spec.batch_size);

            let key_indices: Vec<i32> = (0..batch_rows)
                .map(|_| rng_keys.random_range(0..spec.ndv) as i32)
                .collect();

            let null_mask: Option<NullBuffer> = if spec.null_rate > 0.0 {
                let mask: Vec<bool> = (0..batch_rows)
                    .map(|_| !rng_nulls.random_bool(spec.null_rate))
                    .collect();
                Some(NullBuffer::from(mask))
            } else {
                None
            };

            let k_array: ArrayRef = match encoding {
                Encoding::Utf8View => {
                    build_utf8view(distinct_values, &key_indices, null_mask.as_ref())
                }
                Encoding::Dictionary => build_dictionary(
                    &key_indices,
                    null_mask.as_ref(),
                    Arc::clone(&values_array),
                ),
            };

            let v_array: ArrayRef = Arc::new(Int64Array::from_iter_values(
                (0..batch_rows).map(|_| rng_v.random::<i64>()),
            ));
            let w_array: ArrayRef = Arc::new(Int32Array::from_iter_values(
                (0..batch_rows).map(|_| rng_w.random_range(0..16i32)),
            ));

            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![k_array, v_array, w_array],
            )
            .expect("record batch");

            batches.push(batch);
            chunk_rows_left -= batch_rows;
        }

        rows_remaining -= chunk_rows;
    }

    batches
}

fn build_utf8view(
    distinct_values: &[String],
    key_indices: &[i32],
    null_mask: Option<&NullBuffer>,
) -> ArrayRef {
    let mut builder = StringViewBuilder::with_capacity(key_indices.len());
    for (i, &idx) in key_indices.iter().enumerate() {
        let is_valid = null_mask.map(|m| m.is_valid(i)).unwrap_or(true);
        if is_valid {
            builder.append_value(&distinct_values[idx as usize]);
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

fn build_dictionary(
    key_indices: &[i32],
    null_mask: Option<&NullBuffer>,
    values: Arc<StringArray>,
) -> ArrayRef {
    let mut builder = Int32Builder::with_capacity(key_indices.len());
    match null_mask {
        Some(mask) => {
            for (i, &idx) in key_indices.iter().enumerate() {
                if mask.is_valid(i) {
                    builder.append_value(idx);
                } else {
                    builder.append_null();
                }
            }
        }
        None => {
            for &idx in key_indices {
                builder.append_value(idx);
            }
        }
    }
    let keys: PrimitiveArray<Int32Type> = builder.finish();
    Arc::new(
        DictionaryArray::<Int32Type>::try_new(keys, values as ArrayRef)
            .expect("dictionary array"),
    )
}

fn schema_for(encoding: Encoding) -> Schema {
    let k_type = match encoding {
        Encoding::Utf8View => DataType::Utf8View,
        Encoding::Dictionary => {
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        }
    };
    Schema::new(vec![
        Field::new("k", k_type, true),
        Field::new("v", DataType::Int64, false),
        Field::new("w", DataType::Int32, false),
    ])
}

fn dim_schema_for(encoding: Encoding) -> Schema {
    let k_type = match encoding {
        Encoding::Utf8View => DataType::Utf8View,
        Encoding::Dictionary => {
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        }
    };
    Schema::new(vec![
        Field::new("k", k_type, false),
        Field::new("extra", DataType::Int64, false),
    ])
}

/// Build a dim table `(k, extra)` with one row per distinct value, matching
/// the requested encoding on `k`. Used as the small side of joins.
///
/// `keys_subset` selects which of the full `distinct_values` to include (useful
/// for half-populated anti-join dim tables). Pass `distinct_values.len()` for
/// the full set.
pub fn build_dim_batches(
    encoding: Encoding,
    distinct_values: &[String],
    keys_subset: usize,
    batch_size: usize,
) -> Vec<RecordBatch> {
    assert!(keys_subset <= distinct_values.len());
    let schema = Arc::new(dim_schema_for(encoding));
    let values_array: Arc<StringArray> = Arc::new(StringArray::from(
        distinct_values
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
    ));

    let mut batches = Vec::new();
    let mut i = 0usize;
    while i < keys_subset {
        let end = (i + batch_size).min(keys_subset);
        let batch_rows = end - i;
        let key_indices: Vec<i32> = (i..end).map(|x| x as i32).collect();

        let k_array: ArrayRef = match encoding {
            Encoding::Utf8View => {
                let mut builder = StringViewBuilder::with_capacity(batch_rows);
                for &idx in &key_indices {
                    builder.append_value(&distinct_values[idx as usize]);
                }
                Arc::new(builder.finish())
            }
            Encoding::Dictionary => {
                let mut kb = Int32Builder::with_capacity(batch_rows);
                for &idx in &key_indices {
                    kb.append_value(idx);
                }
                let keys = kb.finish();
                Arc::new(
                    DictionaryArray::<Int32Type>::try_new(
                        keys,
                        Arc::clone(&values_array) as ArrayRef,
                    )
                    .expect("dim dictionary"),
                )
            }
        };

        let extra_array: ArrayRef =
            Arc::new(Int64Array::from_iter_values((i as i64)..(end as i64)));

        batches.push(
            RecordBatch::try_new(Arc::clone(&schema), vec![k_array, extra_array])
                .expect("dim record batch"),
        );
        i = end;
    }
    batches
}

/// Deterministic fixed-width value, exactly `width` printable ASCII bytes.
fn gen_value(seed: u64, idx: usize, width: usize) -> String {
    let mut rng = StdRng::seed_from_u64(
        seed.wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(idx as u64),
    );
    (0..width)
        .map(|_| (b'a' + rng.random_range(0..26u8)) as char)
        .collect()
}

pub fn chunk_info(spec: &TableSpec) -> ChunkInfo {
    let full = spec.rows / spec.dict_chunk_rows;
    let rem = spec.rows % spec.dict_chunk_rows;
    let num_chunks = full + usize::from(rem > 0);
    // All NDV distinct values appear in each chunk (we built the values array
    // that way). "Mean distinct per batch" is bounded by both NDV and batch_size.
    let mean_distinct_per_batch = spec.ndv.min(spec.batch_size);
    ChunkInfo {
        num_chunks,
        values_per_chunk: spec.ndv,
        mean_distinct_per_batch,
    }
}

pub fn make_session(batch_size: usize) -> SessionContext {
    make_session_parallel(batch_size, 1)
}

pub fn make_session_parallel(
    batch_size: usize,
    target_partitions: usize,
) -> SessionContext {
    let cfg = SessionConfig::new()
        .with_target_partitions(target_partitions)
        .with_batch_size(batch_size);
    SessionContext::new_with_config(cfg)
}

pub fn register_table(
    ctx: &SessionContext,
    name: &str,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    let schema: SchemaRef = batches[0].schema();
    let table = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table(name, Arc::new(table))?;
    Ok(())
}

/// Register a table whose batches are already pre-split into partitions.
/// Each inner `Vec<RecordBatch>` becomes one input partition in the MemTable.
pub fn register_table_partitioned(
    ctx: &SessionContext,
    name: &str,
    partitions: Vec<Vec<RecordBatch>>,
) -> Result<()> {
    assert!(!partitions.is_empty() && !partitions[0].is_empty());
    let schema: SchemaRef = partitions[0][0].schema();
    let table = MemTable::try_new(schema, partitions)?;
    ctx.register_table(name, Arc::new(table))?;
    Ok(())
}

/// Round-robin split a batch stream into `n` partitions.
pub fn split_batches_round_robin(
    batches: Vec<RecordBatch>,
    n: usize,
) -> Vec<Vec<RecordBatch>> {
    assert!(n >= 1);
    let mut out: Vec<Vec<RecordBatch>> = (0..n).map(|_| Vec::new()).collect();
    for (i, b) in batches.into_iter().enumerate() {
        out[i % n].push(b);
    }
    out
}

/// Run `warmups` + `iters` iterations, interleaving view and dict calls.
pub async fn run_cell(
    sql: &str,
    ctx_view: &SessionContext,
    ctx_dict: &SessionContext,
    warmups: usize,
    iters: usize,
) -> Result<(Vec<u128>, Vec<u128>)> {
    let plan_view = ctx_view.sql(sql).await?.create_physical_plan().await?;
    let plan_dict = ctx_dict.sql(sql).await?.create_physical_plan().await?;

    for _ in 0..warmups {
        drain(Arc::clone(&plan_view), ctx_view).await?;
        drain(Arc::clone(&plan_dict), ctx_dict).await?;
    }

    let mut view_times = Vec::with_capacity(iters);
    let mut dict_times = Vec::with_capacity(iters);
    for _ in 0..iters {
        view_times.push(time_drain(Arc::clone(&plan_view), ctx_view).await?);
        dict_times.push(time_drain(Arc::clone(&plan_dict), ctx_dict).await?);
    }

    Ok((view_times, dict_times))
}

/// Variant of [`run_cell`] that re-plans the physical plan for every drain.
/// Required for operators (e.g. `RepartitionExec`) whose internal state is
/// consumed on the first execution and would panic on reuse of the same plan.
pub async fn run_cell_replan(
    sql: &str,
    ctx_view: &SessionContext,
    ctx_dict: &SessionContext,
    warmups: usize,
    iters: usize,
) -> Result<(Vec<u128>, Vec<u128>)> {
    for _ in 0..warmups {
        let pv = ctx_view.sql(sql).await?.create_physical_plan().await?;
        drain(pv, ctx_view).await?;
        let pd = ctx_dict.sql(sql).await?.create_physical_plan().await?;
        drain(pd, ctx_dict).await?;
    }

    let mut view_times = Vec::with_capacity(iters);
    let mut dict_times = Vec::with_capacity(iters);
    for _ in 0..iters {
        let pv = ctx_view.sql(sql).await?.create_physical_plan().await?;
        let start = Instant::now();
        drain(pv, ctx_view).await?;
        view_times.push(start.elapsed().as_nanos());

        let pd = ctx_dict.sql(sql).await?.create_physical_plan().await?;
        let start = Instant::now();
        drain(pd, ctx_dict).await?;
        dict_times.push(start.elapsed().as_nanos());
    }

    Ok((view_times, dict_times))
}

async fn time_drain(plan: Arc<dyn ExecutionPlan>, ctx: &SessionContext) -> Result<u128> {
    let start = Instant::now();
    drain(plan, ctx).await?;
    Ok(start.elapsed().as_nanos())
}

async fn drain(plan: Arc<dyn ExecutionPlan>, ctx: &SessionContext) -> Result<()> {
    let mut stream = execute_stream(plan, ctx.task_ctx())?;
    let mut sink: u64 = 0;
    while let Some(batch) = stream.next().await {
        let b = batch?;
        sink = sink.wrapping_add(b.num_rows() as u64);
        for col in b.columns() {
            sink = sink.wrapping_add(col.len() as u64);
        }
    }
    black_box(sink);
    Ok(())
}

/// (median, min, p25, p75) over sorted `times`.
pub fn stats(mut times: Vec<u128>) -> (u128, u128, u128, u128) {
    assert!(!times.is_empty());
    times.sort_unstable();
    let n = times.len();
    let median = times[n / 2];
    let min = times[0];
    let p25 = times[n / 4];
    let p75 = times[(3 * n) / 4];
    (median, min, p25, p75)
}

pub fn csv_header() -> &'static str {
    "git_sha,group,query_id,encoding,rows,ndv,width_bytes,null_rate,batch_size,\
     dict_chunk_rows,process_id,median_ns,min_ns,p25_ns,p75_ns,iters,note"
}

#[expect(clippy::too_many_arguments)]
pub fn write_csv_row<W: Write>(
    out: &mut W,
    git_sha: &str,
    group: &str,
    query_id: &str,
    encoding: Encoding,
    spec: &TableSpec,
    process_id: u32,
    median_ns: u128,
    min_ns: u128,
    p25_ns: u128,
    p75_ns: u128,
    iters: usize,
    note: &str,
) -> std::io::Result<()> {
    writeln!(
        out,
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        git_sha,
        group,
        query_id,
        encoding.as_str(),
        spec.rows,
        spec.ndv,
        spec.width_bytes,
        spec.null_rate,
        spec.batch_size,
        spec.dict_chunk_rows,
        process_id,
        median_ns,
        min_ns,
        p25_ns,
        p75_ns,
        iters,
        note,
    )
}

/// Query catalogs per operator group.
pub mod groups {
    use super::{Query, SqlSpec};

    pub const SCAN: &[Query] = &[
        // Project only the string column — stresses the Utf8View/Dict copy path.
        Query {
            id: "scan_k",
            sql: SqlSpec::Static("SELECT k FROM t"),
        },
        // All three columns — forces projection through the entire record.
        Query {
            id: "scan_all",
            sql: SqlSpec::Static("SELECT k, v, w FROM t"),
        },
        // Count star — minimal materialization, mostly a row-count sanity check.
        Query {
            id: "scan_count_star",
            sql: SqlSpec::Static("SELECT count(*) FROM t"),
        },
        // Count the string column — same shape as count(*) but has to inspect
        // the null bitmap of `k` (identical for both encodings here since
        // nulls=0, but useful once null_rate > 0 is swept).
        Query {
            id: "scan_count_k",
            sql: SqlSpec::Static("SELECT count(k) FROM t"),
        },
    ];

    pub const SORT: &[Query] = &[
        // Full sort on the string column alone.
        Query {
            id: "sort_k",
            sql: SqlSpec::Static("SELECT k FROM t ORDER BY k"),
        },
        // Descending — same comparator path, catches any asc-only fast paths.
        Query {
            id: "sort_k_desc",
            sql: SqlSpec::Static("SELECT k FROM t ORDER BY k DESC"),
        },
        // TopK — ORDER BY + LIMIT triggers DataFusion's heap-based TopK path.
        Query {
            id: "sort_k_limit_1000",
            sql: SqlSpec::Static("SELECT k FROM t ORDER BY k LIMIT 1000"),
        },
        // Compound sort on (k, w) — tests the comparator when the primary key
        // is dict/view-encoded and secondary is a dense int.
        Query {
            id: "sort_kw",
            sql: SqlSpec::Static("SELECT k, w FROM t ORDER BY k, w"),
        },
        // Sort with payload columns — forces the sorter to move the Int64
        // alongside k, so the "did dict help the comparator" signal is diluted
        // by payload movement cost.
        Query {
            id: "sort_v_by_k",
            sql: SqlSpec::Static("SELECT v, k FROM t ORDER BY k"),
        },
    ];

    pub const PROJECT: &[Query] = &[
        // length(k) — simplest string function; dict can compute lengths once
        // per distinct value in principle.
        Query {
            id: "proj_length",
            sql: SqlSpec::Static("SELECT length(k) FROM t"),
        },
        // substr(k, 1, 4) — mirrors TPC-H Q22's SUBSTRING(c_phone FROM 1 FOR 2).
        Query {
            id: "proj_substr",
            sql: SqlSpec::Static("SELECT substr(k, 1, 4) FROM t"),
        },
        // upper(k) — character-wise transform returning a new string.
        Query {
            id: "proj_upper",
            sql: SqlSpec::Static("SELECT upper(k) FROM t"),
        },
        // concat(k, 'x') — allocates a new string per row, which should expose
        // whether dict unpacks before or after.
        Query {
            id: "proj_concat",
            sql: SqlSpec::Static("SELECT concat(k, 'x') FROM t"),
        },
        // CASE WHEN k LIKE 'z%' — TPC-H Q14-style conditional on a string
        // predicate. Returns a dense Int32, so the result cost is small.
        Query {
            id: "proj_case_like",
            sql: SqlSpec::Static("SELECT CASE WHEN k LIKE 'z%' THEN 1 ELSE 0 END FROM t"),
        },
    ];

    pub const REPARTITION: &[Query] = &[
        // Hash repartition on k before the group-by. With target_partitions > 1
        // and an already-partitioned input, the planner inserts a
        // RepartitionExec(Hash([k])) between the scan and the aggregate.
        Query {
            id: "repart_group_k",
            sql: SqlSpec::Static("SELECT k, count(*) FROM t GROUP BY k"),
        },
        // Same shape but joining the dim — hash repartition on both sides.
        Query {
            id: "repart_hash_join",
            sql: SqlSpec::Static("SELECT count(*) FROM t JOIN d ON t.k = d.k"),
        },
        // Round-robin repartition is picked when the plan needs more
        // parallelism without a key constraint (e.g. a pure projection).
        Query {
            id: "repart_project_k",
            sql: SqlSpec::Static("SELECT k FROM t"),
        },
    ];

    fn filter_eq(dv: &[String]) -> String {
        format!("SELECT count(*) FROM t WHERE k = '{}'", dv[0])
    }

    fn filter_ne(dv: &[String]) -> String {
        format!("SELECT count(*) FROM t WHERE k <> '{}'", dv[0])
    }

    fn filter_in10(dv: &[String]) -> String {
        let n = dv.len().min(10);
        let lits = dv[..n]
            .iter()
            .map(|s| format!("'{s}'"))
            .collect::<Vec<_>>()
            .join(",");
        format!("SELECT count(*) FROM t WHERE k IN ({lits})")
    }

    fn filter_like_prefix(dv: &[String]) -> String {
        let prefix = &dv[0][..1];
        format!("SELECT count(*) FROM t WHERE k LIKE '{prefix}%'")
    }

    fn filter_gt(dv: &[String]) -> String {
        format!("SELECT count(*) FROM t WHERE k > '{}'", dv[0])
    }

    fn filter_1pct_group(dv: &[String]) -> String {
        let n = (dv.len() / 100).max(1);
        let lits = dv[..n]
            .iter()
            .map(|s| format!("'{s}'"))
            .collect::<Vec<_>>()
            .join(",");
        format!("SELECT k, count(*) FROM t WHERE k IN ({lits}) GROUP BY k")
    }

    pub const FILTER: &[Query] = &[
        Query {
            id: "filter_eq",
            sql: SqlSpec::Dynamic(filter_eq),
        },
        Query {
            id: "filter_ne",
            sql: SqlSpec::Dynamic(filter_ne),
        },
        Query {
            id: "filter_in10",
            sql: SqlSpec::Dynamic(filter_in10),
        },
        Query {
            id: "filter_like_prefix",
            sql: SqlSpec::Dynamic(filter_like_prefix),
        },
        Query {
            id: "filter_gt",
            sql: SqlSpec::Dynamic(filter_gt),
        },
        Query {
            id: "filter_1pct_group",
            sql: SqlSpec::Dynamic(filter_1pct_group),
        },
    ];

    pub const JOIN: &[Query] = &[
        // Inner equi-join to the dim table (every row of t matches once).
        Query {
            id: "join_inner",
            sql: SqlSpec::Static("SELECT count(*) FROM t JOIN d ON t.k = d.k"),
        },
        // Inner join with projection — forces the join output to materialize
        // real columns rather than collapsing to a count.
        Query {
            id: "join_inner_project",
            sql: SqlSpec::Static("SELECT t.v, d.extra FROM t JOIN d ON t.k = d.k"),
        },
        // Left outer join — same selectivity here since d covers every key.
        Query {
            id: "join_left",
            sql: SqlSpec::Static("SELECT count(*) FROM t LEFT JOIN d ON t.k = d.k"),
        },
        // Semi join via EXISTS subquery.
        Query {
            id: "join_semi",
            sql: SqlSpec::Static(
                "SELECT count(*) FROM t WHERE EXISTS \
                 (SELECT 1 FROM d WHERE d.k = t.k)",
            ),
        },
        // Anti join against a half-populated dim (d_half holds first NDV/2
        // keys), so ~half of t rows survive the NOT EXISTS.
        Query {
            id: "join_anti_half",
            sql: SqlSpec::Static(
                "SELECT count(*) FROM t WHERE NOT EXISTS \
                 (SELECT 1 FROM d_half WHERE d_half.k = t.k)",
            ),
        },
    ];

    pub const AGGREGATION: &[Query] = &[
        // Count distinct over the dictionary-encoded column.
        Query {
            id: "agg_count_distinct_k",
            sql: SqlSpec::Static("SELECT count(DISTINCT k) FROM t"),
        },
        // Group by k, count — the canonical "how many of each key".
        Query {
            id: "agg_group_count_k",
            sql: SqlSpec::Static("SELECT k, count(*) FROM t GROUP BY k"),
        },
        // Group by k, sum a numeric payload — isolates the group-by cost
        // from the dict key side.
        Query {
            id: "agg_group_sum_v_k",
            sql: SqlSpec::Static("SELECT k, sum(v) FROM t GROUP BY k"),
        },
        // Compound group key mixing dict column with a dense int (16 buckets).
        Query {
            id: "agg_group_count_kw",
            sql: SqlSpec::Static("SELECT k, w, count(*) FROM t GROUP BY k, w"),
        },
        // HAVING over the group count — adds a post-aggregation filter.
        Query {
            id: "agg_having_k",
            sql: SqlSpec::Static(
                "SELECT k, count(*) FROM t GROUP BY k HAVING count(*) > 10",
            ),
        },
    ];

    // --- GlobalLimitExec / LocalLimitExec --------------------------------
    // LIMIT without ORDER BY. With --target-partitions 1 the plan is
    // GlobalLimitExec on top of the scan. With >1 partitions a LocalLimitExec
    // is inserted on each partition and GlobalLimit sits above the merge.
    pub const LIMIT: &[Query] = &[
        Query {
            id: "limit_k_100",
            sql: SqlSpec::Static("SELECT k FROM t LIMIT 100"),
        },
        Query {
            id: "limit_k_1k",
            sql: SqlSpec::Static("SELECT k FROM t LIMIT 1000"),
        },
        Query {
            id: "limit_k_10k",
            sql: SqlSpec::Static("SELECT k FROM t LIMIT 10000"),
        },
        Query {
            id: "limit_all_1k",
            sql: SqlSpec::Static("SELECT k, v, w FROM t LIMIT 1000"),
        },
        Query {
            id: "limit_offset_50k",
            sql: SqlSpec::Static("SELECT k FROM t LIMIT 1000 OFFSET 50000"),
        },
    ];

    // --- CoalesceBatchesExec ---------------------------------------------
    // Inserted after RepartitionExec and after highly selective filters to
    // combine skinny batches into target-size batches. Needs
    // --target-partitions > 1 to force Repartition into the plan.
    fn cb_filter_eq(dv: &[String]) -> String {
        format!("SELECT k FROM t WHERE k = '{}'", dv[0])
    }

    fn cb_filter_project(dv: &[String]) -> String {
        format!("SELECT k, v FROM t WHERE k = '{}'", dv[0])
    }

    pub const COALESCE_BATCHES: &[Query] = &[
        // Highly selective filter: ~rows/NDV survive. Many partial batches
        // feed a CoalesceBatchesExec before anything downstream can consume.
        Query {
            id: "cb_filter_eq",
            sql: SqlSpec::Dynamic(cb_filter_eq),
        },
        Query {
            id: "cb_filter_project",
            sql: SqlSpec::Dynamic(cb_filter_project),
        },
        // Non-key filter (~1% survival) with a group-by downstream — exercises
        // CoalesceBatches between Repartition(Hash(k)) and the agg.
        Query {
            id: "cb_filter_group",
            sql: SqlSpec::Static(
                "SELECT k, count(*) FROM t WHERE v % 100 = 0 GROUP BY k",
            ),
        },
    ];

    // --- CoalescePartitionsExec ------------------------------------------
    // Merge N partitioned streams into one, no ordering preserved. Needs
    // --target-partitions > 1. Showing up at the plan root for raw scans and
    // between partial and final aggregates.
    pub const COALESCE_PARTITIONS: &[Query] = &[
        // Collect a partitioned scan into a single stream.
        Query {
            id: "cp_scan_k",
            sql: SqlSpec::Static("SELECT k FROM t"),
        },
        // Partial count on each partition + CP + final count.
        Query {
            id: "cp_count_star",
            sql: SqlSpec::Static("SELECT count(*) FROM t"),
        },
        // Partial sum on each partition + CP + final sum.
        Query {
            id: "cp_sum_v",
            sql: SqlSpec::Static("SELECT sum(v) FROM t"),
        },
    ];

    // --- SortPreservingMergeExec -----------------------------------------
    // Chosen instead of CoalescePartitions when ordering must be preserved
    // across partitions (i.e. ORDER BY after per-partition sort). Needs
    // --target-partitions > 1.
    pub const SPM: &[Query] = &[
        Query {
            id: "spm_sort_k",
            sql: SqlSpec::Static("SELECT k FROM t ORDER BY k"),
        },
        Query {
            id: "spm_sort_k_limit",
            sql: SqlSpec::Static("SELECT k FROM t ORDER BY k LIMIT 1000"),
        },
        Query {
            id: "spm_sort_v_by_k",
            sql: SqlSpec::Static("SELECT v FROM t ORDER BY k"),
        },
    ];

    pub fn get(name: &str) -> Option<&'static [Query]> {
        match name {
            "scan" => Some(SCAN),
            "filter" => Some(FILTER),
            "aggregation" => Some(AGGREGATION),
            "join" => Some(JOIN),
            "sort" => Some(SORT),
            "project" => Some(PROJECT),
            "repartition" => Some(REPARTITION),
            "limit" => Some(LIMIT),
            "coalesce_batches" => Some(COALESCE_BATCHES),
            "coalesce_partitions" => Some(COALESCE_PARTITIONS),
            "spm" => Some(SPM),
            _ => None,
        }
    }

    pub fn names() -> &'static [&'static str] {
        &[
            "scan",
            "filter",
            "aggregation",
            "join",
            "sort",
            "project",
            "repartition",
            "limit",
            "coalesce_batches",
            "coalesce_partitions",
            "spm",
        ]
    }
}
