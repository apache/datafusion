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

//! Property / fuzz tests for pushing `OFFSET` (skip) into `TableScan`.
//!
//! Random `LIMIT`/`OFFSET` queries (nested, filtered, projected) using
//! "weird" values (0, 1, n-1, n, n+1, `u32::MAX`, `i64::MAX`, ...) are run
//! against:
//!
//! * a provider that opts into skip pushdown
//!   ([`TableProvider::supports_skip_pushdown`] returns `true`), and
//! * the very same provider with skip pushdown disabled,
//!
//! and the results are checked against each other and against a
//! pure-Rust oracle that slices a `Vec` of row ids.

use std::fmt::Write as _;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{ScanArgs, ScanResult, Session, TableProvider};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, Result};
use datafusion::datasource::TableType;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{
    Expr, LogicalPlan, TableProviderFilterPushDown, TableScan,
};
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::unparser::plan_to_sql;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;

/// Number of random queries per (data set, table kind, partitions) combo
const QUERIES_PER_CASE: usize = 60;

/// Ground truth for the generated test table, shared by the table
/// builders and the oracle.
///
/// Row `i` (`0..len()`) of the table has these columns:
///
/// * `id`: `i` itself. It is not stored, but derived from the position, so
///   a query result's `id`s tell the oracle exactly which rows were
///   returned.
/// * `v`: `self.v[i]`, the only stored column data.
/// * `part`: the index of the file holding row `i` (see [`Self::part_of`]).
///   It is a regular column in the in-memory and plain parquet tables, and
///   a hive partition column (`part=N/`) for [`TableKind::PartitionedParquet`].
///
/// The table is materialized from this description by [`Self::batch`],
/// [`Self::random_batches`] and `write_parquet`.
#[derive(Debug, Clone)]
struct Rows {
    /// Value of the `v` column for each row, in `id` order. Small, and
    /// sometimes null, so filters on it select a varying subset of the rows.
    v: Vec<Option<i64>>,
    /// Consecutive `id` ranges, one per parquet file (and per `part` value):
    /// file `p` holds the next `file_sizes[p]` rows after files `0..p`.
    /// Entries may be 0 (empty files); they sum to `v.len()`.
    file_sizes: Vec<usize>,
}

impl Rows {
    /// `n` rows with random `v` values (about 20% null, otherwise in
    /// `-5..5`), split at random cut points into 1 to 4 files, some of
    /// which may be empty.
    fn random(rng: &mut StdRng, n: usize) -> Self {
        let v = (0..n)
            .map(|_| {
                if rng.random_bool(0.2) {
                    None
                } else {
                    Some(rng.random_range(-5..5))
                }
            })
            .collect();
        // Split the rows into 1..=4 contiguous chunks (files / partitions),
        // allowing empty ones.
        let num_files = rng.random_range(1..=4);
        let mut cuts: Vec<usize> = (0..num_files - 1)
            .map(|_| rng.random_range(0..=n))
            .collect();
        cuts.sort_unstable();
        let mut file_sizes = vec![];
        let mut prev = 0;
        for c in cuts.into_iter().chain(std::iter::once(n)) {
            file_sizes.push(c - prev);
            prev = c;
        }
        Self { v, file_sizes }
    }

    /// Number of rows in the table
    fn len(&self) -> usize {
        self.v.len()
    }

    /// Index of the file (and so the `part` value) holding row `id`.
    ///
    /// Panics if `id >= self.len()`.
    fn part_of(&self, id: usize) -> usize {
        let mut end = 0;
        for (p, s) in self.file_sizes.iter().enumerate() {
            end += s;
            if id < end {
                return p;
            }
        }
        unreachable!("id {id} out of range")
    }

    /// The row ids a sequential scan returns, for every possible order in
    /// which the files can be read (one `Vec` per permutation of the files).
    ///
    /// `ListingTable` doesn't read files in a fixed order, so the oracle
    /// accepts a result if it matches any of these.
    fn file_orders(&self) -> Vec<Vec<usize>> {
        let mut ranges = vec![];
        let mut start = 0;
        for &size in &self.file_sizes {
            ranges.push(start..start + size);
            start += size;
        }
        let mut out = vec![];
        permute(&mut ranges, 0, &mut out);
        out
    }

    /// Schema of the table: `id` and `v` (both `Int64`, `v` nullable), plus
    /// a `part` (`Utf8`) column if `with_part` is true.
    ///
    /// `with_part` is false for the files of a hive partitioned table, where
    /// `part` comes from the directory name instead of the file.
    fn schema(with_part: bool) -> SchemaRef {
        let mut fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ];
        if with_part {
            fields.push(Field::new("part", DataType::Utf8, false));
        }
        Arc::new(Schema::new(fields))
    }

    /// A batch with the rows `start..end`, in `id` order, using
    /// [`Self::schema`]`(with_part)`.
    fn batch(&self, start: usize, end: usize, with_part: bool) -> RecordBatch {
        let mut cols: Vec<Arc<dyn Array>> = vec![
            Arc::new(Int64Array::from_iter_values((start..end).map(|i| i as i64))),
            Arc::new(Int64Array::from(self.v[start..end].to_vec())),
        ];
        if with_part {
            cols.push(Arc::new(StringArray::from_iter_values(
                (start..end).map(|i| self.part_of(i).to_string()),
            )));
        }
        RecordBatch::try_new(Self::schema(with_part), cols).unwrap()
    }

    /// All rows, in `id` order, split into batches of 1 to 37 rows each,
    /// so a scan crosses many batch boundaries. Includes the `part` column.
    fn random_batches(&self, rng: &mut StdRng) -> Vec<RecordBatch> {
        let mut out = vec![];
        let mut start = 0;
        while start < self.len() {
            let end = (start + rng.random_range(1..=37)).min(self.len());
            out.push(self.batch(start, end, true));
            start = end;
        }
        out
    }
}

fn permute(
    ranges: &mut Vec<std::ops::Range<usize>>,
    k: usize,
    out: &mut Vec<Vec<usize>>,
) {
    if k == ranges.len() {
        out.push(ranges.iter().flat_map(|r| r.clone()).collect());
        return;
    }
    for i in k..ranges.len() {
        ranges.swap(k, i);
        permute(ranges, k + 1, out);
        ranges.swap(k, i);
    }
}

// ---------------------------------------------------------------------------
// Providers
// ---------------------------------------------------------------------------

/// An in-memory, single-partition provider that honours `skip`, `limit`
/// *and* filters exactly (filters are `Exact`, so the optimizer removes the
/// `Filter` node and `skip` is pushed "past" the filters), exercising the
/// documented evaluation order: filters, then skip/limit, then projection.
#[derive(Debug)]
struct ExactSkipTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    skip_pushdown: bool,
}

#[async_trait]
impl TableProvider for ExactSkipTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_skip_pushdown(&self) -> bool {
        self.skip_pushdown
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&[usize]>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let args = ScanArgs::default()
            .with_projection(projection)
            .with_filters(Some(filters))
            .with_limit(limit);
        Ok(Arc::clone(self.scan_with_args(state, args).await?.plan()))
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> Result<ScanResult> {
        if !self.skip_pushdown {
            assert_eq!(
                args.skip(),
                None,
                "skip pushed to a provider that opted out"
            );
        }
        let mut plan: Arc<dyn ExecutionPlan> = MemorySourceConfig::try_new_exec(
            std::slice::from_ref(&self.batches),
            Arc::clone(&self.schema),
            None,
        )?;

        // 1. filters
        let filters = args.filters().unwrap_or(&[]).to_vec();
        if let Some(predicate) = conjunction(filters) {
            let predicate = unqualify(predicate)?;
            let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
            let predicate = state.create_physical_expr(predicate, &df_schema)?;
            plan = Arc::new(FilterExec::try_new(predicate, plan)?);
        }

        // 2. skip + limit
        let skip = args.skip().unwrap_or(0);
        if skip > 0 || args.limit().is_some() {
            plan = Arc::new(GlobalLimitExec::new(plan, skip, args.limit()));
        }

        // 3. projection
        if let Some(projection) = args.projection() {
            let exprs = projection.iter().map(|&i| {
                let name = self.schema.field(i).name();
                ProjectionExpr {
                    expr: Arc::new(PhysicalColumn::new(name, i)),
                    alias: name.to_string(),
                }
            });
            plan = Arc::new(ProjectionExec::try_new(exprs, plan)?);
        }
        Ok(ScanResult::new(plan))
    }
}

fn unqualify(expr: Expr) -> Result<Expr> {
    expr.transform(|e| match e {
        Expr::Column(c) => Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
            c.name,
        )))),
        e => Ok(Transformed::no(e)),
    })
    .map(|t| t.data)
}

/// Delegates to `inner` but refuses skip pushdown. Used as the reference
/// ("no pushdown") side for [`ListingTable`] based tables.
#[derive(Debug)]
struct NoSkipPushdown(Arc<dyn TableProvider>);

#[async_trait]
impl TableProvider for NoSkipPushdown {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn table_type(&self) -> TableType {
        self.0.table_type()
    }

    fn supports_skip_pushdown(&self) -> bool {
        false
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.0.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&[usize]>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.0.scan(state, projection, filters, limit).await
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> Result<ScanResult> {
        assert_eq!(
            args.skip(),
            None,
            "skip pushed to a provider that opted out"
        );
        self.0.scan_with_args(state, args).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum TableKind {
    /// [`ExactSkipTable`]: exact filters + skip
    Exact,
    /// [`ListingTable`] over several parquet files
    Parquet,
    /// [`ListingTable`] over hive partitioned parquet files (`part=N/`),
    /// filters on `part` are `Exact`
    PartitionedParquet,
}

/// Writes `rows` as parquet files, one per `rows.file_sizes` entry, with
/// tiny random row groups so that the scan crosses many row group and
/// file boundaries.
fn write_parquet(
    rng: &mut StdRng,
    rows: &Rows,
    dir: &TempDir,
    partitioned: bool,
) -> Result<()> {
    let mut start = 0;
    for (i, &size) in rows.file_sizes.iter().enumerate() {
        let (path, with_part) = if partitioned {
            let d = dir.path().join(format!("part={i}"));
            std::fs::create_dir_all(&d)?;
            (d.join("data.parquet"), false)
        } else {
            (dir.path().join(format!("{i:03}.parquet")), true)
        };
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(rng.random_range(1..=16)))
            .build();
        let file = std::fs::File::create(path)?;
        let mut writer =
            ArrowWriter::try_new(file, Rows::schema(with_part), Some(props))?;
        let mut s = start;
        while s < start + size {
            let e = (s + rng.random_range(1..=23)).min(start + size);
            writer.write(&rows.batch(s, e, with_part))?;
            s = e;
        }
        writer.close()?;
        start += size;
    }
    Ok(())
}

async fn listing_table(
    ctx: &SessionContext,
    dir: &TempDir,
    partitioned: bool,
) -> Result<Arc<dyn TableProvider>> {
    let mut options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension(".parquet");
    if partitioned {
        options =
            options.with_table_partition_cols(vec![("part".to_string(), DataType::Utf8)]);
    }
    let url = ListingTableUrl::parse(dir.path().to_str().unwrap())?;
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .infer_schema(&ctx.state())
        .await?;
    Ok(Arc::new(ListingTable::try_new(config)?))
}

// ---------------------------------------------------------------------------
// Query generation + oracle
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum Pred {
    VGreater(i64),
    VIsNull,
    IdModEq(i64, i64),
    PartEq(usize),
    PartGe(usize),
}

impl Pred {
    fn random(rng: &mut StdRng, rows: &Rows, has_part: bool) -> Option<Self> {
        let parts = rows.file_sizes.len();
        match rng.random_range(0..if has_part { 7 } else { 5 }) {
            0 | 1 => None,
            2 => Some(Pred::VGreater(rng.random_range(-6..6))),
            3 => Some(Pred::VIsNull),
            4 => {
                let m = rng.random_range(1..5);
                Some(Pred::IdModEq(m, rng.random_range(0..m)))
            }
            5 => Some(Pred::PartEq(rng.random_range(0..=parts))),
            _ => Some(Pred::PartGe(rng.random_range(0..=parts))),
        }
    }

    fn sql(&self) -> String {
        match self {
            Pred::VGreater(k) => format!("v > {k}"),
            Pred::VIsNull => "v IS NULL".to_string(),
            Pred::IdModEq(m, r) => format!("id % {m} = {r}"),
            Pred::PartEq(p) => format!("part = '{p}'"),
            // string comparison on single digit values == numeric comparison
            Pred::PartGe(p) => format!("part >= '{p}'"),
        }
    }

    fn eval(&self, rows: &Rows, id: usize) -> bool {
        match self {
            Pred::VGreater(k) => rows.v[id].is_some_and(|v| v > *k),
            Pred::VIsNull => rows.v[id].is_none(),
            Pred::IdModEq(m, r) => (id as i64) % m == *r,
            Pred::PartEq(p) => rows.part_of(id) == *p,
            Pred::PartGe(p) => rows.part_of(id) >= *p,
        }
    }
}

/// One `SELECT ... [LIMIT l] [OFFSET o]` layer
#[derive(Debug, Clone, Copy)]
struct Layer {
    limit: Option<u64>,
    offset: Option<u64>,
    /// Render the values as constant expressions (e.g. `(3 - 1) + 1`) that
    /// only become literals after expression simplification
    as_expr: bool,
    /// `LIMIT ... OFFSET ...` vs `OFFSET ... LIMIT ...`
    offset_first: bool,
    /// Add a computed column so the layer has a real projection
    project: bool,
}

/// "Weird" values relative to the table size `n`
fn weird_value(rng: &mut StdRng, n: usize) -> u64 {
    let n = n as u64;
    match rng.random_range(0..14) {
        0 => 0,
        1 => 1,
        2 => 2,
        3 => n.saturating_sub(1),
        4 => n,
        5 => n + 1,
        6 => 2 * n,
        7 => u32::MAX as u64,
        8 => u32::MAX as u64 + 1,
        9 => i64::MAX as u64,
        10 => i64::MAX as u64 - 1,
        11 => i64::MAX as u64 / 2 + 1,
        _ => rng.random_range(0..=n + 3),
    }
}

impl Layer {
    fn random(rng: &mut StdRng, n: usize) -> Self {
        Self {
            limit: rng.random_bool(0.7).then(|| weird_value(rng, n)),
            offset: rng.random_bool(0.8).then(|| weird_value(rng, n)),
            as_expr: rng.random_bool(0.15),
            offset_first: rng.random_bool(0.3),
            project: rng.random_bool(0.3),
        }
    }

    fn value_sql(&self, v: u64) -> String {
        if self.as_expr && v > 0 && v < i64::MAX as u64 {
            format!("({v} - 1) + 1")
        } else {
            v.to_string()
        }
    }

    fn clause_sql(&self) -> String {
        let limit = self
            .limit
            .map(|l| format!(" LIMIT {}", self.value_sql(l)))
            .unwrap_or_default();
        let offset = self
            .offset
            .map(|o| format!(" OFFSET {}", self.value_sql(o)))
            .unwrap_or_default();
        if self.offset_first {
            format!("{offset}{limit}")
        } else {
            format!("{limit}{offset}")
        }
    }

    fn apply(&self, ids: Vec<usize>) -> Vec<usize> {
        let skip = usize::try_from(self.offset.unwrap_or(0)).unwrap_or(usize::MAX);
        let fetch = self
            .limit
            .map(|l| usize::try_from(l).unwrap_or(usize::MAX))
            .unwrap_or(usize::MAX);
        ids.into_iter().skip(skip).take(fetch).collect()
    }
}

#[derive(Debug, Clone)]
struct Query {
    pred: Option<Pred>,
    /// innermost first
    layers: Vec<Layer>,
}

impl Query {
    fn random(rng: &mut StdRng, rows: &Rows, has_part: bool) -> Self {
        let num_layers = rng.random_range(1..=3);
        Self {
            pred: Pred::random(rng, rows, has_part),
            layers: (0..num_layers)
                .map(|_| Layer::random(rng, rows.len()))
                .collect(),
        }
    }

    fn sql(&self, table: &str) -> String {
        let mut sql = String::new();
        for (i, layer) in self.layers.iter().enumerate() {
            let cols = if layer.project {
                "id, v, v + 1 AS w"
            } else {
                "id, v"
            };
            if i == 0 {
                write!(sql, "SELECT {cols} FROM {table}").unwrap();
                if let Some(pred) = &self.pred {
                    write!(sql, " WHERE {}", pred.sql()).unwrap();
                }
            } else {
                sql = format!("SELECT {cols} FROM ({sql}) AS l{i}");
            }
            sql.push_str(&layer.clause_sql());
        }
        sql
    }

    fn total_offset(&self) -> u128 {
        self.layers
            .iter()
            .map(|l| l.offset.unwrap_or(0) as u128)
            .sum()
    }

    fn expected(&self, rows: &Rows) -> Vec<usize> {
        self.expected_in_order(rows, (0..rows.len()).collect())
    }

    /// Expected result if the scan produced the rows in `scan_order`
    fn expected_in_order(&self, rows: &Rows, scan_order: Vec<usize>) -> Vec<usize> {
        let mut ids: Vec<usize> = scan_order
            .into_iter()
            .filter(|&id| self.pred.is_none_or(|p| p.eval(rows, id)))
            .collect();
        for layer in &self.layers {
            ids = layer.apply(ids);
        }
        ids
    }
}

async fn run_ids(ctx: &SessionContext, sql: &str) -> Result<Vec<usize>> {
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut ids = vec![];
    for batch in batches {
        let col = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        ids.extend(col.values().iter().map(|&i| i as usize));
    }
    Ok(ids)
}

fn scans_with_skip(plan: &LogicalPlan) -> usize {
    let mut count = 0;
    plan.apply(|p| {
        if let LogicalPlan::TableScan(TableScan { skip: Some(_), .. }) = p {
            count += 1;
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })
    .unwrap();
    count
}

/// Runs `QUERIES_PER_CASE` random queries against `t` (skip pushdown) and
/// `t_ref` (no skip pushdown) and checks both against the oracle.
///
/// Returns the number of queries whose optimized plan had a `TableScan`
/// with `skip` set, so callers can assert the feature was exercised.
async fn check_queries(
    rng: &mut StdRng,
    ctx: &SessionContext,
    rows: &Rows,
    kind: TableKind,
    target_partitions: usize,
    seed: u64,
) -> usize {
    let has_part = kind != TableKind::Parquet;
    let mut pushed = 0;
    for _ in 0..QUERIES_PER_CASE {
        let query = Query::random(rng, rows, has_part);
        let sql = query.sql("t");
        let ref_sql = query.sql("t_ref");
        let ctx_msg = format!(
            "seed={seed} kind={kind:?} target_partitions={target_partitions} \
             n={} files={:?}\nsql: {sql}",
            rows.len(),
            rows.file_sizes
        );

        let actual = run_ids(ctx, &sql).await;
        let reference = run_ids(ctx, &ref_sql).await;
        let expected = query.expected(rows);

        let (actual, reference) = match (actual, reference) {
            (Ok(a), Ok(r)) => (a, r),
            // Pre-existing (also on `main`, independent of skip pushdown):
            // merging adjacent `Limit`s saturates the combined skip in
            // `usize` and then re-encodes it with `lit(skip as i64)`, which
            // wraps to a negative OFFSET once the sum exceeds `i64::MAX`.
            (Err(a), Err(r))
                if a.to_string() == r.to_string()
                    && a.to_string().contains("OFFSET must be >=0")
                    && query.total_offset() > i64::MAX as u128 =>
            {
                continue;
            }
            (a, r) => panic!(
                "{ctx_msg}\nexpected {} rows, pushdown: {:?}, reference: {:?}",
                expected.len(),
                a.map(|v| v.len()),
                r.map(|v| v.len())
            ),
        };

        let plan = ctx.sql(&sql).await.unwrap().into_optimized_plan().unwrap();
        let skip_scans = scans_with_skip(&plan);
        pushed += usize::from(skip_scans > 0);

        // A single partition reads the files sequentially, but a
        // `ListingTable` lists (and, with a limit, collects statistics for)
        // its files concurrently, so the files may be read in any order:
        // the result must match the oracle for *some* order of the files.
        let candidates: Vec<Vec<usize>> = match kind {
            TableKind::Exact => vec![expected.clone()],
            TableKind::Parquet | TableKind::PartitionedParquet => rows
                .file_orders()
                .into_iter()
                .map(|order| query.expected_in_order(rows, order))
                .collect(),
        };
        if target_partitions == 1 {
            assert!(
                candidates.contains(&actual),
                "{ctx_msg}\nplan:\n{plan}\nactual: {actual:?}\ncandidates: {candidates:?}"
            );
            assert!(
                candidates.contains(&reference),
                "{ctx_msg} (reference)\nreference: {reference:?}\ncandidates: {candidates:?}"
            );
        } else {
            // With several partitions the order in which rows arrive at the
            // limit is not deterministic, so only the row count is.
            assert_eq!(actual.len(), expected.len(), "{ctx_msg}\nplan:\n{plan}");
            assert_eq!(reference.len(), expected.len(), "{ctx_msg} (reference)");
            let mut dedup = actual.clone();
            dedup.sort_unstable();
            dedup.dedup();
            assert_eq!(dedup.len(), actual.len(), "{ctx_msg}: duplicate rows");
            let filtered = Query {
                pred: query.pred,
                layers: vec![],
            }
            .expected(rows);
            for id in &actual {
                assert!(filtered.contains(id), "{ctx_msg}: unexpected row {id}");
            }
        }

        // The optimized plan (with `TableScan::skip`) must unparse to SQL
        // that still produces the same rows.
        if skip_scans > 0 && target_partitions == 1 {
            let unparsed = plan_to_sql(&plan)
                .unwrap_or_else(|e| panic!("{ctx_msg}\nunparse failed: {e}"))
                .to_string();
            let roundtrip = run_ids(ctx, &unparsed).await.unwrap_or_else(|e| {
                panic!("{ctx_msg}\nunparsed: {unparsed}\nfailed: {e}")
            });
            assert!(
                candidates.contains(&roundtrip),
                "{ctx_msg}\nunparsed: {unparsed}\nplan:\n{plan}\nroundtrip: {roundtrip:?}\ncandidates: {candidates:?}"
            );
        }
    }
    pushed
}

/// Calls [`TableProvider::scan_with_args`] directly (no optimizer, no outer
/// `Limit` to hide mistakes) and checks the provider returns exactly
/// `min(limit, n - skip)` rows, as documented for `ScanArgs::skip`.
async fn check_direct_scans(
    rng: &mut StdRng,
    ctx: &SessionContext,
    table: &dyn TableProvider,
    n: usize,
    seed: u64,
) {
    let state = ctx.state();
    for _ in 0..10 {
        let skip = rng
            .random_bool(0.9)
            .then(|| usize::try_from(weird_value(rng, n)).unwrap_or(usize::MAX));
        let limit = rng
            .random_bool(0.8)
            .then(|| usize::try_from(weird_value(rng, n)).unwrap_or(usize::MAX));
        let args = ScanArgs::default().with_skip(skip).with_limit(limit);
        let mut plan = table
            .scan_with_args(&state, args)
            .await
            .unwrap()
            .into_inner();
        // A provider's plan may rely on the physical optimizer to satisfy
        // its operators' input requirements (e.g. `ListingTable` returns a
        // `GlobalLimitExec` over a multi-partition scan and relies on
        // `EnforceDistribution` to coalesce it), exactly as during a query.
        for rule in state.physical_optimizers() {
            plan = rule.optimize(plan, state.config_options()).unwrap();
        }
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        let actual: usize = batches.iter().map(|b| b.num_rows()).sum();
        let expected = n
            .saturating_sub(skip.unwrap_or(0))
            .min(limit.unwrap_or(usize::MAX));
        let msg = format!(
            "seed={seed} n={n} direct scan_with_args skip={skip:?} limit={limit:?}"
        );
        if skip.is_some() {
            // `skip` must be honoured exactly, and so must the `limit`
            // that comes with it
            assert_eq!(actual, expected, "{msg}");
        } else {
            // Pre-existing (also on `main`): without a skip, `ListingTable`
            // only uses `limit` as a per-file-group / row-group hint and may
            // return *more* rows than `limit`, relying on the `Limit` above
            // the scan, although `TableProvider::scan` documents "at most".
            assert!(actual >= expected && actual <= n, "{msg}: got {actual}");
        }
    }
}

async fn run_fuzz(kind: TableKind) {
    let seed: u64 = std::env::var("SKIP_PUSHDOWN_FUZZ_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(rand::random);
    println!("SKIP_PUSHDOWN_FUZZ_SEED={seed}");
    let mut rng = StdRng::seed_from_u64(seed);

    let mut pushed = 0;
    for n in [0, 1, 2, 7, 64, 333, rng.random_range(0..2000)] {
        let rows = Rows::random(&mut rng, n);
        let dir = TempDir::new().unwrap();
        for target_partitions in [1, 4] {
            let ctx = SessionContext::new_with_config(
                SessionConfig::new()
                    .with_target_partitions(target_partitions)
                    .with_batch_size(rng.random_range(1..=64)),
            );

            let (t, t_ref): (Arc<dyn TableProvider>, Arc<dyn TableProvider>) = match kind
            {
                TableKind::Exact => {
                    let batches = rows.random_batches(&mut rng);
                    let schema = Rows::schema(true);
                    (
                        Arc::new(ExactSkipTable {
                            schema: Arc::clone(&schema),
                            batches: batches.clone(),
                            skip_pushdown: true,
                        }),
                        Arc::new(ExactSkipTable {
                            schema,
                            batches,
                            skip_pushdown: false,
                        }),
                    )
                }
                TableKind::Parquet | TableKind::PartitionedParquet => {
                    let partitioned = kind == TableKind::PartitionedParquet;
                    if target_partitions == 1 {
                        write_parquet(&mut rng, &rows, &dir, partitioned).unwrap();
                    }
                    let t = listing_table(&ctx, &dir, partitioned).await.unwrap();
                    (Arc::clone(&t), Arc::new(NoSkipPushdown(t)))
                }
            };
            check_direct_scans(&mut rng, &ctx, t.as_ref(), n, seed).await;
            ctx.register_table("t", t).unwrap();
            ctx.register_table("t_ref", t_ref).unwrap();

            if n > 0 {
                // Sanity: the full scan returns the rows in oracle order
                let all = run_ids(&ctx, "SELECT id FROM t").await.unwrap();
                if target_partitions == 1 && kind != TableKind::Exact {
                    assert!(rows.file_orders().contains(&all), "seed={seed}");
                } else if target_partitions == 1 {
                    assert_eq!(all, (0..n).collect::<Vec<_>>(), "seed={seed}");
                } else {
                    assert_eq!(all.len(), n, "seed={seed}");
                }
            }

            pushed +=
                check_queries(&mut rng, &ctx, &rows, kind, target_partitions, seed).await;
        }
    }
    assert!(
        pushed > 0,
        "seed={seed}: skip was never pushed into a TableScan"
    );
}

#[tokio::test]
async fn skip_pushdown_fuzz_exact_provider() {
    run_fuzz(TableKind::Exact).await
}

#[tokio::test]
async fn skip_pushdown_fuzz_parquet() {
    run_fuzz(TableKind::Parquet).await
}

#[tokio::test]
async fn skip_pushdown_fuzz_partitioned_parquet() {
    run_fuzz(TableKind::PartitionedParquet).await
}

/// `LIMIT $1 OFFSET $2` only becomes a literal after parameter binding
#[tokio::test]
async fn skip_pushdown_prepared_statement() -> Result<()> {
    let mut rng = StdRng::seed_from_u64(42);
    let rows = Rows::random(&mut rng, 100);
    let dir = TempDir::new()?;
    write_parquet(&mut rng, &rows, &dir, false)?;
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_table("t", listing_table(&ctx, &dir, false).await?)?;
    ctx.sql("PREPARE q(BIGINT, BIGINT) AS SELECT id FROM t LIMIT $1 OFFSET $2")
        .await?
        .collect()
        .await?;
    for (limit, offset) in [
        (0, 0),
        (3, 0),
        (3, 97),
        (3, 99),
        (3, 100),
        (3, 101),
        (i64::MAX, 1),
        (1, i64::MAX),
        (i64::MAX, i64::MAX),
    ] {
        let ids = run_ids(&ctx, &format!("EXECUTE q({limit}, {offset})")).await?;
        let expected: Vec<usize> = (0..100)
            .skip(offset as usize)
            .take(limit as usize)
            .collect();
        assert_eq!(ids, expected, "LIMIT {limit} OFFSET {offset}");
    }
    Ok(())
}
