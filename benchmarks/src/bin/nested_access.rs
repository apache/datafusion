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

//! Benchmarks scalar functions that read part of a nested column, through
//! the `NestedInputAccess` capability.
//!
//! Generates a Parquet file with two equivalent columns:
//!
//! - `v`: a Variant-shaped struct, `Struct<metadata, value,
//!   typed_value: Struct<k0..kN: Struct<typed_value: Int64, value: Binary>>>`,
//!   read with `variant_like_get(v, 'kI')`. A value is in `typed_value`, or
//!   in the untyped fallback `value` (8 little-endian bytes) if it fell back.
//! - `s`: a plain struct, `Struct<a0..aN: Int64>`, read with
//!   `struct_get(s, 'aI')`, which behaves like `get_field`.
//!
//! `k0` / `a0` increase with the row number, so filters on them are
//! selective by row group and page. Some row groups have a fallback value for
//! `k0`, so statistics must not be used to skip them.
//!
//! Each query runs with each function in three modes:
//!
//! - `off`: no capability; the whole column is read.
//! - `reads`: `required_fields` only. For `struct_get`, this is how
//!   `get_field` behaves without this change.
//! - `reads+stats`: `required_fields` and `output_statistics`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, Int64Array, RecordBatch, StructArray,
};
use arrow::datatypes::{DataType, Field, Fields, Int64Type, Schema, SchemaRef};
use clap::Parser;
use datafusion::common::instant::Instant;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Result, Statistics};
use datafusion::logical_expr::{
    ColumnarValue, ExpressionPlacement, FieldPath, NestedInputAccess, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

#[derive(Debug, Parser)]
#[command(about = "Benchmark nested input access for Parquet scans")]
struct Args {
    /// Number of rows to generate.
    #[arg(long, default_value_t = 4_000_000)]
    rows: usize,
    /// Number of shredded keys / struct fields.
    #[arg(long, default_value_t = 50)]
    keys: usize,
    /// Rows per row group.
    #[arg(long, default_value_t = 131_072)]
    row_group_rows: usize,
    /// Every n-th row group has one fallback value for `k0`.
    #[arg(long, default_value_t = 4)]
    fallback_every: usize,
    /// Timed iterations per query and mode (after one warm-up run).
    #[arg(long, default_value_t = 5)]
    iterations: usize,
    /// Directory for the generated file (reused if present).
    #[arg(long, default_value = "data/nested_access")]
    path: PathBuf,
    /// Also run with `datafusion.execution.parquet.pushdown_filters=true`.
    #[arg(long)]
    pushdown_filters: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Mode {
    Off,
    Reads,
    ReadsAndStats,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Mode::Off => "off",
            Mode::Reads => "reads",
            Mode::ReadsAndStats => "reads+stats",
        }
    }
}

/// Returns the literal string argument at `index`.
fn literal_key<'a>(args: &'a ReturnFieldArgs, index: usize) -> Option<&'a str> {
    args.scalar_arguments
        .get(index)?
        .and_then(|value| value.try_as_str().flatten())
}

fn constant_key(args: &ScalarFunctionArgs) -> String {
    let ColumnarValue::Scalar(key) = &args.args[1] else {
        panic!("keys must be constant")
    };
    key.try_as_str().flatten().unwrap().to_string()
}

fn leaf_placement(args: &[ExpressionPlacement]) -> ExpressionPlacement {
    if args.first() == Some(&ExpressionPlacement::Column)
        && args[1..]
            .iter()
            .all(|arg| *arg == ExpressionPlacement::Literal)
    {
        ExpressionPlacement::MoveTowardsLeafNodes
    } else {
        ExpressionPlacement::KeepInPlace
    }
}

/// `variant_like_get(v, 'key')`: see the module documentation.
#[derive(Debug, PartialEq, Eq, Hash)]
struct VariantLikeGet {
    signature: Signature,
    mode: Mode,
}

impl NestedInputAccess for VariantLikeGet {
    fn required_fields(&self, args: &ReturnFieldArgs) -> Vec<Option<Vec<FieldPath>>> {
        let Some(key) = literal_key(args, 1) else {
            return vec![];
        };
        let leaf = |name: &str| vec!["typed_value".into(), key.into(), name.into()];
        vec![Some(vec![leaf("typed_value"), leaf("value")])]
    }

    fn output_statistics(
        &self,
        _args: &ReturnFieldArgs,
        inputs: &Statistics,
    ) -> Option<ColumnStatistics> {
        if self.mode != Mode::ReadsAndStats {
            return None;
        }
        let [typed, fallback] = inputs.column_statistics.as_slice() else {
            return None;
        };
        // The typed leaf describes the output only if nothing fell back.
        match (fallback.null_count, inputs.num_rows) {
            (Precision::Exact(nulls), Precision::Exact(rows)) if nulls == rows => {
                Some(typed.clone())
            }
            _ => None,
        }
    }
}

impl ScalarUDFImpl for VariantLikeGet {
    fn name(&self) -> &str {
        "variant_like_get"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let key = constant_key(&args);
        let v = args.args[0].to_array(args.number_rows)?;
        let entry = v
            .as_struct()
            .column_by_name("typed_value")
            .unwrap()
            .as_struct()
            .column_by_name(&key)
            .unwrap()
            .as_struct();
        let typed = entry
            .column_by_name("typed_value")
            .unwrap()
            .as_primitive::<Int64Type>();
        let fallback = entry.column_by_name("value").unwrap().as_binary::<i32>();
        let result = if fallback.null_count() == fallback.len() {
            typed.clone()
        } else {
            (0..v.len())
                .map(|i| {
                    if typed.is_valid(i) {
                        Some(typed.value(i))
                    } else if fallback.is_valid(i) {
                        Some(i64::from_le_bytes(fallback.value(i).try_into().unwrap()))
                    } else {
                        None
                    }
                })
                .collect()
        };
        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        leaf_placement(args)
    }

    fn as_nested_input_access(&self) -> Option<&dyn NestedInputAccess> {
        (self.mode != Mode::Off).then_some(self as &dyn NestedInputAccess)
    }
}

/// `struct_get(s, 'field')`: `get_field` on a struct, with a switchable
/// capability.
#[derive(Debug, PartialEq, Eq, Hash)]
struct StructGet {
    signature: Signature,
    mode: Mode,
}

impl NestedInputAccess for StructGet {
    fn required_fields(&self, args: &ReturnFieldArgs) -> Vec<Option<Vec<FieldPath>>> {
        let Some(key) = literal_key(args, 1) else {
            return vec![];
        };
        vec![Some(vec![vec![key.into()]])]
    }

    fn output_statistics(
        &self,
        _args: &ReturnFieldArgs,
        inputs: &Statistics,
    ) -> Option<ColumnStatistics> {
        (self.mode == Mode::ReadsAndStats)
            .then(|| inputs.column_statistics.first().cloned())
            .flatten()
    }
}

impl ScalarUDFImpl for StructGet {
    fn name(&self) -> &str {
        "struct_get"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let key = constant_key(&args);
        let s = args.args[0].to_array(args.number_rows)?;
        Ok(ColumnarValue::Array(Arc::clone(
            s.as_struct().column_by_name(&key).unwrap(),
        )))
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        leaf_placement(args)
    }

    fn as_nested_input_access(&self) -> Option<&dyn NestedInputAccess> {
        (self.mode != Mode::Off).then_some(self as &dyn NestedInputAccess)
    }
}

fn schema(keys: usize) -> SchemaRef {
    let entry = DataType::Struct(Fields::from(vec![
        Field::new("typed_value", DataType::Int64, true),
        Field::new("value", DataType::Binary, true),
    ]));
    let typed_value = Fields::from(
        (0..keys)
            .map(|k| Field::new(format!("k{k}"), entry.clone(), true))
            .collect::<Vec<_>>(),
    );
    let v = Fields::from(vec![
        Field::new("metadata", DataType::Binary, true),
        Field::new("value", DataType::Binary, true),
        Field::new("typed_value", DataType::Struct(typed_value), true),
    ]);
    let s = Fields::from(
        (0..keys)
            .map(|k| Field::new(format!("a{k}"), DataType::Int64, true))
            .collect::<Vec<_>>(),
    );
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Struct(v), true),
        Field::new("s", DataType::Struct(s), true),
    ]))
}

/// Builds rows `start..end`.
fn batch(schema: &SchemaRef, args: &Args, start: usize, end: usize) -> RecordBatch {
    let rows = end - start;
    let mut rng = StdRng::seed_from_u64(start as u64);
    let DataType::Struct(v_fields) = schema.field(1).data_type() else {
        unreachable!()
    };
    let DataType::Struct(s_fields) = schema.field(2).data_type() else {
        unreachable!()
    };
    let DataType::Struct(typed_value_fields) = v_fields[2].data_type() else {
        unreachable!()
    };
    let DataType::Struct(entry_fields) = typed_value_fields[0].data_type() else {
        unreachable!()
    };

    let mut typed_values = vec![];
    let mut entries = vec![];
    for k in 0..args.keys {
        let values: Vec<i64> = if k == 0 {
            (start as i64..end as i64).collect()
        } else {
            (0..rows).map(|_| rng.random_range(0..1_000_000)).collect()
        };
        // One fallback value for k0 in the middle of every
        // `fallback_every`-th row group.
        let fallback_row = |row: usize| {
            k == 0
                && (row / args.row_group_rows).is_multiple_of(args.fallback_every)
                && row % args.row_group_rows == args.row_group_rows / 2
        };
        let typed = (start..end)
            .zip(&values)
            .map(|(row, value)| (!fallback_row(row)).then_some(*value))
            .collect::<Int64Array>();
        let fallback = (start..end)
            .zip(&values)
            .map(|(row, value)| fallback_row(row).then(|| value.to_le_bytes()))
            .collect::<BinaryArray>();
        entries.push(Arc::new(
            StructArray::try_new(
                entry_fields.clone(),
                vec![Arc::new(typed) as ArrayRef, Arc::new(fallback)],
                None,
            )
            .unwrap(),
        ) as ArrayRef);
        typed_values.push(Arc::new(Int64Array::from(values)) as ArrayRef);
    }

    let typed_value =
        StructArray::try_new(typed_value_fields.clone(), entries, None).unwrap();
    let v = StructArray::try_new(
        v_fields.clone(),
        vec![
            Arc::new(BinaryArray::from_iter_values(
                (0..rows).map(|_| [1u8, 0, 0]),
            )) as ArrayRef,
            Arc::new(BinaryArray::new_null(rows)),
            Arc::new(typed_value),
        ],
        None,
    )
    .unwrap();
    let s = StructArray::try_new(s_fields.clone(), typed_values, None).unwrap();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from_iter_values(start as i64..end as i64)),
            Arc::new(v),
            Arc::new(s),
        ],
    )
    .unwrap()
}

fn generate(args: &Args) -> PathBuf {
    let file = args.path.join(format!(
        "rows{}_keys{}_rg{}_fb{}.parquet",
        args.rows, args.keys, args.row_group_rows, args.fallback_every
    ));
    if file.exists() {
        return file;
    }
    std::fs::create_dir_all(&args.path).unwrap();
    println!("Generating {}", file.display());
    let schema = schema(args.keys);
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(args.row_group_rows))
        .build();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&file).unwrap(),
        Arc::clone(&schema),
        Some(props),
    )
    .unwrap();
    let chunk = 65_536;
    for start in (0..args.rows).step_by(chunk) {
        let end = (start + chunk).min(args.rows);
        writer.write(&batch(&schema, args, start, end)).unwrap();
    }
    writer.close().unwrap();
    file
}

struct Run {
    elapsed: Duration,
    output: String,
    bytes_scanned: usize,
    row_groups_pruned: usize,
    page_rows_pruned: usize,
}

fn find_scan_metrics(plan: &Arc<dyn ExecutionPlan>) -> Option<MetricsSet> {
    if plan.name() == "DataSourceExec" {
        return plan.metrics();
    }
    plan.children().into_iter().find_map(find_scan_metrics)
}

fn metric(metrics: &MetricsSet, name: &str) -> usize {
    metrics
        .iter()
        .filter(|metric| metric.value().name() == name)
        .map(|metric| match metric.value() {
            MetricValue::PruningMetrics {
                pruning_metrics, ..
            } => pruning_metrics.pruned(),
            value => value.as_usize(),
        })
        .sum()
}

async fn run(file: &Path, sql: &str, mode: Mode, pushdown_filters: bool) -> Run {
    let config = SessionConfig::new().set_bool(
        "datafusion.execution.parquet.pushdown_filters",
        pushdown_filters,
    );
    let ctx = SessionContext::new_with_config(config);
    ctx.register_udf(ScalarUDF::from(VariantLikeGet {
        signature: Signature::any(2, Volatility::Immutable),
        mode,
    }));
    ctx.register_udf(ScalarUDF::from(StructGet {
        signature: Signature::any(2, Volatility::Immutable),
        mode,
    }));
    ctx.register_parquet("t", file.to_str().unwrap(), ParquetReadOptions::default())
        .await
        .unwrap();

    let start = Instant::now();
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
    let elapsed = start.elapsed();
    let metrics = find_scan_metrics(&plan).unwrap_or_default();
    Run {
        elapsed,
        output: arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string(),
        bytes_scanned: metric(&metrics, "bytes_scanned"),
        row_groups_pruned: metric(&metrics, "row_groups_pruned_statistics"),
        page_rows_pruned: metric(&metrics, "page_index_rows_pruned"),
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let file = generate(&args);
    let mid = args.rows as i64 / 2;
    let high = args.rows as i64 - args.rows as i64 / 100;

    let queries = [
        (
            "variant: projection",
            "SELECT sum(variant_like_get(v, 'k1')) FROM t".to_string(),
        ),
        (
            "variant: selective filter",
            format!(
                "SELECT count(*), sum(variant_like_get(v, 'k1')) FROM t \
                 WHERE variant_like_get(v, 'k0') BETWEEN {mid} AND {}",
                mid + 1_000
            ),
        ),
        (
            "variant: top 1% filter",
            format!("SELECT count(*) FROM t WHERE variant_like_get(v, 'k0') > {high}"),
        ),
        (
            "struct: projection",
            "SELECT sum(struct_get(s, 'a1')) FROM t".to_string(),
        ),
        (
            "struct: selective filter",
            format!(
                "SELECT count(*), sum(struct_get(s, 'a1')) FROM t \
                 WHERE struct_get(s, 'a0') BETWEEN {mid} AND {}",
                mid + 1_000
            ),
        ),
        (
            "get_field: selective filter",
            format!(
                "SELECT count(*), sum(s['a1']) FROM t \
                 WHERE s['a0'] BETWEEN {mid} AND {}",
                mid + 1_000
            ),
        ),
    ];

    let pushdown_modes = if args.pushdown_filters {
        vec![false, true]
    } else {
        vec![false]
    };
    println!(
        "| query | pushdown_filters | mode | median ms | bytes scanned | row groups pruned | page rows pruned |"
    );
    println!("|---|---|---|---:|---:|---:|---:|");
    for pushdown_filters in pushdown_modes {
        for (name, sql) in &queries {
            // get_field always has the capability; one mode is enough.
            let modes: &[Mode] = if name.starts_with("get_field") {
                &[Mode::ReadsAndStats]
            } else {
                &[Mode::Off, Mode::Reads, Mode::ReadsAndStats]
            };
            let mut expected = None;
            for &mode in modes {
                run(&file, sql, mode, pushdown_filters).await; // warm-up
                let mut runs = vec![];
                for _ in 0..args.iterations {
                    runs.push(run(&file, sql, mode, pushdown_filters).await);
                }
                runs.sort_by_key(|run| run.elapsed);
                let median = &runs[runs.len() / 2];
                match &expected {
                    None => expected = Some(median.output.clone()),
                    Some(expected) => assert_eq!(
                        &median.output,
                        expected,
                        "{name}: results differ for mode {}",
                        mode.name()
                    ),
                }
                println!(
                    "| {name} | {pushdown_filters} | {} | {:.1} | {} | {} | {} |",
                    mode.name(),
                    median.elapsed.as_secs_f64() * 1000.0,
                    median.bytes_scanned,
                    median.row_groups_pruned,
                    median.page_rows_pruned,
                );
            }
        }
    }
}
