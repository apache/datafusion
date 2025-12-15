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

//! Micro-benchmark for `COUNT(DISTINCT)` and `SUM(DISTINCT)` on primitive types.

use crate::util::{BenchmarkRun, CommonOpt};
use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_common::instant::Instant;
use datafusion_common::{Result, ScalarValue};
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;
use structopt::StructOpt;

#[derive(Debug, Clone, Copy)]
enum DistinctType {
    Int64,
    Float64,
}

impl Display for DistinctType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DistinctType::Int64 => write!(f, "int64"),
            DistinctType::Float64 => write!(f, "float64"),
        }
    }
}

impl FromStr for DistinctType {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "int" | "int64" => Ok(DistinctType::Int64),
            "float" | "float64" => Ok(DistinctType::Float64),
            other => Err(format!("unsupported type: {other}")),
        }
    }
}

#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct RunOpt {
    /// Common options
    #[structopt(flatten)]
    common: CommonOpt,

    /// If present, write results json here
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<std::path::PathBuf>,

    /// Total rows in the synthetic batch
    #[structopt(long = "rows", default_value = "1000000")]
    rows: usize,

    /// Number of distinct values in the column
    #[structopt(long = "cardinality", default_value = "100000")]
    cardinality: usize,

    /// Column type: int64 or float64
    #[structopt(long = "type", default_value = "int64")]
    data_type: DistinctType,
}

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        let config = self.common.config()?;
        let rt_builder = self.common.runtime_env_builder()?;
        let ctx = SessionContext::new_with_config_rt(config, rt_builder.build_arc()?);

        let (schema, batch) = match self.data_type {
            DistinctType::Int64 => {
                let values = (0..self.rows)
                    .map(|i| (i % self.cardinality) as i64)
                    .collect::<Vec<_>>();
                let array = Arc::new(Int64Array::from(values)) as _;
                let schema =
                    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
                (schema.clone(), RecordBatch::try_new(schema, vec![array])?)
            }
            DistinctType::Float64 => {
                let values = (0..self.rows)
                    .map(|i| (i % self.cardinality) as f64)
                    .collect::<Vec<_>>();
                let array = Arc::new(Float64Array::from(values)) as _;
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "v",
                    DataType::Float64,
                    false,
                )]));
                (schema.clone(), RecordBatch::try_new(schema, vec![array])?)
            }
        };

        // Single-partition memtable is enough for a micro-benchmark.
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("t", Arc::new(table))?;

        let mut run = BenchmarkRun::new();

        self.bench_query(
            &mut run,
            "count_distinct",
            "SELECT COUNT(DISTINCT v) FROM t",
            &ctx,
        )
        .await?;

        self.bench_query(
            &mut run,
            "sum_distinct",
            "SELECT SUM(DISTINCT v) FROM t",
            &ctx,
        )
        .await?;

        run.maybe_write_json(self.output_path.as_ref())?;
        Ok(())
    }

    async fn bench_query(
        &self,
        run: &mut BenchmarkRun,
        label: &str,
        sql: &str,
        ctx: &SessionContext,
    ) -> Result<()> {
        run.start_new_case(&format!("{label}-{}", self.data_type));

        for i in 0..self.common.iterations {
            let start = Instant::now();
            let rows = execute_scalar_query(sql, ctx).await?;
            let elapsed = start.elapsed();
            println!(
                "{label} ({}) iteration {i} returned {:?} in {:?}",
                self.data_type, rows, elapsed
            );
            run.write_iter(elapsed, 1);
        }

        Ok(())
    }
}

async fn execute_scalar_query(sql: &str, ctx: &SessionContext) -> Result<ScalarValue> {
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;
    let batch = batches
        .first()
        .cloned()
        .unwrap_or_else(|| RecordBatch::new_empty(Arc::new(Schema::empty())));

    if batch.num_rows() == 0 {
        return Ok(ScalarValue::Null);
    }

    ScalarValue::try_from_array(batch.column(0), 0)
}
