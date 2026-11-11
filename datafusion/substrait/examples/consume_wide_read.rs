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

//! Manually measure consuming Substrait ReadRel plans for wide tables.

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::empty::EmptyTable;
use datafusion::prelude::SessionContext;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    println!("columns,round,iterations,ns_per_consume");

    for columns in [10, 100, 1_000, 10_000] {
        let fields = (0..columns)
            .map(|i| Field::new(format!("c{i}"), DataType::Int64, true))
            .collect::<Vec<_>>();
        let context = SessionContext::new();
        context.register_table(
            "wide",
            Arc::new(EmptyTable::new(Arc::new(Schema::new(fields)))),
        )?;
        let plan = runtime.block_on(async {
            let dataframe = context.sql("SELECT c0 FROM wide").await?;
            let logical_plan = dataframe.into_optimized_plan()?;
            Ok::<_, Box<dyn std::error::Error>>(to_substrait_plan(
                &logical_plan,
                &context.state(),
            )?)
        })?;
        let state = context.state();
        let consume = || {
            runtime
                .block_on(async { from_substrait_plan(&state, black_box(&plan)).await })
        };

        let output = consume()?;
        assert_eq!(output.schema().fields().len(), 1);
        assert_eq!(output.schema().field(0).name(), "c0");
        drop(output);

        let warmup = Instant::now();
        while warmup.elapsed() < Duration::from_millis(500) {
            black_box(consume()?);
        }

        for round in 1..=7 {
            let start = Instant::now();
            let mut iterations = 0_u64;
            while start.elapsed() < Duration::from_secs(1) {
                black_box(consume()?);
                iterations += 1;
            }
            let elapsed = start.elapsed();
            println!(
                "{columns},{round},{iterations},{:.1}",
                elapsed.as_nanos() as f64 / iterations as f64
            );
        }
    }
    Ok(())
}
