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

//! See `main.rs` for how to run it.
//!
//! This example shows how an extension can return window metadata from an
//! aggregate by passing the relevant input columns directly to the aggregate.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Array, StructArray, TimestampNanosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::assert_batches_eq;
use datafusion::common::{cast::as_primitive_array, exec_err};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{AccumulatorFactoryFunction, Volatility, create_udaf};
use datafusion::physical_plan::Accumulator;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;

pub async fn struct_returning_udaf() -> Result<()> {
    let ctx = create_context()?;

    register_augmented_avg(&ctx);

    // The `augmented_avg` aggregate returns both the average and metadata about
    // the time window from which the average was computed.
    let sql = "
        SELECT
            augmented_avg(time, value)['window_start'] AS window_start,
            augmented_avg(time, value)['window_end'] AS window_end,
            augmented_avg(time, value)['window_duration'] AS window_duration,
            augmented_avg(time, value)['avg_value'] AS avg_value
        FROM t
        GROUP BY date_bin(INTERVAL '5 microseconds', time)
        ORDER BY window_start
    ";

    let results = ctx.sql(sql).await?.collect().await?;
    let expected = [
        "+----------------------------+----------------------------+-----------------+-----------+",
        "| window_start               | window_end                 | window_duration | avg_value |",
        "+----------------------------+----------------------------+-----------------+-----------+",
        "| 1970-01-01T00:00:00.000001 | 1970-01-01T00:00:00.000002 | 1000            | 15.0      |",
        "| 1970-01-01T00:00:00.000005 | 1970-01-01T00:00:00.000009 | 4000            | 3.0       |",
        "+----------------------------+----------------------------+-----------------+-----------+",
    ];
    assert_batches_eq!(expected, &results);

    println!("Struct-returning aggregate produced window metadata:");
    ctx.sql(sql).await?.show().await?;

    Ok(())
}

fn create_context() -> Result<SessionContext> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![
                1000, 2000, 5000, 7000, 9000,
            ])) as ArrayRef,
            Arc::new(Float64Array::from(vec![10.0, 20.0, 1.0, 3.0, 5.0])),
        ],
    )?;

    let ctx = SessionContext::new();
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(provider))?;
    Ok(ctx)
}

fn register_augmented_avg(ctx: &SessionContext) {
    let accumulator: AccumulatorFactoryFunction =
        Arc::new(|_| Ok(Box::new(AugmentedAvg::new())));

    let augmented_avg = create_udaf(
        "augmented_avg",
        vec![
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Float64,
        ],
        Arc::new(AugmentedAvg::output_datatype()),
        Volatility::Immutable,
        accumulator,
        Arc::new(AugmentedAvg::state_datatypes()),
    );

    ctx.register_udaf(augmented_avg);
}

#[derive(Debug, Clone)]
struct AugmentedAvg {
    window_start: Option<i64>,
    window_end: Option<i64>,
    sum: f64,
    count: u64,
}

impl AugmentedAvg {
    fn new() -> Self {
        Self {
            window_start: None,
            window_end: None,
            sum: 0.0,
            count: 0,
        }
    }

    fn fields() -> Fields {
        vec![
            Field::new(
                "window_start",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "window_end",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("window_duration", DataType::Int64, true),
            Field::new("avg_value", DataType::Float64, true),
        ]
        .into()
    }

    fn output_datatype() -> DataType {
        DataType::Struct(Self::fields())
    }

    fn state_datatypes() -> Vec<DataType> {
        vec![
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Float64,
            DataType::UInt64,
        ]
    }

    fn update_one(&mut self, time: i64, value: f64) {
        self.window_start = Some(self.window_start.map_or(time, |start| start.min(time)));
        self.window_end = Some(self.window_end.map_or(time, |end| end.max(time)));
        self.sum += value;
        self.count += 1;
    }
}

impl Accumulator for AugmentedAvg {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // DataFusion can merge partial aggregate results across execution
        // stages, so all values needed to reconstruct the final struct are
        // included in the state.
        Ok(vec![
            ScalarValue::TimestampNanosecond(self.window_start, None),
            ScalarValue::TimestampNanosecond(self.window_end, None),
            ScalarValue::Float64(Some(self.sum)),
            ScalarValue::UInt64(Some(self.count)),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [times, values] = values else {
            return exec_err!("augmented_avg expects time and value arrays");
        };
        let times =
            as_primitive_array::<arrow::datatypes::TimestampNanosecondType>(times)?;
        let values = as_primitive_array::<arrow::datatypes::Float64Type>(values)?;

        // Track the window bounds and aggregate values directly from the input
        // rows assigned to each group by `date_bin`.
        for (time, value) in times.iter().zip(values.iter()) {
            if let (Some(time), Some(value)) = (time, value) {
                self.update_one(time, value);
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [starts, ends, sums, counts] = states else {
            return exec_err!("augmented_avg expects four state arrays");
        };
        let starts =
            as_primitive_array::<arrow::datatypes::TimestampNanosecondType>(starts)?;
        let ends = as_primitive_array::<arrow::datatypes::TimestampNanosecondType>(ends)?;
        let sums = as_primitive_array::<arrow::datatypes::Float64Type>(sums)?;
        let counts = counts
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("Expected UInt64Array".to_string())
            })?;

        // Combine partial states by preserving the earliest start, latest end,
        // and additive average components.
        for (((start, end), sum), count) in starts
            .iter()
            .zip(ends.iter())
            .zip(sums.iter())
            .zip(counts.iter())
        {
            let Some(count) = count else {
                continue;
            };
            if count == 0 {
                continue;
            }
            if let (Some(start), Some(end), Some(sum)) = (start, end, sum) {
                self.window_start = Some(
                    self.window_start
                        .map_or(start, |current| current.min(start)),
                );
                self.window_end =
                    Some(self.window_end.map_or(end, |current| current.max(end)));
                self.sum += sum;
                self.count += count;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let duration = self
            .window_start
            .zip(self.window_end)
            .map(|(start, end)| end - start);
        let avg = (self.count > 0).then_some(self.sum / self.count as f64);

        // Return one Struct scalar whose fields can be projected from SQL with
        // expressions like `augmented_avg(time, value)['window_start']`.
        let struct_array = StructArray::try_new(
            AugmentedAvg::fields(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![self.window_start]))
                    as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(vec![self.window_end]))
                    as ArrayRef,
                Arc::new(arrow::array::Int64Array::from(vec![duration])) as ArrayRef,
                Arc::new(Float64Array::from(vec![avg])) as ArrayRef,
            ],
            None,
        )?;

        Ok(ScalarValue::Struct(Arc::new(struct_array)))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}
