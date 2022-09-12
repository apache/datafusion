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

//! This module contains end to end demonstrations of creating
//! user defined aggregate functions

use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{as_primitive_array, ArrayRef, Float64Array, TimestampNanosecondArray},
        datatypes::{DataType, Field, Float64Type, TimeUnit, TimestampNanosecondType},
        record_batch::RecordBatch,
    },
    assert_batches_eq,
    datasource::MemTable,
    error::Result,
    logical_expr::AggregateState,
    logical_expr::{
        AccumulatorFunctionImplementation, AggregateUDF, ReturnTypeFunction, Signature,
        StateTypeFunction, TypeSignature, Volatility,
    },
    physical_plan::Accumulator,
    prelude::SessionContext,
    scalar::ScalarValue,
};

#[tokio::test]
/// Basic query for with a udaf returning a structure
async fn test_udf_returning_struct() {
    let ctx = udaf_struct_context();
    let sql = "SELECT first(value, time) from t";
    let expected = vec![
        "+--------------------------------------------------+",
        "| first(t.value,t.time)                            |",
        "+--------------------------------------------------+",
        "| {\"value\": 2, \"time\": 1970-01-01 00:00:00.000002} |",
        "+--------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &execute(&ctx, sql).await);
}

#[tokio::test]
/// Demonstrate extracting the fields from the a structure using a subquery
async fn test_udf_returning_struct_sq() {
    let ctx = udaf_struct_context();
    let sql = "select sq.first['value'], sq.first['time'] from (SELECT first(value, time) as first from t) as sq";
    let expected = vec![
        "+-----------------+----------------------------+",
        "| sq.first[value] | sq.first[time]             |",
        "+-----------------+----------------------------+",
        "| 2               | 1970-01-01 00:00:00.000002 |",
        "+-----------------+----------------------------+",
    ];
    assert_batches_eq!(expected, &execute(&ctx, sql).await);
}

async fn execute(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

/// Returns an context with a table "t" and the "first" aggregate registered.
///
/// "t" contains this data:
///
/// ```text
/// value | time
///  3.0  | 1970-01-01 00:00:00.000003
///  2.0  | 1970-01-01 00:00:00.000002
///  1.0  | 1970-01-01 00:00:00.000004
/// ```
fn udaf_struct_context() -> SessionContext {
    let value: Float64Array = vec![3.0, 2.0, 1.0].into_iter().map(Some).collect();
    let time = TimestampNanosecondArray::from_vec(vec![3000, 2000, 4000], None);

    let batch = RecordBatch::try_from_iter(vec![
        ("value", Arc::new(value) as _),
        ("time", Arc::new(time) as _),
    ])
    .unwrap();

    let mut ctx = SessionContext::new();
    let t = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(t)).unwrap();

    // Tell datafusion about the "first" function
    register_aggregate(&mut ctx);

    ctx
}

fn register_aggregate(ctx: &mut SessionContext) {
    let return_type = Arc::new(FirstSelector::output_datatype());
    let state_type = Arc::new(FirstSelector::state_datatypes());

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));
    let state_type: StateTypeFunction = Arc::new(move |_| Ok(state_type.clone()));

    // Possible input signatures
    let signatures = vec![TypeSignature::Exact(FirstSelector::input_datatypes())];

    let accumulator: AccumulatorFunctionImplementation =
        Arc::new(|_| Ok(Box::new(FirstSelector::new())));

    let volatility = Volatility::Immutable;

    let name = "first";

    let first = AggregateUDF::new(
        name,
        &Signature::one_of(signatures, volatility),
        &return_type,
        &accumulator,
        &state_type,
    );

    // register the selector as "first"
    ctx.state
        .write()
        .aggregate_functions
        .insert(name.to_string(), Arc::new(first));
}

/// This structureg models a specialized timeseries aggregate function
/// called a "selector" in InfluxQL and Flux.
///
/// It returns the value and corresponding timestamp of the
/// input with the earliest timestamp as a structure.
#[derive(Debug, Clone)]
struct FirstSelector {
    value: f64,
    time: i64,
}

impl FirstSelector {
    /// Create a new empty selector
    fn new() -> Self {
        Self {
            value: 0.0,
            time: i64::MAX,
        }
    }

    /// Return the schema fields
    fn fields() -> Vec<Field> {
        vec![
            Field::new("value", DataType::Float64, true),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]
    }

    fn update(&mut self, val: f64, time: i64) {
        // remember the point with the earliest timestamp
        if time < self.time {
            self.value = val;
            self.time = time;
        }
    }

    // output data type
    fn output_datatype() -> DataType {
        DataType::Struct(Self::fields())
    }

    // input argument data types
    fn input_datatypes() -> Vec<DataType> {
        vec![
            DataType::Float64,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ]
    }

    // Internally, keep the data types as this type
    fn state_datatypes() -> Vec<DataType> {
        vec![
            DataType::Float64,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ]
    }

    /// Convert to a set of ScalarValues
    fn to_state(&self) -> Vec<ScalarValue> {
        vec![
            ScalarValue::Float64(Some(self.value)),
            ScalarValue::TimestampNanosecond(Some(self.time), None),
        ]
    }

    /// return this selector as a single scalar (struct) value
    fn to_scalar(&self) -> ScalarValue {
        ScalarValue::Struct(Some(self.to_state()), Box::new(Self::fields()))
    }
}

impl Accumulator for FirstSelector {
    fn state(&self) -> Result<Vec<AggregateState>> {
        let state = self
            .to_state()
            .into_iter()
            .map(AggregateState::Scalar)
            .collect::<Vec<_>>();

        Ok(state)
    }

    /// produce the output structure
    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.to_scalar())
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // cast argumets to the appropriate type (DataFusion will type
        // check these based on the declared allowed input types)
        let v = as_primitive_array::<Float64Type>(&values[0]);
        let t = as_primitive_array::<TimestampNanosecondType>(&values[1]);

        // Update the actual values
        for (value, time) in v.iter().zip(t.iter()) {
            if let (Some(time), Some(value)) = (time, value) {
                self.update(value, time)
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // same logic is needed as in update_batch
        self.update_batch(states)
    }
}
