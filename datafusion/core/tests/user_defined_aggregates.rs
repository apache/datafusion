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

use arrow::{array::AsArray, datatypes::Fields};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use datafusion::{
    arrow::{
        array::{ArrayRef, Float64Array, TimestampNanosecondArray},
        datatypes::{DataType, Field, Float64Type, TimeUnit, TimestampNanosecondType},
        record_batch::RecordBatch,
    },
    assert_batches_eq,
    error::Result,
    logical_expr::{
        AccumulatorFunctionImplementation, AggregateUDF, ReturnTypeFunction, Signature,
        StateTypeFunction, TypeSignature, Volatility,
    },
    physical_plan::Accumulator,
    prelude::SessionContext,
    scalar::ScalarValue,
};
use datafusion_common::cast::as_primitive_array;

/// Basic user defined aggregate
#[tokio::test]
async fn test_udaf() {
    let TestContext { ctx, counters } = TestContext::new();
    assert!(!counters.update_batch());
    let sql = "SELECT time_sum(time) from t";
    let expected = vec![
        "+----------------------------+",
        "| time_sum(t.time)           |",
        "+----------------------------+",
        "| 1970-01-01T00:00:00.000019 |",
        "+----------------------------+",
    ];
    assert_batches_eq!(expected, &execute(&ctx, sql).await);
    // normal aggregates call update_batch
    assert!(counters.update_batch());
    assert!(!counters.retract_batch());
}

/// User defined aggregate used as a window function
#[tokio::test]
async fn test_udaf_as_window() {
    let TestContext { ctx, counters } = TestContext::new();
    let sql = "SELECT time_sum(time) OVER() as time_sum from t";
    let expected = vec![
        "+----------------------------+",
        "| time_sum                   |",
        "+----------------------------+",
        "| 1970-01-01T00:00:00.000019 |",
        "| 1970-01-01T00:00:00.000019 |",
        "| 1970-01-01T00:00:00.000019 |",
        "| 1970-01-01T00:00:00.000019 |",
        "| 1970-01-01T00:00:00.000019 |",
        "+----------------------------+",
    ];
    assert_batches_eq!(expected, &execute(&ctx, sql).await);
    // aggregate over the entire window function call update_batch
    assert!(counters.update_batch());
    assert!(!counters.retract_batch());
}

/// User defined aggregate used as a window function with a window frame
#[tokio::test]
async fn test_udaf_as_window_with_frame() {
    let TestContext { ctx, counters } = TestContext::new();
    let sql = "SELECT time_sum(time) OVER(ORDER BY time ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as time_sum from t";
    let expected = vec![
        "+----------------------------+",
        "| time_sum                   |",
        "+----------------------------+",
        "| 1970-01-01T00:00:00.000005 |",
        "| 1970-01-01T00:00:00.000009 |",
        "| 1970-01-01T00:00:00.000014 |",
        "| 1970-01-01T00:00:00.000019 |",
        "| 1970-01-01T00:00:00.000019 |",
        "+----------------------------+",
    ];
    assert_batches_eq!(expected, &execute(&ctx, sql).await);
    // user defined aggregates with window frame should be calling retract batch
    // but doesn't yet: https://github.com/apache/arrow-datafusion/issues/6611
    assert!(counters.update_batch());
    assert!(!counters.retract_batch());
}

/// Basic query for with a udaf returning a structure
#[tokio::test]
async fn test_udaf_returning_struct() {
    let TestContext { ctx, counters: _ } = TestContext::new();
    let sql = "SELECT first(value, time) from t";
    let expected = vec![
        "+------------------------------------------------+",
        "| first(t.value,t.time)                          |",
        "+------------------------------------------------+",
        "| {value: 2.0, time: 1970-01-01T00:00:00.000002} |",
        "+------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &execute(&ctx, sql).await);
}

/// Demonstrate extracting the fields from a structure using a subquery
#[tokio::test]
async fn test_udaf_returning_struct_subquery() {
    let TestContext { ctx, counters: _ } = TestContext::new();
    let sql = "select sq.first['value'], sq.first['time'] from (SELECT first(value, time) as first from t) as sq";
    let expected = vec![
        "+-----------------+----------------------------+",
        "| sq.first[value] | sq.first[time]             |",
        "+-----------------+----------------------------+",
        "| 2.0             | 1970-01-01T00:00:00.000002 |",
        "+-----------------+----------------------------+",
    ];
    assert_batches_eq!(expected, &execute(&ctx, sql).await);
}

async fn execute(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

/// Returns an context with a table "t" and the "first" and "time_sum"
/// aggregate functions registered.
///
/// "t" contains this data:
///
/// ```text
/// value | time
///  3.0  | 1970-01-01T00:00:00.000003
///  2.0  | 1970-01-01T00:00:00.000002
///  1.0  | 1970-01-01T00:00:00.000004
///  5.0  | 1970-01-01T00:00:00.000005
///  5.0  | 1970-01-01T00:00:00.000005
/// ```
struct TestContext {
    ctx: SessionContext,
    counters: Arc<TestCounters>,
}

impl TestContext {
    fn new() -> Self {
        let counters = Arc::new(TestCounters::new());

        let value = Float64Array::from(vec![3.0, 2.0, 1.0, 5.0, 5.0]);
        let time = TimestampNanosecondArray::from(vec![3000, 2000, 4000, 5000, 5000]);

        let batch = RecordBatch::try_from_iter(vec![
            ("value", Arc::new(value) as _),
            ("time", Arc::new(time) as _),
        ])
        .unwrap();

        let mut ctx = SessionContext::new();

        ctx.register_batch("t", batch).unwrap();

        // Tell DataFusion about the "first" function
        FirstSelector::register(&mut ctx);
        // Tell DataFusion about the "time_sum" function
        TimeSum::register(&mut ctx, Arc::clone(&counters));

        Self { ctx, counters }
    }
}

#[derive(Debug, Default)]
struct TestCounters {
    /// was update_batch called?
    update_batch: AtomicBool,
    /// was retract_batch called?
    retract_batch: AtomicBool,
}

impl TestCounters {
    fn new() -> Self {
        Default::default()
    }

    /// Has `update_batch` been called?
    fn update_batch(&self) -> bool {
        self.update_batch.load(Ordering::SeqCst)
    }

    /// Has `retract_batch` been called?
    fn retract_batch(&self) -> bool {
        self.retract_batch.load(Ordering::SeqCst)
    }
}

/// Models a user defined aggregate function that computes the a sum
/// of timestamps (not a quantity that has much real world meaning)
#[derive(Debug)]
struct TimeSum {
    sum: i64,
    counters: Arc<TestCounters>,
}

impl TimeSum {
    fn new(counters: Arc<TestCounters>) -> Self {
        Self { sum: 0, counters }
    }

    fn register(ctx: &mut SessionContext, counters: Arc<TestCounters>) {
        let timestamp_type = DataType::Timestamp(TimeUnit::Nanosecond, None);

        // Returns the same type as its input
        let return_type = Arc::new(timestamp_type.clone());
        let return_type: ReturnTypeFunction =
            Arc::new(move |_| Ok(Arc::clone(&return_type)));

        let state_type = Arc::new(vec![timestamp_type.clone()]);
        let state_type: StateTypeFunction =
            Arc::new(move |_| Ok(Arc::clone(&state_type)));

        let volatility = Volatility::Immutable;

        let signature = Signature::exact(vec![timestamp_type], volatility);

        let accumulator: AccumulatorFunctionImplementation =
            Arc::new(move |_| Ok(Box::new(Self::new(Arc::clone(&counters)))));

        let name = "time_sum";

        let time_sum =
            AggregateUDF::new(name, &signature, &return_type, &accumulator, &state_type);

        // register the selector as "time_sum"
        ctx.register_udaf(time_sum)
    }
}

impl Accumulator for TimeSum {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.counters.update_batch.store(true, Ordering::SeqCst);
        assert_eq!(values.len(), 1);
        let arr = &values[0];
        let arr = arr.as_primitive::<TimestampNanosecondType>();

        for v in arr.values().iter() {
            self.sum += v;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // merge and update is the same for time sum
        self.update_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::TimestampNanosecond(Some(self.sum), None))
    }

    fn size(&self) -> usize {
        // accurate size estimates are not important for this example
        42
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.counters.retract_batch.store(true, Ordering::SeqCst);
        assert_eq!(values.len(), 1);
        let arr = &values[0];
        let arr = arr.as_primitive::<TimestampNanosecondType>();

        for v in arr.values().iter() {
            self.sum -= v;
        }
        Ok(())
    }
}

/// Models a specialized timeseries aggregate function
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

    fn register(ctx: &mut SessionContext) {
        let return_type = Arc::new(Self::output_datatype());
        let state_type = Arc::new(Self::state_datatypes());

        let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));
        let state_type: StateTypeFunction = Arc::new(move |_| Ok(state_type.clone()));

        // Possible input signatures
        let signatures = vec![TypeSignature::Exact(Self::input_datatypes())];

        let accumulator: AccumulatorFunctionImplementation =
            Arc::new(|_| Ok(Box::new(Self::new())));

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
        ctx.register_udaf(first)
    }

    /// Return the schema fields
    fn fields() -> Fields {
        vec![
            Field::new("value", DataType::Float64, true),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]
        .into()
    }

    fn output_datatype() -> DataType {
        DataType::Struct(Self::fields())
    }

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
        ScalarValue::Struct(Some(self.to_state()), Self::fields())
    }
}

impl Accumulator for FirstSelector {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let state = self.to_state().into_iter().collect::<Vec<_>>();

        Ok(state)
    }

    /// produce the output structure
    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.to_scalar())
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // cast argumets to the appropriate type (DataFusion will type
        // check these based on the declared allowed input types)
        let v = as_primitive_array::<Float64Type>(&values[0])?;
        let t = as_primitive_array::<TimestampNanosecondType>(&values[1])?;

        // Update the actual values
        for (value, time) in v.iter().zip(t.iter()) {
            if let (Some(time), Some(value)) = (time, value) {
                if time < self.time {
                    self.value = value;
                    self.time = time;
                }
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // same logic is needed as in update_batch
        self.update_batch(states)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}
