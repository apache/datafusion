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
use arrow_array::Int32Array;
use arrow_schema::Schema;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use datafusion::datasource::MemTable;
use datafusion::{
    arrow::{
        array::{ArrayRef, Float64Array, TimestampNanosecondArray},
        datatypes::{DataType, Field, Float64Type, TimeUnit, TimestampNanosecondType},
        record_batch::RecordBatch,
    },
    assert_batches_eq,
    error::Result,
    logical_expr::{
        AccumulatorFactoryFunction, AggregateUDF, Signature, TypeSignature, Volatility,
    },
    physical_plan::Accumulator,
    prelude::SessionContext,
    scalar::ScalarValue,
};
use datafusion_common::{
    assert_contains, cast::as_primitive_array, exec_err, DataFusionError,
};
use datafusion_expr::{create_udaf, SimpleAggregateUDF};
use datafusion_physical_expr::expressions::AvgAccumulator;

/// Test to show the contents of the setup
#[tokio::test]
async fn test_setup() {
    let TestContext { ctx, test_state: _ } = TestContext::new();
    let sql = "SELECT * from t order by time";
    let expected = [
        "+-------+----------------------------+",
        "| value | time                       |",
        "+-------+----------------------------+",
        "| 2.0   | 1970-01-01T00:00:00.000002 |",
        "| 3.0   | 1970-01-01T00:00:00.000003 |",
        "| 1.0   | 1970-01-01T00:00:00.000004 |",
        "| 5.0   | 1970-01-01T00:00:00.000005 |",
        "| 5.0   | 1970-01-01T00:00:00.000005 |",
        "+-------+----------------------------+",
    ];
    assert_batches_eq!(expected, &execute(&ctx, sql).await.unwrap());
}

/// Basic user defined aggregate
#[tokio::test]
async fn test_udaf() {
    let TestContext { ctx, test_state } = TestContext::new();
    assert!(!test_state.update_batch());
    let sql = "SELECT time_sum(time) from t";
    let expected = [
        "+----------------------------+",
        "| time_sum(t.time)           |",
        "+----------------------------+",
        "| 1970-01-01T00:00:00.000019 |",
        "+----------------------------+",
    ];
    assert_batches_eq!(expected, &execute(&ctx, sql).await.unwrap());
    // normal aggregates call update_batch
    assert!(test_state.update_batch());
    assert!(!test_state.retract_batch());
}

/// User defined aggregate used as a window function
#[tokio::test]
async fn test_udaf_as_window() {
    let TestContext { ctx, test_state } = TestContext::new();
    let sql = "SELECT time_sum(time) OVER() as time_sum from t";
    let expected = [
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
    assert_batches_eq!(expected, &execute(&ctx, sql).await.unwrap());
    // aggregate over the entire window function call update_batch
    assert!(test_state.update_batch());
    assert!(!test_state.retract_batch());
}

/// User defined aggregate used as a window function with a window frame
#[tokio::test]
async fn test_udaf_as_window_with_frame() {
    let TestContext { ctx, test_state } = TestContext::new();
    let sql = "SELECT time_sum(time) OVER(ORDER BY time ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as time_sum from t";
    let expected = [
        "+----------------------------+",
        "| time_sum                   |",
        "+----------------------------+",
        "| 1970-01-01T00:00:00.000005 |",
        "| 1970-01-01T00:00:00.000009 |",
        "| 1970-01-01T00:00:00.000012 |",
        "| 1970-01-01T00:00:00.000014 |",
        "| 1970-01-01T00:00:00.000010 |",
        "+----------------------------+",
    ];
    assert_batches_eq!(expected, &execute(&ctx, sql).await.unwrap());
    // user defined aggregates with window frame should be calling retract batch
    assert!(test_state.update_batch());
    assert!(test_state.retract_batch());
}

/// Ensure that User defined aggregate used as a window function with a window
/// frame, but that does not implement retract_batch, returns an error
#[tokio::test]
async fn test_udaf_as_window_with_frame_without_retract_batch() {
    let test_state = Arc::new(TestState::new().with_error_on_retract_batch());

    let TestContext { ctx, test_state: _ } = TestContext::new_with_test_state(test_state);
    let sql = "SELECT time_sum(time) OVER(ORDER BY time ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as time_sum from t";
    // Note if this query ever does start working
    let err = execute(&ctx, sql).await.unwrap_err();
    assert_contains!(err.to_string(), "This feature is not implemented: Aggregate can not be used as a sliding accumulator because `retract_batch` is not implemented: AggregateUDF { name: \"time_sum\"");
}

/// Basic query for with a udaf returning a structure
#[tokio::test]
async fn test_udaf_returning_struct() {
    let TestContext { ctx, test_state: _ } = TestContext::new();
    let sql = "SELECT first(value, time) from t";
    let expected = [
        "+------------------------------------------------+",
        "| first(t.value,t.time)                          |",
        "+------------------------------------------------+",
        "| {value: 2.0, time: 1970-01-01T00:00:00.000002} |",
        "+------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &execute(&ctx, sql).await.unwrap());
}

/// Demonstrate extracting the fields from a structure using a subquery
#[tokio::test]
async fn test_udaf_returning_struct_subquery() {
    let TestContext { ctx, test_state: _ } = TestContext::new();
    let sql = "select sq.first['value'], sq.first['time'] from (SELECT first(value, time) as first from t) as sq";
    let expected = [
        "+-----------------+----------------------------+",
        "| sq.first[value] | sq.first[time]             |",
        "+-----------------+----------------------------+",
        "| 2.0             | 1970-01-01T00:00:00.000002 |",
        "+-----------------+----------------------------+",
    ];
    assert_batches_eq!(expected, &execute(&ctx, sql).await.unwrap());
}

#[tokio::test]
async fn test_udaf_shadows_builtin_fn() {
    let TestContext {
        mut ctx,
        test_state,
    } = TestContext::new();
    let sql = "SELECT sum(arrow_cast(time, 'Int64')) from t";

    // compute with builtin `sum` aggregator
    let expected = [
        "+-------------+",
        "| SUM(t.time) |",
        "+-------------+",
        "| 19000       |",
        "+-------------+",
    ];
    assert_batches_eq!(expected, &execute(&ctx, sql).await.unwrap());

    // Register `TimeSum` with name `sum`. This will shadow the builtin one
    let sql = "SELECT sum(time) from t";
    TimeSum::register(&mut ctx, test_state.clone(), "sum");
    let expected = [
        "+----------------------------+",
        "| sum(t.time)                |",
        "+----------------------------+",
        "| 1970-01-01T00:00:00.000019 |",
        "+----------------------------+",
    ];
    assert_batches_eq!(expected, &execute(&ctx, sql).await.unwrap());
}

async fn execute(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}

/// tests the creation, registration and usage of a UDAF
#[tokio::test]
async fn simple_udaf() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch1 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )?;
    let batch2 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(vec![4, 5]))],
    )?;

    let ctx = SessionContext::new();

    let provider = MemTable::try_new(Arc::new(schema), vec![vec![batch1], vec![batch2]])?;
    ctx.register_table("t", Arc::new(provider))?;

    // define a udaf, using a DataFusion's accumulator
    let my_avg = create_udaf(
        "my_avg",
        vec![DataType::Float64],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::<AvgAccumulator>::default())),
        Arc::new(vec![DataType::UInt64, DataType::Float64]),
    );

    ctx.register_udaf(my_avg);

    let result = ctx.sql("SELECT MY_AVG(a) FROM t").await?.collect().await?;

    let expected = [
        "+-------------+",
        "| my_avg(t.a) |",
        "+-------------+",
        "| 3.0         |",
        "+-------------+",
    ];
    assert_batches_eq!(expected, &result);

    Ok(())
}

#[tokio::test]
async fn case_sensitive_identifiers_user_defined_aggregates() -> Result<()> {
    let ctx = SessionContext::new();
    let arr = Int32Array::from(vec![1]);
    let batch = RecordBatch::try_from_iter(vec![("i", Arc::new(arr) as _)])?;
    ctx.register_batch("t", batch).unwrap();

    // Note capitalization
    let my_avg = create_udaf(
        "MY_AVG",
        vec![DataType::Float64],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::<AvgAccumulator>::default())),
        Arc::new(vec![DataType::UInt64, DataType::Float64]),
    );

    ctx.register_udaf(my_avg);

    // doesn't work as it was registered as non lowercase
    let err = ctx.sql("SELECT MY_AVG(i) FROM t").await.unwrap_err();
    assert!(err
        .to_string()
        .contains("Error during planning: Invalid function \'my_avg\'"));

    // Can call it if you put quotes
    let result = ctx
        .sql("SELECT \"MY_AVG\"(i) FROM t")
        .await?
        .collect()
        .await?;

    let expected = [
        "+-------------+",
        "| MY_AVG(t.i) |",
        "+-------------+",
        "| 1.0         |",
        "+-------------+",
    ];
    assert_batches_eq!(expected, &result);

    Ok(())
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
    test_state: Arc<TestState>,
}

impl TestContext {
    fn new() -> Self {
        let test_state = Arc::new(TestState::new());
        Self::new_with_test_state(test_state)
    }

    fn new_with_test_state(test_state: Arc<TestState>) -> Self {
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
        TimeSum::register(&mut ctx, Arc::clone(&test_state), "time_sum");

        Self { ctx, test_state }
    }
}

#[derive(Debug, Default)]
struct TestState {
    /// was update_batch called?
    update_batch: AtomicBool,
    /// was retract_batch called?
    retract_batch: AtomicBool,
    /// should the udaf throw an error if retract batch is called? Can
    /// only be configured at construction time.
    error_on_retract_batch: bool,
}

impl TestState {
    fn new() -> Self {
        Default::default()
    }

    /// Has `update_batch` been called?
    fn update_batch(&self) -> bool {
        self.update_batch.load(Ordering::SeqCst)
    }

    /// Set the `update_batch` flag
    fn set_update_batch(&self) {
        self.update_batch.store(true, Ordering::SeqCst)
    }

    /// Has `retract_batch` been called?
    fn retract_batch(&self) -> bool {
        self.retract_batch.load(Ordering::SeqCst)
    }

    /// set the `retract_batch` flag
    fn set_retract_batch(&self) {
        self.retract_batch.store(true, Ordering::SeqCst)
    }

    /// Is this state configured to return an error on retract batch?
    fn error_on_retract_batch(&self) -> bool {
        self.error_on_retract_batch
    }

    /// Configure the test to return error on retract batch
    fn with_error_on_retract_batch(mut self) -> Self {
        self.error_on_retract_batch = true;
        self
    }
}

/// Models a user defined aggregate function that computes the a sum
/// of timestamps (not a quantity that has much real world meaning)
#[derive(Debug)]
struct TimeSum {
    sum: i64,
    test_state: Arc<TestState>,
}

impl TimeSum {
    fn new(test_state: Arc<TestState>) -> Self {
        Self { sum: 0, test_state }
    }

    fn register(ctx: &mut SessionContext, test_state: Arc<TestState>, name: &str) {
        let timestamp_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let input_type = vec![timestamp_type.clone()];

        // Returns the same type as its input
        let return_type = timestamp_type.clone();

        let state_type = vec![timestamp_type.clone()];

        let volatility = Volatility::Immutable;

        let captured_state = Arc::clone(&test_state);
        let accumulator: AccumulatorFactoryFunction =
            Arc::new(move |_| Ok(Box::new(Self::new(Arc::clone(&captured_state)))));

        let time_sum = AggregateUDF::from(SimpleAggregateUDF::new(
            name,
            input_type,
            return_type,
            volatility,
            accumulator,
            state_type,
        ));

        // register the selector as "time_sum"
        ctx.register_udaf(time_sum)
    }
}

impl Accumulator for TimeSum {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.test_state.set_update_batch();
        assert_eq!(values.len(), 1);
        let arr = &values[0];
        let arr = arr.as_primitive::<TimestampNanosecondType>();

        for v in arr.values().iter() {
            println!("Adding {v}");
            self.sum += v;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // merge and update is the same for time sum
        self.update_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        println!("Evaluating to {}", self.sum);
        Ok(ScalarValue::TimestampNanosecond(Some(self.sum), None))
    }

    fn size(&self) -> usize {
        // accurate size estimates are not important for this example
        42
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if self.test_state.error_on_retract_batch() {
            return exec_err!("Error in Retract Batch");
        }

        self.test_state.set_retract_batch();
        assert_eq!(values.len(), 1);
        let arr = &values[0];
        let arr = arr.as_primitive::<TimestampNanosecondType>();

        for v in arr.values().iter() {
            println!("Retracting {v}");
            self.sum -= v;
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        !self.test_state.error_on_retract_batch()
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
        let return_type = Self::output_datatype();
        let state_type = Self::state_datatypes();

        // Possible input signatures
        let signatures = vec![TypeSignature::Exact(Self::input_datatypes())];

        let accumulator: AccumulatorFactoryFunction =
            Arc::new(|_| Ok(Box::new(Self::new())));

        let volatility = Volatility::Immutable;

        let name = "first";

        let first = AggregateUDF::from(SimpleAggregateUDF::new_with_signature(
            name.to_string(),
            Signature::one_of(signatures, volatility),
            return_type,
            accumulator,
            state_type,
        ));

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
