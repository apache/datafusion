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

use datafusion::{arrow::datatypes::DataType, logical_expr::Volatility};
use std::any::Any;

use arrow::{
    array::{ArrayRef, AsArray, Float64Array},
    datatypes::Float64Type,
};
use arrow_schema::Field;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::ScalarValue;
use datafusion_expr::function::WindowUDFFieldArgs;
use datafusion_expr::{
    PartitionEvaluator, Signature, WindowFrame, WindowUDF, WindowUDFImpl,
};
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;

/// This example shows how to use the full WindowUDFImpl API to implement a user
/// defined window function. As in the `simple_udwf.rs` example, this struct implements
/// a function `partition_evaluator` that returns the `MyPartitionEvaluator` instance.
///
/// To do so, we must implement the `WindowUDFImpl` trait.
#[derive(Debug, Clone)]
struct SmoothItUdf {
    signature: Signature,
}

impl SmoothItUdf {
    /// Create a new instance of the SmoothItUdf struct
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                // this function will always take one arguments of type f64
                vec![DataType::Float64],
                // this function is deterministic and will always return the same
                // result for the same input
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for SmoothItUdf {
    /// We implement as_any so that we can downcast the WindowUDFImpl trait object
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the name of this function
    fn name(&self) -> &str {
        "smooth_it"
    }

    /// Return the "signature" of this function -- namely that types of arguments it will take
    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Create a `PartitionEvaluator` to evaluate this function on a new
    /// partition.
    fn partition_evaluator(
        &self,
        _partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(MyPartitionEvaluator::new()))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<Field> {
        Ok(Field::new(field_args.name(), DataType::Float64, true))
    }
}

/// This implements the lowest level evaluation for a window function
///
/// It handles calculating the value of the window function for each
/// distinct values of `PARTITION BY` (each car type in our example)
#[derive(Clone, Debug)]
struct MyPartitionEvaluator {}

impl MyPartitionEvaluator {
    fn new() -> Self {
        Self {}
    }
}

/// Different evaluation methods are called depending on the various
/// settings of WindowUDF. This example uses the simplest and most
/// general, `evaluate`. See `PartitionEvaluator` for the other more
/// advanced uses.
impl PartitionEvaluator for MyPartitionEvaluator {
    /// Tell DataFusion the window function varies based on the value
    /// of the window frame.
    fn uses_window_frame(&self) -> bool {
        true
    }

    /// This function is called once per input row.
    ///
    /// `range`specifies which indexes of `values` should be
    /// considered for the calculation.
    ///
    /// Note this is the SLOWEST, but simplest, way to evaluate a
    /// window function. It is much faster to implement
    /// evaluate_all or evaluate_all_with_rank, if possible
    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &std::ops::Range<usize>,
    ) -> Result<ScalarValue> {
        // Again, the input argument is an array of floating
        // point numbers to calculate a moving average
        let arr: &Float64Array = values[0].as_ref().as_primitive::<Float64Type>();

        let range_len = range.end - range.start;

        // our smoothing function will average all the values in the
        let output = if range_len > 0 {
            let sum: f64 = arr.values().iter().skip(range.start).take(range_len).sum();
            Some(sum / range_len as f64)
        } else {
            None
        };

        Ok(ScalarValue::Float64(output))
    }
}

// create local execution context with `cars.csv` registered as a table named `cars`
async fn create_context() -> Result<SessionContext> {
    // declare a new context. In spark API, this corresponds to a new spark SQL session
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    println!("pwd: {}", std::env::current_dir().unwrap().display());
    let csv_path = "../../datafusion/core/tests/data/cars.csv".to_string();
    let read_options = CsvReadOptions::default().has_header(true);

    ctx.register_csv("cars", &csv_path, read_options).await?;
    Ok(ctx)
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = create_context().await?;
    let smooth_it = WindowUDF::from(SmoothItUdf::new());
    ctx.register_udwf(smooth_it.clone());

    // Use SQL to run the new window function
    let df = ctx.sql("SELECT * from cars").await?;
    // print the results
    df.show().await?;

    // Use SQL to run the new window function:
    //
    // `PARTITION BY car`:each distinct value of car (red, and green)
    // should be treated as a separate partition (and will result in
    // creating a new `PartitionEvaluator`)
    //
    // `ORDER BY time`: within each partition ('green' or 'red') the
    // rows will be ordered by the value in the `time` column
    //
    // `evaluate_inside_range` is invoked with a window defined by the
    // SQL. In this case:
    //
    // The first invocation will be passed row 0, the first row in the
    // partition.
    //
    // The second invocation will be passed rows 0 and 1, the first
    // two rows in the partition.
    //
    // etc.
    let df = ctx
        .sql(
            "SELECT \
               car, \
               speed, \
               smooth_it(speed) OVER (PARTITION BY car ORDER BY time) AS smooth_speed,\
               time \
               from cars \
             ORDER BY \
               car",
        )
        .await?;
    // print the results
    df.show().await?;

    // this time, call the new widow function with an explicit
    // window so evaluate will be invoked with each window.
    //
    // `ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING`: each invocation
    // sees at most 3 rows: the row before, the current row, and the 1
    // row afterward.
    let df = ctx.sql(
        "SELECT \
           car, \
           speed, \
           smooth_it(speed) OVER (PARTITION BY car ORDER BY time ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS smooth_speed,\
           time \
           from cars \
         ORDER BY \
           car",
    ).await?;
    // print the results
    df.show().await?;

    // Now, run the function using the DataFrame API:
    let window_expr = smooth_it
        .call(vec![col("speed")]) // smooth_it(speed)
        .partition_by(vec![col("car")]) // PARTITION BY car
        .order_by(vec![col("time").sort(true, true)]) // ORDER BY time ASC
        .window_frame(WindowFrame::new(None))
        .build()?;
    let df = ctx.table("cars").await?.window(vec![window_expr])?;

    // print the results
    df.show().await?;

    Ok(())
}
