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

use std::{fs::File, io::Write, sync::Arc};

use arrow::{
    array::{ArrayRef, AsArray, Float64Array},
    datatypes::{DataType, Float64Type},
};
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::logical_expr::{PartitionEvaluator, Volatility, WindowFrame};
use datafusion::prelude::*;
use tempfile::tempdir;

// create local execution context with `cars.csv` registered as a table named `cars`
async fn create_context() -> Result<SessionContext> {
    // declare a new context. In spark API, this corresponds to a new spark SQL session
    let ctx = SessionContext::new();

    // content from file 'datafusion/core/tests/data/cars.csv'
    let csv_data = r#"car,speed,time
red,20.0,1996-04-12T12:05:03.000000000
red,20.3,1996-04-12T12:05:04.000000000
red,21.4,1996-04-12T12:05:05.000000000
red,21.5,1996-04-12T12:05:06.000000000
red,19.0,1996-04-12T12:05:07.000000000
red,18.0,1996-04-12T12:05:08.000000000
red,17.0,1996-04-12T12:05:09.000000000
red,7.0,1996-04-12T12:05:10.000000000
red,7.1,1996-04-12T12:05:11.000000000
red,7.2,1996-04-12T12:05:12.000000000
red,3.0,1996-04-12T12:05:13.000000000
red,1.0,1996-04-12T12:05:14.000000000
red,0.0,1996-04-12T12:05:15.000000000
green,10.0,1996-04-12T12:05:03.000000000
green,10.3,1996-04-12T12:05:04.000000000
green,10.4,1996-04-12T12:05:05.000000000
green,10.5,1996-04-12T12:05:06.000000000
green,11.0,1996-04-12T12:05:07.000000000
green,12.0,1996-04-12T12:05:08.000000000
green,14.0,1996-04-12T12:05:09.000000000
green,15.0,1996-04-12T12:05:10.000000000
green,15.1,1996-04-12T12:05:11.000000000
green,15.2,1996-04-12T12:05:12.000000000
green,8.0,1996-04-12T12:05:13.000000000
green,2.0,1996-04-12T12:05:14.000000000
"#;
    let dir = tempdir()?;
    let file_path = dir.path().join("cars.csv");
    {
        let mut file = File::create(&file_path)?;
        // write CSV data
        file.write_all(csv_data.as_bytes())?;
    } // scope closes the file
    let file_path = file_path.to_str().unwrap();

    ctx.register_csv("cars", file_path, CsvReadOptions::new())
        .await?;

    Ok(ctx)
}

/// In this example we will declare a user defined window function that computes a moving average and then run it using SQL
pub async fn simple_udwf() -> Result<()> {
    let ctx = create_context().await?;

    // here is where we define the UDWF. We also declare its signature:
    let smooth_it = create_udwf(
        "smooth_it",
        DataType::Float64,
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(make_partition_evaluator),
    );

    // register the window function with DataFusion so we can call it
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

/// Create a `PartitionEvaluator` to evaluate this function on a new
/// partition.
fn make_partition_evaluator() -> Result<Box<dyn PartitionEvaluator>> {
    Ok(Box::new(MyPartitionEvaluator::new()))
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
