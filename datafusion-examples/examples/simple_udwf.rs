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

use std::sync::Arc;

use arrow::{
    array::{AsArray, Float64Array, ArrayRef},
    datatypes::Float64Type,
};
use arrow_schema::DataType;
use datafusion::datasource::file_format::options::CsvReadOptions;

use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{
    partition_evaluator::PartitionEvaluator, Signature, Volatility, WindowUDF,
};

// create local execution context with `cars.csv` registered as a table named `cars`
async fn create_context() -> Result<SessionContext> {
    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    println!("pwd: {}", std::env::current_dir().unwrap().display());
    let csv_path = format!("datafusion/core/tests/data/cars.csv");
    let read_options = CsvReadOptions::default().has_header(true);

    ctx.register_csv("cars", &csv_path, read_options).await?;
    Ok(ctx)
}

/// In this example we will declare a user defined window function that computes a moving average and then run it using SQL
#[tokio::main]
async fn main() -> Result<()> {
    let ctx = create_context().await?;

    // register the window function with DataFusion so wecan call it
    ctx.register_udwf(smooth_it());

    // Use SQL to run the new window function
    let df = ctx.sql("SELECT * from cars").await?;
    // print the results
    df.show().await?;

    // Use SQL to run the new window function:
    //
    // `PARTITION BY car`:each distinct value of car (red, and green)
    // should be treated as a seprate partition (and will result in
    // creating a new `PartitionEvaluator`)
    //
    // `ORDER BY time`: within each partition ('green' or 'red') the
    // rows will be be orderd by the value in the `time` column
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
    let df = ctx.sql(
            "SELECT \
               car, \
               speed, \
               smooth_it(speed) OVER (PARTITION BY car ORDER BY time),\
               time \
               from cars \
             ORDER BY \
               car",
        )
        .await?;
    // print the results
    df.show().await?;

    // this time, call the new widow function with an explicit window
    //
    // `ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING`: each invocation
    // sees at most 3 rows: the row before, the current row, and the 1
    // row afterward.
    let df = ctx.sql(
        "SELECT \
           car, \
           speed, \
           smooth_it(speed) OVER (PARTITION BY car ORDER BY time ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),\
           time \
           from cars \
         ORDER BY \
           car",
    ).await?;
    // print the results
    df.show().await?;

    // todo show how to run dataframe API as well

    Ok(())
}
fn smooth_it() -> WindowUDF {
    WindowUDF {
        name: String::from("smooth_it"),
        // it will take 1 arguments -- the column to smooth
        signature: Signature::exact(vec![DataType::Int32], Volatility::Immutable),
        return_type: Arc::new(return_type),
        partition_evaluator: Arc::new(make_partition_evaluator),
        // specify that the user defined window function gets a window
        // frame (so that the user can use the window frame definition
        // (ROWS BETWEEN 2 PRECEDING AND 3 FOLLOWING)
        uses_window_frame: true,
        supports_bounded_execution: false,
    }
}

/// Compute the return type of the smooth_it window function given
/// arguments of `arg_types`.
fn return_type(arg_types: &[DataType]) -> Result<Arc<DataType>> {
    if arg_types.len() != 1 {
        return Err(DataFusionError::Plan(format!(
            "my_udwf expects 1 argument, got {}: {:?}",
            arg_types.len(),
            arg_types
        )));
    }
    Ok(Arc::new(arg_types[0].clone()))
}

/// Create a `PartitionEvalutor` to evaluate this function on a new
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

/// These different evaluation methods are called depending on the various settings of WindowUDF
impl PartitionEvaluator for MyPartitionEvaluator {
    fn get_range(&self, _idx: usize, _n_rows: usize) -> Result<std::ops::Range<usize>> {
        Err(DataFusionError::NotImplemented(
            "get_range is not implemented for this window function".to_string(),
        ))
    }

    /// This function is called once per input row.
    ///
    /// `range`
    /// specifies which indexes of `values` should be considered for
    /// the calculation.
    ///
    /// Note this is not the fastest way to evaluate a window
    /// function. It is much faster to implement evaluate_stateful or
    /// range less / rank based calculations if possible.
    fn evaluate_inside_range(
        &self,
        values: &[ArrayRef],
        range: &std::ops::Range<usize>,
    ) -> Result<ScalarValue> {
        //println!("evaluate_inside_range(). range: {range:#?}, values: {values:#?}");

        // Again, the input argument is an array of floating
        // point numbers to calculate a moving average
        let arr: &Float64Array = values[0].as_ref().as_primitive::<Float64Type>();

        let range_len = range.end - range.start;

        // our smoothing function will average all the values in the
        let output = if range_len > 0 {
            let sum: f64 =  arr
                .values()
                .iter()
                .skip(range.start)
                .take(range_len)
                .sum();
            Some(sum / range_len as f64)
        } else {
            None
        };

        Ok(ScalarValue::Float64(output))
    }
}
