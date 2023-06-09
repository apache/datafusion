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
    array::{AsArray, Float64Array},
    datatypes::Float64Type,
};
use arrow_schema::DataType;
use datafusion::datasource::file_format::options::CsvReadOptions;

use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::DataFusionError;
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
    ctx.register_udwf(my_average());

    // Use SQL to run the new window function
    let df = ctx.sql("SELECT * from cars").await?;
    // print the results
    df.show().await?;

    // Use SQL to run the new window function
    // `PARTITION BY car`:each distinct value of car (red, and green) should be treated separately
    // `ORDER BY time`: within each group (greed or green) the values will be orderd by time
    let df = ctx
        .sql(
            "SELECT car, \
                      speed, \
                      lag(speed, 1) OVER (PARTITION BY car ORDER BY time),\
                      my_average(speed) OVER (PARTITION BY car ORDER BY time),\
                      time \
                      from cars",
        )
        .await?;
    // print the results
    df.show().await?;

    // // ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING: Run the window functon so that each invocation only sees 5 rows: the 2 before and 2 after) using
    // let df = ctx.sql("SELECT car, \
    //                   speed, \
    //                   lag(speed, 1) OVER (PARTITION BY car ORDER BY time ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING),\
    //                   time \
    //                   from cars").await?;
    // // print the results
    // df.show().await?;

    // todo show how to run dataframe API as well

    Ok(())
}

// TODO make a helper funciton like `crate_udf` that helps to make these signatures

fn my_average() -> WindowUDF {
    WindowUDF {
        name: String::from("my_average"),
        // it will take 2 arguments -- the column and the window size
        signature: Signature::exact(vec![DataType::Int32], Volatility::Immutable),
        return_type: Arc::new(return_type),
        partition_evaluator: Arc::new(make_partition_evaluator),
    }
}

/// Compute the return type of the function given the argument types
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

/// Create a partition evaluator for this argument
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

    /// This function is given the values of each partition
    fn evaluate(
        &self,
        values: &[arrow::array::ArrayRef],
        _num_rows: usize,
    ) -> Result<arrow::array::ArrayRef> {
        // datafusion has handled ensuring we get the correct input argument
        assert_eq!(values.len(), 1);

        // For this example, we convert convert the input argument to an
        // array of floating point numbers to calculate a moving average
        let arr: &Float64Array = values[0].as_ref().as_primitive::<Float64Type>();

        // implement a simple moving average by averaging the current
        // value with the previous value
        //
        // value | avg
        // ------+------
        //  10   | 10
        //  20   | 15
        //  30   | 25
        //  30   | 30
        //
        let mut previous_value = None;
        let new_values: Float64Array = arr
            .values()
            .iter()
            .map(|&value| {
                let new_value = previous_value
                    .map(|previous_value| (value + previous_value) / 2.0)
                    .unwrap_or(value);
                previous_value = Some(value);
                new_value
            })
            .collect();

        Ok(Arc::new(new_values))
    }

    fn evaluate_stateful(
        &mut self,
        _values: &[arrow::array::ArrayRef],
    ) -> Result<datafusion_common::ScalarValue> {
        Err(DataFusionError::NotImplemented(
            "evaluate_stateful is not implemented by default".into(),
        ))
    }

    fn evaluate_with_rank(
        &self,
        _num_rows: usize,
        _ranks_in_partition: &[std::ops::Range<usize>],
    ) -> Result<arrow::array::ArrayRef> {
        Err(DataFusionError::NotImplemented(
            "evaluate_partition_with_rank is not implemented by default".into(),
        ))
    }

    fn evaluate_inside_range(
        &self,
        _values: &[arrow::array::ArrayRef],
        _range: &std::ops::Range<usize>,
    ) -> Result<datafusion_common::ScalarValue> {
        Err(DataFusionError::NotImplemented(
            "evaluate_inside_range is not implemented by default".into(),
        ))
    }
}

// TODO show how to use other evaluate methods
