<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Adding User Defined Functions: Scalar/Window/Aggregate

User Defined Functions (UDFs) are functions that can be used in the context of DataFusion execution.

This page covers how to add UDFs to DataFusion. In particular, it covers how to add Scalar, Window, and Aggregate UDFs.

| UDF Type  | Description                                                                                                | Example                                                                                                            |
| --------- | ---------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Scalar    | A function that takes a row of data and returns a single value.                                            | [simple_udf.rs](https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/simple_udf.rs)   |
| Window    | A function that takes a row of data and returns a single value, but also has access to the rows around it. | [simple_udwf.rs](https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/simple_udwf.rs) |
| Aggregate | A function that takes a group of rows and returns a single value.                                          | [simple_udaf.rs](https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/simple_udaf.rs) |

First we'll talk about adding an Scalar UDF end-to-end, then we'll talk about the differences between the different types of UDFs.

## Adding a Scalar UDF

A Scalar UDF is a function that takes a row of data and returns a single value. For example, this function takes a single i64 and returns a single i64 with 1 added to it:

```rust
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::common::Result;

use datafusion::common::cast::as_int64_array;

pub fn add_one(args: &[ArrayRef]) -> Result<ArrayRef> {
    // Error handling omitted for brevity

    let i64s = as_int64_array(&args[0])?;

    let new_array = i64s
      .iter()
      .map(|array_elem| array_elem.map(|value| value + 1))
      .collect::<Int64Array>();

    Ok(Arc::new(new_array))
}
```

For brevity, we'll skipped some error handling, but e.g. you may want to check that `args.len()` is the expected number of arguments.

This "works" in isolation, i.e. if you have a slice of `ArrayRef`s, you can call `add_one` and it will return a new `ArrayRef` with 1 added to each value.

```rust
let input = vec![Some(1), None, Some(3)];
let input = Arc::new(Int64Array::from(input)) as ArrayRef;

let result = add_one(&[input]).unwrap();
let result = result.as_any().downcast_ref::<Int64Array>().unwrap();

assert_eq!(result, &Int64Array::from(vec![Some(2), None, Some(4)]));
```

The challenge however is that DataFusion doesn't know about this function. We need to register it with DataFusion so that it can be used in the context of a query.

### Registering a Scalar UDF

To register a Scalar UDF, you need to wrap the function implementation in a `ScalarUDF` struct and then register it with the `SessionContext`. DataFusion provides the `create_udf` and `make_scalar_function` helper functions to make this easier.

```rust
use datafusion::logical_expr::{Volatility, create_udf};
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::arrow::datatypes::DataType;
use std::sync::Arc;

let udf = create_udf(
    "add_one",
    vec![DataType::Int64],
    Arc::new(DataType::Int64),
    Volatility::Immutable,
    make_scalar_function(add_one),
);
```

A few things to note:

- The first argument is the name of the function. This is the name that will be used in SQL queries.
- The second argument is a vector of `DataType`s. This is the list of argument types that the function accepts. I.e. in this case, the function accepts a single `Int64` argument.
- The third argument is the return type of the function. I.e. in this case, the function returns an `Int64`.
- The fourth argument is the volatility of the function. In short, this is used to determine if the function's performance can be optimized in some situations. In this case, the function is `Immutable` because it always returns the same value for the same input. A random number generator would be `Volatile` because it returns a different value for the same input.
- The fifth argument is the function implementation. This is the function that we defined above.

That gives us a `ScalarUDF` that we can register with the `SessionContext`:

```rust
use datafusion::execution::context::SessionContext;

let mut ctx = SessionContext::new();

ctx.register_udf(udf);
```

At this point, you can use the `add_one` function in your query:

```rust
let sql = "SELECT add_one(1)";

let df = ctx.sql(&sql).await.unwrap();
```

## Adding a Window UDF

Scalar UDFs are functions that take a row of data and return a single value. Window UDFs are similar, but they also have access to the rows around them. Access to the the proximal rows is helpful, but adds some complexity to the implementation.

For example, we will declare a user defined window function that computes a moving average.

```rust
use datafusion::arrow::{array::{ArrayRef, Float64Array, AsArray}, datatypes::Float64Type};
use datafusion::logical_expr::{PartitionEvaluator};
use datafusion::common::ScalarValue;
use datafusion::error::Result;
/// This implements the lowest level evaluation for a window function
///
/// It handles calculating the value of the window function for each
/// distinct values of `PARTITION BY`
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

/// Create a `PartitionEvalutor` to evaluate this function on a new
/// partition.
fn make_partition_evaluator() -> Result<Box<dyn PartitionEvaluator>> {
    Ok(Box::new(MyPartitionEvaluator::new()))
}
```

### Registering a Window UDF

To register a Window UDF, you need to wrap the function implementation in a `WindowUDF` struct and then register it with the `SessionContext`. DataFusion provides the `create_udwf` helper functions to make this easier.

```rust
use datafusion::logical_expr::{Volatility, create_udwf};
use datafusion::arrow::datatypes::DataType;
use std::sync::Arc;

// here is where we define the UDWF. We also declare its signature:
let smooth_it = create_udwf(
    "smooth_it",
    DataType::Float64,
    Arc::new(DataType::Float64),
    Volatility::Immutable,
    Arc::new(make_partition_evaluator),
);
```

Most arguments are just like `ScalarUDF` above, but still a little different with the second and fifth arguments:

- The first argument is the name of the function. This is the name that will be used in SQL queries. 
- **The second argument** is the `DataType` of input array (attention: this is not a list of arrays). I.e. in this case, the function accepts  `Float64` as argument.
- The third argument is the return type of the function. I.e. in this case, the function returns an `Float64`.
- The fourth argument is the volatility of the function. In short, this is used to determine if the function's performance can be optimized in some situations. In this case, the function is `Immutable` because it always returns the same value for the same input. A random number generator would be `Volatile` because it returns a different value for the same input.
- **The fifth argument** is the function implementation. This is the function that we defined above.

That gives us a `WindowUDF` that we can register with the `SessionContext`:

```rust
use datafusion::execution::context::SessionContext;

let ctx = SessionContext::new();

ctx.register_udwf(smooth_it);
```

At this point, you can use the `smooth_it` function in your query:

For example, if we have a [`cars.csv`](https://github.com/apache/arrow-datafusion/blob/main/datafusion/core/tests/data/cars.csv) whose contents like

```csv
car,speed,time
red,20.0,1996-04-12T12:05:03.000000000
red,20.3,1996-04-12T12:05:04.000000000
green,10.0,1996-04-12T12:05:03.000000000
green,10.3,1996-04-12T12:05:04.000000000
...
```

Then, we can query like below:

```rust
use datafusion::datasource::file_format::options::CsvReadOptions;
// register csv table first
let csv_path = "cars.csv".to_string();
ctx.register_csv("cars", &csv_path, CsvReadOptions::default().has_header(true)).await?;
// do query with smooth_it
let df = ctx
    .sql(
        "SELECT \
           car, \
           speed, \
           smooth_it(speed) OVER (PARTITION BY car ORDER BY time) as smooth_speed,\
           time \
           from cars \
         ORDER BY \
           car",
    )
    .await?;
// print the results
df.show().await?;
```

the output will be like:

```csv
+-------+-------+--------------------+---------------------+
| car   | speed | smooth_speed       | time                |
+-------+-------+--------------------+---------------------+
| green | 10.0  | 10.0               | 1996-04-12T12:05:03 |
| green | 10.3  | 10.15              | 1996-04-12T12:05:04 |
| green | 10.4  | 10.233333333333334 | 1996-04-12T12:05:05 |
| green | 10.5  | 10.3               | 1996-04-12T12:05:06 |
| green | 11.0  | 10.440000000000001 | 1996-04-12T12:05:07 |
| green | 12.0  | 10.700000000000001 | 1996-04-12T12:05:08 |
| green | 14.0  | 11.171428571428573 | 1996-04-12T12:05:09 |
| green | 15.0  | 11.65              | 1996-04-12T12:05:10 |
| green | 15.1  | 12.033333333333333 | 1996-04-12T12:05:11 |
| green | 15.2  | 12.35              | 1996-04-12T12:05:12 |
| green | 8.0   | 11.954545454545455 | 1996-04-12T12:05:13 |
| green | 2.0   | 11.125             | 1996-04-12T12:05:14 |
| red   | 20.0  | 20.0               | 1996-04-12T12:05:03 |
| red   | 20.3  | 20.15              | 1996-04-12T12:05:04 |
...
```

## Adding an Aggregate UDF

Aggregate UDFs are functions that take a group of rows and return a single value. These are akin to SQL's `SUM` or `COUNT` functions.

Body coming soon.
