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

use datafusion::{
    arrow::{
        array::{ArrayRef, Float32Array, Float64Array},
        datatypes::DataType,
        record_batch::RecordBatch,
    },
    logical_expr::Volatility,
};

use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::cast::as_float64_array;
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

/// create local execution context with an in-memory table:
///
/// ```text
/// +-----+-----+
/// | a   | b   |
/// +-----+-----+
/// | 2.1 | 1.0 |
/// | 3.1 | 2.0 |
/// | 4.1 | 3.0 |
/// | 5.1 | 4.0 |
/// +-----+-----+
/// ```
fn create_context() -> Result<SessionContext> {
    // define data.
    let a: ArrayRef = Arc::new(Float32Array::from(vec![2.1, 3.1, 4.1, 5.1]));
    let b: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]));
    let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)])?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    ctx.register_batch("t", batch)?;
    Ok(ctx)
}

/// In this example we will declare a single-type, single return type UDF that exponentiates f64, a^b
#[tokio::main]
async fn main() -> Result<()> {
    let ctx = create_context()?;

    // First, declare the actual implementation of the calculation
    let pow = Arc::new(|args: &[ColumnarValue]| {
        // in DataFusion, all `args` and output are dynamically-typed arrays, which means that we need to:
        // 1. cast the values to the type we want
        // 2. perform the computation for every element in the array (using a loop or SIMD) and construct the result

        // this is guaranteed by DataFusion based on the function's signature.
        assert_eq!(args.len(), 2);

        // Expand the arguments to arrays (this is simple, but inefficient for
        // single constant values).
        let args = ColumnarValue::values_to_arrays(args)?;

        // 1. cast both arguments to f64. These casts MUST be aligned with the signature or this function panics!
        let base = as_float64_array(&args[0]).expect("cast failed");
        let exponent = as_float64_array(&args[1]).expect("cast failed");

        // The array lengths is guaranteed by DataFusion. We assert here to make it obvious.
        assert_eq!(exponent.len(), base.len());

        // 2. perform the computation
        let array = base
            .iter()
            .zip(exponent.iter())
            .map(|(base, exponent)| {
                match (base, exponent) {
                    // in arrow, any value can be null.
                    // Here we decide to make our UDF to return null when either base or exponent is null.
                    (Some(base), Some(exponent)) => Some(base.powf(exponent)),
                    _ => None,
                }
            })
            .collect::<Float64Array>();

        // `Ok` because no error occurred during the calculation (we should add one if exponent was [0, 1[ and the base < 0 because that panics!)
        // `Arc` because arrays are immutable, thread-safe, trait objects.
        Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
    });

    // Next:
    // * give it a name so that it shows nicely when the plan is printed
    // * declare what input it expects
    // * declare its return type
    let pow = create_udf(
        "pow",
        // expects two f64
        vec![DataType::Float64, DataType::Float64],
        // returns f64
        DataType::Float64,
        Volatility::Immutable,
        pow,
    );

    // at this point, we can use it or register it, depending on the use-case:
    // * if the UDF is expected to be used throughout the program in different contexts,
    //   we can register it, and call it later:
    ctx.register_udf(pow.clone()); // clone is only required in this example because we show both usages

    // * if the UDF is expected to be used directly in the scope, `.call` it directly:
    let expr = pow.call(vec![col("a"), col("b")]);

    // get a DataFrame from the context
    let df = ctx.table("t").await?;

    // if we do not have `pow` in the scope and we registered it, we can get it from the registry
    let pow = df.registry().udf("pow")?;
    // equivalent to expr
    let expr1 = pow.call(vec![col("a"), col("b")]);

    // equivalent to `'SELECT pow(a, b), pow(a, b) AS pow1 FROM t'`
    let df = df.select(vec![
        expr,
        // alias so that they have different column names
        expr1.alias("pow1"),
    ])?;

    // note that "b" is f32, not f64. DataFusion coerces the types to match the UDF's signature.

    // print the results
    df.show().await?;

    // Given that `pow` is registered in the context, we can also use it in SQL:
    let sql_df = ctx.sql("SELECT pow(a, b) FROM t").await?;

    // print the results
    sql_df.show().await?;

    Ok(())
}
