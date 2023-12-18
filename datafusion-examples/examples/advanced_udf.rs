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
use std::any::Any;

use arrow::array::{new_null_array, Array, AsArray};
use arrow::compute;
use arrow::datatypes::Float64Type;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::{internal_err, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature};
use std::sync::Arc;

/// This example shows how to use the full ScalarUDFImpl API to implement a user
/// defined function. As in the `simple_udf.rs` example, this struct implements
/// a function that takes two arguments and returns the first argument raised to
/// the power of the second argument `a^b`.
///
/// To do so, we must implement the `ScalarUDFImpl` trait.
struct PowUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl PowUdf {
    /// Create a new instance of the `PowUdf` struct
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                // this function will always take two arguments of type f64
                vec![DataType::Float64, DataType::Float64],
                // this function is deterministic and will always return the same
                // result for the same input
                Volatility::Immutable,
            ),
            // we will also add an alias of "my_pow"
            aliases: vec!["my_pow".to_string()],
        }
    }
}

impl ScalarUDFImpl for PowUdf {
    /// We implement as_any so that we can downcast the ScalarUDFImpl trait object
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the name of this function
    fn name(&self) -> &str {
        "pow"
    }

    /// Return the "signature" of this function -- namely what types of arguments it will take
    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// What is the type of value that will be returned by this function? In
    /// this case it will always be a constant value, but it could also be a
    /// function of the input types.
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    /// This is the function that actually calculates the results.
    ///
    /// This is the same way that functions built into DataFusion are invoked,
    /// which permits important special cases when one or both of the arguments
    /// are single values (constants). For example `pow(a, 2)`
    ///
    /// However, it also means the implementation is more complex than when
    /// using `create_udf`.
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        // DataFusion has arranged for the correct inputs to be passed to this
        // function, but we check again to make sure
        assert_eq!(args.len(), 2);
        let (base, exp) = (&args[0], &args[1]);
        assert_eq!(base.data_type(), DataType::Float64);
        assert_eq!(exp.data_type(), DataType::Float64);

        match (base, exp) {
            // For demonstration purposes we also implement the scalar / scalar
            // case here, but it is not typically required for high performance.
            //
            // For performance it is most important to optimize cases where at
            // least one argument is an array. If all arguments are constants,
            // the DataFusion expression simplification logic will often invoke
            // this path once during planning, and simply use the result during
            // execution.
            (
                ColumnarValue::Scalar(ScalarValue::Float64(base)),
                ColumnarValue::Scalar(ScalarValue::Float64(exp)),
            ) => {
                // compute the output. Note DataFusion treats `None` as NULL.
                let res = match (base, exp) {
                    (Some(base), Some(exp)) => Some(base.powf(*exp)),
                    // one or both arguments were NULL
                    _ => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::from(res)))
            }
            // special case if the exponent is a constant
            (
                ColumnarValue::Array(base_array),
                ColumnarValue::Scalar(ScalarValue::Float64(exp)),
            ) => {
                let result_array = match exp {
                    // a ^ null = null
                    None => new_null_array(base_array.data_type(), base_array.len()),
                    // a ^ exp
                    Some(exp) => {
                        // DataFusion has ensured both arguments are Float64:
                        let base_array = base_array.as_primitive::<Float64Type>();
                        // calculate the result for every row. The `unary` very
                        // fast,  "vectorized" code and handles things like null
                        // values for us.
                        let res: Float64Array =
                            compute::unary(base_array, |base| base.powf(*exp));
                        Arc::new(res)
                    }
                };
                Ok(ColumnarValue::Array(result_array))
            }

            // special case if the base is a constant (note this code is quite
            // similar to the previous case, so we omit comments)
            (
                ColumnarValue::Scalar(ScalarValue::Float64(base)),
                ColumnarValue::Array(exp_array),
            ) => {
                let res = match base {
                    None => new_null_array(exp_array.data_type(), exp_array.len()),
                    Some(base) => {
                        let exp_array = exp_array.as_primitive::<Float64Type>();
                        let res: Float64Array =
                            compute::unary(exp_array, |exp| base.powf(exp));
                        Arc::new(res)
                    }
                };
                Ok(ColumnarValue::Array(res))
            }
            // Both arguments are arrays so we have to perform the calculation for every row
            (ColumnarValue::Array(base_array), ColumnarValue::Array(exp_array)) => {
                let res: Float64Array = compute::binary(
                    base_array.as_primitive::<Float64Type>(),
                    exp_array.as_primitive::<Float64Type>(),
                    |base, exp| base.powf(exp),
                )?;
                Ok(ColumnarValue::Array(Arc::new(res)))
            }
            // if the types were not float, it is a bug in DataFusion
            _ => {
                use datafusion_common::DataFusionError;
                internal_err!("Invalid argument types to pow function")
            }
        }
    }

    /// We will also add an alias of "my_pow"
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// In this example we register `PowUdf` as a user defined function
/// and invoke it via the DataFrame API and SQL
#[tokio::main]
async fn main() -> Result<()> {
    let ctx = create_context()?;

    // create the UDF
    let pow = ScalarUDF::from(PowUdf::new());

    // register the UDF with the context so it can be invoked by name and from SQL
    ctx.register_udf(pow.clone());

    // get a DataFrame from the context for scanning the "t" table
    let df = ctx.table("t").await?;

    // Call pow(a, 10) using the DataFrame API
    let df = df.select(vec![pow.call(vec![col("a"), lit(10i32)])])?;

    // note that the second argument is passed as an i32, not f64. DataFusion
    // automatically coerces the types to match the UDF's defined signature.

    // print the results
    df.show().await?;

    // You can also invoke both pow(2, 10)  and its alias my_pow(a, b) using SQL
    let sql_df = ctx.sql("SELECT pow(2, 10), my_pow(a, b) FROM t").await?;
    sql_df.show().await?;

    Ok(())
}

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
