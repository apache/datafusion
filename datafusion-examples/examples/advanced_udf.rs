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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, Float32Array, Float64Array,
};
use arrow::compute;
use arrow::datatypes::{DataType, Float64Type};
use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::logical_expr::Volatility;
use datafusion::prelude::*;
use datafusion_common::{exec_err, internal_err, ScalarValue};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};

/// This example shows how to use the full ScalarUDFImpl API to implement a user
/// defined function. As in the `simple_udf.rs` example, this struct implements
/// a function that takes two arguments and returns the first argument raised to
/// the power of the second argument `a^b`.
///
/// To do so, we must implement the `ScalarUDFImpl` trait.
#[derive(Debug, Clone)]
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

    /// This function actually calculates the results of the scalar function.
    ///
    /// This is the same way that functions provided with DataFusion are invoked,
    /// which permits important special cases:
    ///
    ///1. When one or both of the arguments are single values (constants).
    ///   For example `pow(a, 2)`
    /// 2. When the input arrays can be reused (avoid allocating a new output array)
    ///
    /// However, it also means the implementation is more complex than when
    /// using `create_udf`.
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // The other fields of the `args` struct are used for more specialized
        // uses, and are not needed in this example
        let ScalarFunctionArgs { mut args, .. } = args;
        // DataFusion has arranged for the correct inputs to be passed to this
        // function, but we check again to make sure
        assert_eq!(args.len(), 2);
        // take ownership of arguments by popping in reverse order
        let exp = args.pop().unwrap();
        let base = args.pop().unwrap();
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
                    (Some(base), Some(exp)) => Some(base.powf(exp)),
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
                        // calculate the result for every row. The `unary`
                        // kernel creates very fast "vectorized" code and
                        // handles things like null values for us.
                        let res: Float64Array =
                            compute::unary(base_array, |base| base.powf(exp));
                        Arc::new(res)
                    }
                };
                Ok(ColumnarValue::Array(result_array))
            }

            // special case if the base is a constant.
            //
            // Note this case is very similar to the previous case, so we could
            // use the same pattern. However, for this case we demonstrate an
            // even more advanced pattern to potentially avoid allocating a new array
            (
                ColumnarValue::Scalar(ScalarValue::Float64(base)),
                ColumnarValue::Array(exp_array),
            ) => {
                let res = match base {
                    None => new_null_array(exp_array.data_type(), exp_array.len()),
                    Some(base) => maybe_pow_in_place(base, exp_array)?,
                };
                Ok(ColumnarValue::Array(res))
            }
            // Both arguments are arrays so we have to perform the calculation
            // for every row
            //
            // Note this could also be done in place using `binary_mut` as
            // is done in `maybe_pow_in_place` but here we use binary for simplicity
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
                internal_err!("Invalid argument types to pow function")
            }
        }
    }

    /// We will also add an alias of "my_pow"
    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        // The POW function preserves the order of its argument.
        Ok(input[0].sort_properties)
    }
}

/// Evaluate `base ^ exp` *without* allocating a new array, if possible
fn maybe_pow_in_place(base: f64, exp_array: ArrayRef) -> Result<ArrayRef> {
    // Calling `unary` creates a new array for the results. Avoiding
    // allocations is a common optimization in performance critical code.
    // arrow-rs allows this optimization via the `unary_mut`
    // and `binary_mut` kernels in certain cases
    //
    // These kernels can only be used if there are no other references to
    // the arrays (exp_array has to be the last remaining reference).
    let owned_array = exp_array
        // as in the previous example, we first downcast to &Float64Array
        .as_primitive::<Float64Type>()
        // non-obviously, we call clone here to get an owned `Float64Array`.
        // Calling clone() is relatively inexpensive as it increments
        // some ref counts but doesn't clone the data)
        //
        // Once we have the owned Float64Array we can drop the original
        // exp_array (untyped) reference
        .clone();

    // We *MUST* drop the reference to `exp_array` explicitly so that
    // owned_array is the only reference remaining in this function.
    //
    // Note that depending on the query there may still be other references
    // to the underlying buffers, which would prevent reuse. The only way to
    // know for sure is the result of `compute::unary_mut`
    drop(exp_array);

    // If we have the only reference, compute the result directly into the same
    // allocation as was used for the input array
    match compute::unary_mut(owned_array, |exp| base.powf(exp)) {
        Err(_orig_array) => {
            // unary_mut will return the original array if there are other
            // references into the underling buffer (and thus reuse is
            // impossible)
            //
            // In a real implementation, this case should fall back to
            // calling `unary` and allocate a new array; In this example
            // we will return an error for demonstration purposes
            exec_err!("Could not reuse array for maybe_pow_in_place")
        }
        // a result of OK means the operation was run successfully
        Ok(res) => Ok(Arc::new(res)),
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

    // You can also invoke both pow(2, 10) and its alias my_pow(a, b) using SQL
    ctx.sql("SELECT pow(2, 10), my_pow(a, b) FROM t")
        .await?
        .show()
        .await?;

    // You can also invoke pow_in_place by passing a constant base and a
    // column `a` as the exponent . If there is only a single
    // reference to `a` the code works well
    ctx.sql("SELECT pow(2, a) FROM t").await?.show().await?;

    // However, if there are multiple references to `a` in the evaluation
    // the array storage can not be reused
    let err = ctx
        .sql("SELECT pow(2, a), pow(3, a) FROM t")
        .await?
        .show()
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Execution error: Could not reuse array for maybe_pow_in_place"
    );

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

    // declare a new context. In Spark API, this corresponds to a new SparkSession
    let ctx = SessionContext::new();

    // declare a table in memory. In Spark API, this corresponds to createDataFrame(...).
    ctx.register_batch("t", batch)?;
    Ok(ctx)
}
