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

use ballista::prelude::udf::UDFPlugin;
use ballista::prelude::{declare_udf_plugin, Result};
use ballista::prelude::{BallistaError, Plugin};
use datafusion::arrow::array::{ArrayRef, Float64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_plan::create_udaf;
use datafusion::physical_plan::functions::{make_scalar_function, Volatility};
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use datafusion_expr::Accumulator;
use std::any::Any;
use std::sync::Arc;

/// this examples show how to implements a udf plugin for Ballista
#[derive(Default)]
struct SimpleUDF {}

impl Plugin for SimpleUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl UDFPlugin for SimpleUDF {
    fn get_scalar_udf_by_name(&self, fun_name: &str) -> Result<ScalarUDF> {
        let error = format!("There is no user-define ScalarUDF named {}", fun_name);
        Ok(match fun_name {
            "pow" => create_pow_udf(),
            _ => {
                return Err(BallistaError::Internal(error));
            }
        })
    }

    fn udf_names(&self) -> Result<Vec<String>> {
        Ok(vec!["pow".to_string()])
    }

    fn get_aggregate_udf_by_name(&self, fun_name: &str) -> Result<AggregateUDF> {
        let error = format!("There is no user-define AggregateUDF named {}", fun_name);
        Ok(match fun_name {
            "geo_mean" => {
                // here is where we define the UDAF. We also declare its signature:
                create_udaf(
                    // the name; used to represent it in plan descriptions and in the registry, to use in SQL.
                    "geo_mean",
                    // the input type; DataFusion guarantees that the first entry of `values` in `update` has this type.
                    DataType::Float64,
                    // the return type; DataFusion expects this to match the type returned by `evaluate`.
                    Arc::new(DataType::Float64),
                    Volatility::Immutable,
                    // This is the accumulator factory; DataFusion uses it to create new accumulators.
                    Arc::new(|| Ok(Box::new(GeometricMean::new()))),
                    // This is the description of the state. `state()` must match the types here.
                    Arc::new(vec![DataType::Float64, DataType::UInt32]),
                )
            }
            _ => {
                return Err(BallistaError::Internal(error));
            }
        })
    }

    fn udaf_names(&self) -> Result<Vec<String>> {
        Ok(vec!["geo_mean".to_string()])
    }
}

declare_udf_plugin!(SimpleUDF, SimpleUDF::default);

fn create_pow_udf() -> ScalarUDF {
    let pow = |args: &[ArrayRef]| {
        // in DataFusion, all `args` and output are dynamically-typed arrays, which means that we need to:
        // 1. cast the values to the type we want
        // 2. perform the computation for every element in the array (using a loop or SIMD) and construct the result

        // this is guaranteed by DataFusion based on the function's signature.
        assert_eq!(args.len(), 2);

        // 1. cast both arguments to f64. These casts MUST be aligned with the signature or this function panics!
        let base = &args[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("cast failed");
        let exponent = &args[1]
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("cast failed");

        // this is guaranteed by DataFusion. We place it just to make it obvious.
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
        Ok(Arc::new(array) as ArrayRef)
    };
    // the function above expects an `ArrayRef`, but DataFusion may pass a scalar to a UDF.
    // thus, we use `make_scalar_function` to decorare the closure so that it can handle both Arrays and Scalar values.
    let pow = make_scalar_function(pow);
    create_udf(
        "pow",
        vec![DataType::Float64, DataType::Float64],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        pow,
    )
}

#[derive(Debug)]
struct GeometricMean {
    n: u32,
    prod: f64,
}

impl GeometricMean {
    // how the struct is initialized
    pub fn new() -> Self {
        GeometricMean { n: 0, prod: 1.0 }
    }

    // this function receives one entry per argument of this accumulator.
    // DataFusion calls this function on every row, and expects this function to update the accumulator's state.
    // fn update(&mut self, values: &[ScalarValue]) -> datafusion::error::Result<()> {
    //     // this is a one-argument UDAF, and thus we use `0`.
    //     let value = &values[0];
    //     match value {
    //         // here we map `ScalarValue` to our internal state. `Float64` indicates that this function
    //         // only accepts Float64 as its argument (DataFusion does try to coerce arguments to this type)
    //         //
    //         // Note that `.map` here ensures that we ignore Nulls.
    //         ScalarValue::Float64(e) => e.map(|value| {
    //             self.prod *= value;
    //             self.n += 1;
    //         }),
    //         _ => unreachable!(""),
    //     };
    //     Ok(())
    // }
    //
    // // this function receives states from other accumulators (Vec<ScalarValue>)
    // // and updates the accumulator.
    // fn merge(&mut self, states: &[ScalarValue]) -> datafusion::error::Result<()> {
    //     let prod = &states[0];
    //     let n = &states[1];
    //     match (prod, n) {
    //         (ScalarValue::Float64(Some(prod)), ScalarValue::UInt32(Some(n))) => {
    //             self.prod *= prod;
    //             self.n += n;
    //         }
    //         _ => unreachable!(""),
    //     };
    //     Ok(())
    // }
}

// UDAFs are built using the trait `Accumulator`, that offers DataFusion the necessary functions
// to use them.
impl Accumulator for GeometricMean {
    // This function serializes our state to `ScalarValue`, which DataFusion uses
    // to pass this state between execution stages.
    // Note that this can be arbitrary data.
    fn state(&self) -> datafusion::error::Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.prod),
            ScalarValue::from(self.n),
        ])
    }

    // DataFusion expects this function to return the final value of this aggregator.
    // in this case, this is the formula of the geometric mean
    fn evaluate(&self) -> datafusion::error::Result<ScalarValue> {
        Ok(ScalarValue::from(10.0))
    }

    // DataFusion calls this function to update the accumulator's state for a batch
    // of inputs rows. In this case the product is updated with values from the first column
    // and the count is updated based on the row count
    fn update_batch(&mut self, _values: &[ArrayRef]) -> datafusion::error::Result<()> {
        Ok(())
    }

    // Optimization hint: this trait also supports `update_batch` and `merge_batch`,
    // that can be used to perform these operations on arrays instead of single values.
    // By default, these methods call `update` and `merge` row by row
    fn merge_batch(&mut self, _states: &[ArrayRef]) -> datafusion::error::Result<()> {
        Ok(())
    }
}
