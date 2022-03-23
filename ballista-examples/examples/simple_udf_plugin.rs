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
use datafusion::arrow::array::{ArrayRef, Int64Array, ListArray};
use datafusion::arrow::datatypes::{DataType, Field, Int64Type};
use datafusion::logical_plan::create_udaf;
use datafusion::physical_plan::functions::{make_scalar_function, Signature, Volatility};
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::scalar::ScalarValue;
use datafusion_expr::{Accumulator, ReturnTypeFunction, ScalarFunctionImplementation};
use lazy_static::lazy_static;
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
            "array_4" => create_udf_array_n("array_4", 4),
            _ => {
                return Err(BallistaError::Internal(error));
            }
        })
    }

    fn udf_names(&self) -> Result<Vec<String>> {
        Ok(vec!["array_4".to_string()])
    }

    fn get_aggregate_udf_by_name(&self, fun_name: &str) -> Result<AggregateUDF> {
        let error = format!("There is no user-define AggregateUDF named {}", fun_name);
        Ok(match fun_name {
            "geo_mean" => {
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

/// array_n, Construct an array of any length within n
pub fn create_udf_array_n(name: &str, n: usize) -> ScalarUDF {
    let fn_array = make_scalar_function(move |args: &[ArrayRef]| _array_n(args, n));
    let re_type = DataType::List(Box::new(Field::new("item", DataType::Int64, true)));
    create_udf(
        name,
        vec![DataType::Int64; n + 1],
        Arc::new(re_type),
        fn_array,
    )
}

/// create udf
pub fn create_udf(
    name: &str,
    input_types: Vec<DataType>,
    return_type: Arc<DataType>,
    fun: ScalarFunctionImplementation,
) -> ScalarUDF {
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));
    ScalarUDF::new(
        name,
        &Signature::exact(input_types, Volatility::Immutable),
        &return_type,
        &fun,
    )
}

/// Construct any length array
fn _array_n(args: &[ArrayRef], uindex: usize) -> datafusion::error::Result<ArrayRef> {
    let array_length = &args[uindex]
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("error: length");

    let array_items = args[0..array_length.value(0) as usize]
        .iter()
        .map(|arg| {
            arg.as_any()
                .downcast_ref::<Int64Array>()
                .expect("error: items")
        })
        .collect::<Vec<_>>();

    let result = INDEX_ARRAY[0..array_length.len()]
        .iter()
        .map(|i| {
            Some(
                array_items
                    .iter()
                    .map(|item| Some(item.value(i.to_owned())))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();

    Ok(Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(result)) as ArrayRef)
}

lazy_static! {
    pub static ref INDEX_ARRAY: Vec<usize> = {
        let mut index_array = vec![0; 10000];
        for (i, item) in index_array.iter_mut().enumerate().take(10000) {
            *item = i;
        }
        index_array
    };
}

/// This UDAF is copy from datafusion-example/examples/simple_udaf.rs and test for udf plugin.
/// A UDAF has state across multiple rows, and thus we require a `struct` with that state.
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
    fn update(&mut self, values: &[ScalarValue]) -> datafusion::error::Result<()> {
        // this is a one-argument UDAF, and thus we use `0`.
        let value = &values[0];
        match value {
            // here we map `ScalarValue` to our internal state. `Float64` indicates that this function
            // only accepts Float64 as its argument (DataFusion does try to coerce arguments to this type)
            //
            // Note that `.map` here ensures that we ignore Nulls.
            ScalarValue::Float64(e) => e.map(|value| {
                self.prod *= value;
                self.n += 1;
            }),
            _ => unreachable!(""),
        };
        Ok(())
    }

    // this function receives states from other accumulators (Vec<ScalarValue>)
    // and updates the accumulator.
    fn merge(&mut self, states: &[ScalarValue]) -> datafusion::error::Result<()> {
        let prod = &states[0];
        let n = &states[1];
        match (prod, n) {
            (ScalarValue::Float64(Some(prod)), ScalarValue::UInt32(Some(n))) => {
                self.prod *= prod;
                self.n += n;
            }
            _ => unreachable!(""),
        };
        Ok(())
    }
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
        let value = self.prod.powf(1.0 / self.n as f64);
        Ok(ScalarValue::from(value))
    }

    // DataFusion calls this function to update the accumulator's state for a batch
    // of inputs rows. In this case the product is updated with values from the first column
    // and the count is updated based on the row count
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::error::Result<()> {
        if values.is_empty() {
            return Ok(());
        };
        (0..values[0].len()).try_for_each(|index| {
            let v = values
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            self.update(&v)
        })
    }

    // Optimization hint: this trait also supports `update_batch` and `merge_batch`,
    // that can be used to perform these operations on arrays instead of single values.
    // By default, these methods call `update` and `merge` row by row
    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::error::Result<()> {
        if states.is_empty() {
            return Ok(());
        };
        (0..states[0].len()).try_for_each(|index| {
            let v = states
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            self.merge(&v)
        })
    }
}
