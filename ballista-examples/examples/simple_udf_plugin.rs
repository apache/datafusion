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
use datafusion::physical_plan::functions::{make_scalar_function, Signature, Volatility};
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion_expr::{ReturnTypeFunction, ScalarFunctionImplementation};
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
        Err(BallistaError::Internal(error))
    }

    fn udaf_names(&self) -> Result<Vec<String>> {
        Ok(vec![])
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
