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

//! implementation of make_array function

use arrow::datatypes::{DataType, Field};
use arrow::datatypes::DataType::{FixedSizeList, LargeList, List, Utf8};
use datafusion_common::{plan_err, DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;
use arrow::array::{Array, ArrayData, ArrayRef, Capacities, GenericListArray, MutableArrayData, new_null_array, NullArray, OffsetSizeTrait};
use arrow::buffer::OffsetBuffer;
use datafusion_common::utils::array_into_list_array;

// Create static instances of ScalarUDFs for each function
make_udf_function!(
    MakeArray, make_array, xx, // arg name
    "yyy", // The name of the function to create the ScalarUDF
    udf
);

#[derive(Debug)]
pub(super) struct MakeArray {
    signature: Signature,
    aliases: Vec<String>,
}

impl MakeArray {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec![
                String::from("array_to_string"),
                String::from("list_to_string"),
                String::from("array_join"),
                String::from("list_join"),
            ],
        }
    }
}

impl ScalarUDFImpl for MakeArray {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_to_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => Utf8,
            _ => {
                return plan_err!("The array_to_string function can only accept List/LargeList/FixedSizeList.");
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        Ok(make_array_inner(&ColumnarValue::values_to_arrays(args)?)
            .map(ColumnarValue::Array)?)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}



