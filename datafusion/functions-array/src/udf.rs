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

//! [`ScalarUDFImpl`] definitions for array functions.

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use datafusion_common::plan_err;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::Expr;
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;
// Create static instances of ScalarUDFs for each function
make_udf_function!(ArrayToString,
    array_to_string,
    array delimiter, // arg name
    "converts each element to its text representation.", // doc
    array_to_string_udf // internal function name
);
#[derive(Debug)]
pub(super) struct ArrayToString {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayToString {
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

impl ScalarUDFImpl for ArrayToString {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_to_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        use DataType::*;
        Ok(match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => Utf8,
            _ => {
                return plan_err!("The array_to_string function can only accept List/LargeList/FixedSizeList.");
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        crate::kernels::array_to_string(&args).map(ColumnarValue::Array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(
    Range,
    range,
    start stop step,
    "create a list of values in the range between start and stop",
    range_udf
);
#[derive(Debug)]
pub(super) struct Range {
    signature: Signature,
    aliases: Vec<String>,
}
impl Range {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Int64]),
                    Exact(vec![Int64, Int64]),
                    Exact(vec![Int64, Int64, Int64]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("range")],
        }
    }
}
impl ScalarUDFImpl for Range {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "range"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        use DataType::*;
        Ok(List(Arc::new(Field::new(
            "item",
            arg_types[0].clone(),
            true,
        ))))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        crate::kernels::gen_range(&args, 0).map(ColumnarValue::Array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(
    GenSeries,
    gen_series,
    start stop step,
    "create a list of values in the range between start and stop, include upper bound",
    gen_series_udf
);
#[derive(Debug)]
pub(super) struct GenSeries {
    signature: Signature,
    aliases: Vec<String>,
}
impl GenSeries {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Int64]),
                    Exact(vec![Int64, Int64]),
                    Exact(vec![Int64, Int64, Int64]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("generate_series")],
        }
    }
}
impl ScalarUDFImpl for GenSeries {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "generate_series"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        use DataType::*;
        Ok(List(Arc::new(Field::new(
            "item",
            arg_types[0].clone(),
            true,
        ))))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        crate::kernels::gen_range(&args, 1).map(ColumnarValue::Array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(
    ArrayDims,
    array_dims,
    array,
    "returns an array of the array's dimensions.",
    array_dims_udf
);

#[derive(Debug)]
pub(super) struct ArrayDims {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayDims {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec!["array_dims".to_string(), "list_dims".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayDims {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_dims"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        use DataType::*;
        Ok(match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => {
                List(Arc::new(Field::new("item", UInt64, true)))
            }
            _ => {
                return plan_err!("The array_dims function can only accept List/LargeList/FixedSizeList.");
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        crate::kernels::array_dims(&args).map(ColumnarValue::Array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(
    Cardinality,
    cardinality,
    array,
    "returns the total number of elements in the array.",
    cardinality_udf
);

impl Cardinality {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec![String::from("cardinality")],
        }
    }
}

#[derive(Debug)]
pub(super) struct Cardinality {
    signature: Signature,
    aliases: Vec<String>,
}
impl ScalarUDFImpl for Cardinality {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "cardinality"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        use DataType::*;
        Ok(match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => UInt64,
            _ => {
                return plan_err!("The cardinality function can only accept List/LargeList/FixedSizeList.");
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        crate::kernels::cardinality(&args).map(ColumnarValue::Array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(
    ArrayNdims,
    array_ndims,
    array,
    "returns the number of dimensions of the array.",
    array_ndims_udf
);

#[derive(Debug)]
pub(super) struct ArrayNdims {
    signature: Signature,
    aliases: Vec<String>,
}
impl ArrayNdims {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec![String::from("array_ndims"), String::from("list_ndims")],
        }
    }
}

impl ScalarUDFImpl for ArrayNdims {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_ndims"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        use DataType::*;
        Ok(match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => UInt64,
            _ => {
                return plan_err!("The array_ndims function can only accept List/LargeList/FixedSizeList.");
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        crate::kernels::array_ndims(&args).map(ColumnarValue::Array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
