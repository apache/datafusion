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

use arrow::array::{NullArray, StringArray};
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::IntervalUnit::MonthDayNano;
use arrow_schema::DataType::{LargeUtf8, List, Utf8};
use datafusion_common::exec_err;
use datafusion_common::plan_err;
use datafusion_common::Result;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::Expr;
use datafusion_expr::TypeSignature;
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
pub struct ArrayToString {
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
        let args = ColumnarValue::values_to_arrays(args)?;
        crate::kernels::array_to_string(&args).map(ColumnarValue::Array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(StringToArray,
    string_to_array,
    string delimiter null_string, // arg name
    "splits a `string` based on a `delimiter` and returns an array of parts. Any parts matching the optional `null_string` will be replaced with `NULL`", // doc
    string_to_array_udf // internal function name
);
#[derive(Debug)]
pub struct StringToArray {
    signature: Signature,
    aliases: Vec<String>,
}

impl StringToArray {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec![
                String::from("string_to_array"),
                String::from("string_to_list"),
            ],
        }
    }
}

impl ScalarUDFImpl for StringToArray {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "string_to_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(match arg_types[0] {
            Utf8 | LargeUtf8 => {
                List(Arc::new(Field::new("item", arg_types[0].clone(), true)))
            }
            _ => {
                return plan_err!(
                    "The string_to_array function can only accept Utf8 or LargeUtf8."
                );
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let mut args = ColumnarValue::values_to_arrays(args)?;
        // Case: delimiter is NULL, needs to be handled as well.
        if args[1].as_any().is::<NullArray>() {
            args[1] = Arc::new(StringArray::new_null(args[1].len()));
        };

        match args[0].data_type() {
            Utf8 => {
                crate::kernels::string_to_array::<i32>(&args).map(ColumnarValue::Array)
            }
            LargeUtf8 => {
                crate::kernels::string_to_array::<i64>(&args).map(ColumnarValue::Array)
            }
            other => {
                exec_err!("unsupported type for string_to_array function as {other}")
            }
        }
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
pub struct Range {
    signature: Signature,
    aliases: Vec<String>,
}
impl Range {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![Int64]),
                    TypeSignature::Exact(vec![Int64, Int64]),
                    TypeSignature::Exact(vec![Int64, Int64, Int64]),
                    TypeSignature::Exact(vec![Date32, Date32, Interval(MonthDayNano)]),
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(List(Arc::new(Field::new(
            "item",
            arg_types[0].clone(),
            true,
        ))))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        match args[0].data_type() {
            arrow::datatypes::DataType::Int64 => {
                crate::kernels::gen_range(&args, false).map(ColumnarValue::Array)
            }
            arrow::datatypes::DataType::Date32 => {
                crate::kernels::gen_range_date(&args, false).map(ColumnarValue::Array)
            }
            _ => {
                exec_err!("unsupported type for range")
            }
        }
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
pub struct GenSeries {
    signature: Signature,
    aliases: Vec<String>,
}
impl GenSeries {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![Int64]),
                    TypeSignature::Exact(vec![Int64, Int64]),
                    TypeSignature::Exact(vec![Int64, Int64, Int64]),
                    TypeSignature::Exact(vec![Date32, Date32, Interval(MonthDayNano)]),
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(List(Arc::new(Field::new(
            "item",
            arg_types[0].clone(),
            true,
        ))))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        match args[0].data_type() {
            arrow::datatypes::DataType::Int64 => {
                crate::kernels::gen_range(&args, true).map(ColumnarValue::Array)
            }
            arrow::datatypes::DataType::Date32 => {
                crate::kernels::gen_range_date(&args, true).map(ColumnarValue::Array)
            }
            _ => {
                exec_err!("unsupported type for range")
            }
        }
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
pub struct ArrayDims {
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        crate::kernels::array_dims(&args).map(ColumnarValue::Array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(
    ArraySort,
    array_sort,
    array desc null_first,
    "returns sorted array.",
    array_sort_udf
);

#[derive(Debug)]
pub struct ArraySort {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArraySort {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec!["array_sort".to_string(), "list_sort".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArraySort {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_sort"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        match &arg_types[0] {
            List(field) | FixedSizeList(field, _) => Ok(List(Arc::new(Field::new(
                "item",
                field.data_type().clone(),
                true,
            )))),
            LargeList(field) => Ok(LargeList(Arc::new(Field::new(
                "item",
                field.data_type().clone(),
                true,
            )))),
            _ => exec_err!(
                "Not reachable, data_type should be List, LargeList or FixedSizeList"
            ),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        crate::kernels::array_sort(&args).map(ColumnarValue::Array)
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
pub struct Cardinality {
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => UInt64,
            _ => {
                return plan_err!("The cardinality function can only accept List/LargeList/FixedSizeList.");
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
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
pub struct ArrayNdims {
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => UInt64,
            _ => {
                return plan_err!("The array_ndims function can only accept List/LargeList/FixedSizeList.");
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        crate::kernels::array_ndims(&args).map(ColumnarValue::Array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(
    ArrayEmpty,
    array_empty,
    array,
    "returns true for an empty array or false for a non-empty array.",
    array_empty_udf
);

#[derive(Debug)]
pub struct ArrayEmpty {
    signature: Signature,
    aliases: Vec<String>,
}
impl ArrayEmpty {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec![String::from("empty")],
        }
    }
}

impl ScalarUDFImpl for ArrayEmpty {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "empty"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => Boolean,
            _ => {
                return plan_err!("The array_empty function can only accept List/LargeList/FixedSizeList.");
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        crate::kernels::array_empty(&args).map(ColumnarValue::Array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(
    ArrayRepeat,
    array_repeat,
    element count, // arg name
    "returns an array containing element `count` times.", // doc
    array_repeat_udf // internal function name
);
#[derive(Debug)]
pub struct ArrayRepeat {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayRepeat {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec![String::from("array_repeat"), String::from("list_repeat")],
        }
    }
}

impl ScalarUDFImpl for ArrayRepeat {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_repeat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(List(Arc::new(Field::new(
            "item",
            arg_types[0].clone(),
            true,
        ))))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        crate::kernels::array_repeat(&args).map(ColumnarValue::Array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(
    ArrayLength,
    array_length,
    array,
    "returns the length of the array dimension.",
    array_length_udf
);

#[derive(Debug)]
pub struct ArrayLength {
    signature: Signature,
    aliases: Vec<String>,
}
impl ArrayLength {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec![String::from("array_length"), String::from("list_length")],
        }
    }
}

impl ScalarUDFImpl for ArrayLength {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => UInt64,
            _ => {
                return plan_err!("The array_length function can only accept List/LargeList/FixedSizeList.");
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        crate::kernels::array_length(&args).map(ColumnarValue::Array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(
    Flatten,
    flatten,
    array,
    "flattens an array of arrays into a single array.",
    flatten_udf
);

#[derive(Debug)]
pub struct Flatten {
    signature: Signature,
    aliases: Vec<String>,
}
impl Flatten {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec![String::from("flatten")],
        }
    }
}

impl ScalarUDFImpl for Flatten {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "flatten"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        fn get_base_type(data_type: &DataType) -> Result<DataType> {
            match data_type {
                List(field) | FixedSizeList(field, _)
                    if matches!(field.data_type(), List(_) | FixedSizeList(_, _)) =>
                {
                    get_base_type(field.data_type())
                }
                LargeList(field) if matches!(field.data_type(), LargeList(_)) => {
                    get_base_type(field.data_type())
                }
                Null | List(_) | LargeList(_) => Ok(data_type.to_owned()),
                FixedSizeList(field, _) => Ok(List(field.clone())),
                _ => exec_err!(
                    "Not reachable, data_type should be List, LargeList or FixedSizeList"
                ),
            }
        }

        let data_type = get_base_type(&arg_types[0])?;
        Ok(data_type)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        crate::kernels::flatten(&args).map(ColumnarValue::Array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(
    ArrayDistinct,
    array_distinct,
    array,
    "return distinct values from the array after removing duplicates.",
    array_distinct_udf
);

#[derive(Debug)]
pub struct ArrayDistinct {
    signature: Signature,
    aliases: Vec<String>,
}

impl crate::udf::ArrayDistinct {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec!["array_distinct".to_string(), "list_distinct".to_string()],
        }
    }
}

impl ScalarUDFImpl for crate::udf::ArrayDistinct {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_distinct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        match &arg_types[0] {
            List(field) | FixedSizeList(field, _) => Ok(List(Arc::new(Field::new(
                "item",
                field.data_type().clone(),
                true,
            )))),
            LargeList(field) => Ok(LargeList(Arc::new(Field::new(
                "item",
                field.data_type().clone(),
                true,
            )))),
            _ => exec_err!(
                "Not reachable, data_type should be List, LargeList or FixedSizeList"
            ),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        crate::kernels::array_distinct(&args).map(ColumnarValue::Array)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
