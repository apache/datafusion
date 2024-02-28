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
use arrow_schema::Field;
use datafusion_common::utils::list_ndims;
use datafusion_common::{plan_err, DataFusionError};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::type_coercion::binary::get_wider_type;
use datafusion_expr::Expr;
use datafusion_expr::TypeSignature::{Any as expr_Any, VariadicEqual};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::cmp::Ordering;
use std::sync::Arc;

use crate::kernels::make_scalar_function_with_hints;

// Create static instances of ScalarUDFs for each function
make_udf_function!(
    ArrayToString,
    array_to_string,
    array delimiter,                                                // arg name
    "converts each element to its text representation.", // doc
    array_to_string_udf                                  // internal function name
);

make_udf_function!(
    ArrayAppend,
    array_append,
    array element,                                         // arg name
    "appends an element to the end of an array.", // doc
    array_append_udf                              // internal function name
);

make_udf_function!(
    ArrayPrepend,
    array_prepend,
    element array,                                                // arg name
    "Prepends an element to the beginning of an array.", // doc
    array_prepend_udf                                    // internal function name
);

make_udf_function!(
    ArrayConcat,
    array_concat,
    "Concatenates arrays.", // doc
    array_concat_udf        // internal function name
);

make_udf_function!(
    MakeArray,
    make_array,
    "Returns an Arrow array using the specified input expressions.", // doc
    make_array_udf // internal function name
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
        make_scalar_function_with_hints(crate::kernels::array_to_string)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[derive(Debug)]
pub(super) struct ArrayAppend {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayAppend {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_element(Volatility::Immutable),
            aliases: vec![
                String::from("array_append"),
                String::from("list_append"),
                String::from("array_push_back"),
                String::from("list_push_back"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayAppend {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_append"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        make_scalar_function_with_hints(crate::kernels::array_append)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[derive(Debug)]
pub(super) struct ArrayPrepend {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayPrepend {
    pub fn new() -> Self {
        Self {
            signature: Signature::element_and_array(Volatility::Immutable),
            aliases: vec![
                String::from("array_prepend"),
                String::from("list_prepend"),
                String::from("array_push_front"),
                String::from("list_push_front"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayPrepend {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_prepend"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(arg_types[1].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        make_scalar_function_with_hints(crate::kernels::array_prepend)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[derive(Debug)]
pub(super) struct ArrayConcat {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayConcat {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec![
                String::from("array_concat"),
                String::from("array_cat"),
                String::from("list_concat"),
                String::from("list_cat"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayConcat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_concat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        let mut expr_type = DataType::Null;
        let mut max_dims = 0;
        for arg_type in arg_types {
            match arg_type {
                DataType::List(field) => {
                    if !field.data_type().equals_datatype(&DataType::Null) {
                        let dims = list_ndims(arg_type);
                        expr_type = match max_dims.cmp(&dims) {
                            Ordering::Greater => expr_type,
                            Ordering::Equal => get_wider_type(&expr_type, arg_type)?,
                            Ordering::Less => {
                                max_dims = dims;
                                arg_type.clone()
                            }
                        };
                    }
                }
                _ => {
                    return plan_err!(
                        "The array_concat function can only accept list as the args."
                    )
                }
            }
        }

        Ok(expr_type)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        make_scalar_function_with_hints(crate::kernels::array_concat)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[derive(Debug)]
pub struct MakeArray {
    signature: Signature,
    aliases: Vec<String>,
}

impl MakeArray {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![VariadicEqual, expr_Any(0)],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("make_array"), String::from("make_list")],
        }
    }
}

impl ScalarUDFImpl for MakeArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "make_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        match arg_types.len() {
            0 => Ok(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Null,
                true,
            )))),
            _ => {
                let mut expr_type = DataType::Null;
                for arg_type in arg_types {
                    if !arg_type.equals_datatype(&DataType::Null) {
                        expr_type = arg_type.clone();
                        break;
                    }
                }

                Ok(DataType::List(Arc::new(Field::new(
                    "item", expr_type, true,
                ))))
            }
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        make_scalar_function_with_hints(crate::kernels::make_array)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}
