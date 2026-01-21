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

//! [`ScalarUDFImpl`] definitions for array_dims and array_ndims functions.

use arrow::array::{Array, ArrayRef, ListArray, UInt64Array};
use arrow::datatypes::{
    DataType,
    DataType::{FixedSizeList, LargeList, List, Null, UInt64},
    UInt64Type,
};
use std::any::Any;

use datafusion_common::cast::{
    as_fixed_size_list_array, as_large_list_array, as_list_array,
};
use datafusion_common::{Result, exec_err, utils::take_function_args};

use crate::utils::{compute_array_dims, make_scalar_function};
use datafusion_common::utils::list_ndims;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use itertools::Itertools;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArrayDims,
    array_dims,
    array,
    "returns an array of the array's dimensions.",
    array_dims_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns an array of the array's dimensions.",
    syntax_example = "array_dims(array)",
    sql_example = r#"```sql
> select array_dims([[1, 2, 3], [4, 5, 6]]);
+---------------------------------+
| array_dims(List([1,2,3,4,5,6])) |
+---------------------------------+
| [2, 3]                          |
+---------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayDims {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayDims {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayDims {
    pub fn new() -> Self {
        Self {
            signature: Signature::arrays(1, None, Volatility::Immutable),
            aliases: vec!["list_dims".to_string()],
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::new_list(UInt64, true))
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_dims_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

make_udf_expr_and_func!(
    ArrayNdims,
    array_ndims,
    array,
    "returns the number of dimensions of the array.",
    array_ndims_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the number of dimensions of the array.",
    syntax_example = "array_ndims(array, element)",
    sql_example = r#"```sql
> select array_ndims([[1, 2, 3], [4, 5, 6]]);
+----------------------------------+
| array_ndims(List([1,2,3,4,5,6])) |
+----------------------------------+
| 2                                |
+----------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(name = "element", description = "Array element.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct ArrayNdims {
    signature: Signature,
    aliases: Vec<String>,
}
impl ArrayNdims {
    pub fn new() -> Self {
        Self {
            signature: Signature::arrays(1, None, Volatility::Immutable),
            aliases: vec![String::from("list_ndims")],
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(UInt64)
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_ndims_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_dims_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("array_dims", args)?;
    let data: Vec<_> = match array.data_type() {
        List(_) => as_list_array(&array)?
            .iter()
            .map(compute_array_dims)
            .try_collect()?,
        LargeList(_) => as_large_list_array(&array)?
            .iter()
            .map(compute_array_dims)
            .try_collect()?,
        FixedSizeList(..) => as_fixed_size_list_array(&array)?
            .iter()
            .map(compute_array_dims)
            .try_collect()?,
        arg_type => {
            return exec_err!("array_dims does not support type {arg_type}");
        }
    };

    let result = ListArray::from_iter_primitive::<UInt64Type, _, _>(data);
    Ok(Arc::new(result))
}

fn array_ndims_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("array_ndims", args)?;

    fn general_list_ndims(array: &ArrayRef) -> Result<ArrayRef> {
        let ndims = list_ndims(array.data_type());
        let data = vec![ndims; array.len()];
        let result = UInt64Array::new(data.into(), array.nulls().cloned());
        Ok(Arc::new(result))
    }

    match array.data_type() {
        Null => Ok(Arc::new(UInt64Array::new_null(array.len()))),
        List(_) | LargeList(_) | FixedSizeList(..) => general_list_ndims(array),
        arg_type => exec_err!("array_ndims does not support type {arg_type}"),
    }
}
