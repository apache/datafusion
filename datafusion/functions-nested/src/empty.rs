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

//! [`ScalarUDFImpl`] definitions for array_empty function.

use crate::utils::make_scalar_function;
use arrow::array::{Array, ArrayRef, BooleanArray, OffsetSizeTrait};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::{
    DataType,
    DataType::{Boolean, FixedSizeList, LargeList, List},
};
use datafusion_common::cast::as_generic_list_array;
use datafusion_common::{Result, exec_err, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArrayEmpty,
    array_empty,
    array,
    "returns true for an empty array or false for a non-empty array.",
    array_empty_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns 1 for an empty array or 0 for a non-empty array.",
    syntax_example = "empty(array)",
    sql_example = r#"```sql
> select empty([1]);
+------------------+
| empty(List([1])) |
+------------------+
| 0                |
+------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayEmpty {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayEmpty {
    fn default() -> Self {
        Self::new()
    }
}
impl ArrayEmpty {
    pub fn new() -> Self {
        Self {
            signature: Signature::arrays(1, None, Volatility::Immutable),
            aliases: vec!["array_empty".to_string(), "list_empty".to_string()],
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Boolean)
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_empty_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_empty_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("array_empty", args)?;
    match array.data_type() {
        List(_) => general_array_empty::<i32>(array),
        LargeList(_) => general_array_empty::<i64>(array),
        FixedSizeList(_, size) => {
            let values = if *size == 0 {
                BooleanBuffer::new_set(array.len())
            } else {
                BooleanBuffer::new_unset(array.len())
            };
            Ok(Arc::new(BooleanArray::new(values, array.nulls().cloned())))
        }
        arg_type => exec_err!("array_empty does not support type {arg_type}"),
    }
}

fn general_array_empty<O: OffsetSizeTrait>(array: &ArrayRef) -> Result<ArrayRef> {
    let result = as_generic_list_array::<O>(array)?
        .iter()
        .map(|arr| arr.map(|arr| arr.is_empty()))
        .collect::<BooleanArray>();
    Ok(Arc::new(result))
}
