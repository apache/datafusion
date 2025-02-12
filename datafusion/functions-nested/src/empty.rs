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
use arrow::array::{ArrayRef, BooleanArray, OffsetSizeTrait};
use arrow::datatypes::{
    DataType,
    DataType::{Boolean, FixedSizeList, LargeList, List},
};
use datafusion_common::cast::as_generic_list_array;
use datafusion_common::{exec_err, plan_err, Result};
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
#[derive(Debug)]
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
            signature: Signature::array(Volatility::Immutable),
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => Boolean,
            _ => {
                return plan_err!("The array_empty function can only accept List/LargeList/FixedSizeList.");
            }
        })
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_empty_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Array_empty SQL function
pub fn array_empty_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_empty expects one argument");
    }

    let array_type = args[0].data_type();
    match array_type {
        List(_) => general_array_empty::<i32>(&args[0]),
        LargeList(_) => general_array_empty::<i64>(&args[0]),
        _ => exec_err!("array_empty does not support type '{array_type:?}'."),
    }
}

fn general_array_empty<O: OffsetSizeTrait>(array: &ArrayRef) -> Result<ArrayRef> {
    let array = as_generic_list_array::<O>(array)?;

    let builder = array
        .iter()
        .map(|arr| arr.map(|arr| arr.is_empty()))
        .collect::<BooleanArray>();
    Ok(Arc::new(builder))
}
