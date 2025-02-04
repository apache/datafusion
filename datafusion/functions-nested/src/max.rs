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

//! [`ScalarUDFImpl`] definitions for array_max function.
use crate::sort::array_sort_inner;
use crate::utils::make_scalar_function;
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_schema::DataType;
use arrow_schema::DataType::{FixedSizeList, LargeList, List};
use datafusion_common::cast::as_list_array;
use datafusion_common::exec_err;
use datafusion_doc::Documentation;
use datafusion_expr::{ColumnarValue, Expr, ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArrayMax,
    array_max,
    array,
    "returns the maximum value in the array.",
    array_max_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the maximum value in the array.",
    syntax_example = "array_max(array)",
    sql_example = r#"```sql
> select array_max([3,1,4,2]);
+-----------------------------------------+
| array_max(List([3,1,4,2]))              |
+-----------------------------------------+
| 4                                       |
+-----------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug)]
pub struct ArrayMax {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayMax {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayMax {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec!["list_max".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayMax {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_max"
    }

    fn display_name(&self, args: &[Expr]) -> datafusion_common::Result<String> {
        let args_name = args.iter().map(ToString::to_string).collect::<Vec<_>>();
        if args_name.len() != 1 {
            return exec_err!("expects 1 arg, got {}", args_name.len());
        }

        Ok(format!("{}", args_name[0]))
    }

    fn schema_name(&self, args: &[Expr]) -> datafusion_common::Result<String> {
        let args_name = args
            .iter()
            .map(|e| e.schema_name().to_string())
            .collect::<Vec<_>>();
        if args_name.len() != 1 {
            return exec_err!("expects 1 arg, got {}", args_name.len());
        }

        Ok(format!("{}", args_name[0]))
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        match &arg_types[0] {
            List(field) | LargeList(field) | FixedSizeList(field, _) => {
                Ok(field.data_type().clone())
            }
            _ => exec_err!(
                "Not reachable, data_type should be List, LargeList or FixedSizeList"
            ),
        }
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> datafusion_common::Result<ColumnarValue> {
        make_scalar_function(array_max_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// array_max SQL function
///
/// There is one argument for array_max as the array.
/// `array_max(array)`
///
/// For example:
/// > array_max(\[1, 3, 2]) -> 3
pub fn array_max_inner(args: &[ArrayRef]) -> datafusion_common::Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_max needs one argument");
    }

    match &args[0].data_type() {
        List(_) | LargeList(_) | FixedSizeList(_, _) => {
            let new_args = vec![
                args[0].clone(),
                Arc::new(StringArray::from_iter(vec![Some("DESC")])),
                Arc::new(StringArray::from_iter(vec![Some("NULLS LAST")])),
            ];
            array_max_internal(&new_args)
        }
        _ => exec_err!("array_max does not support type: {:?}", args[0].data_type()),
    }
}

fn array_max_internal(args: &[ArrayRef]) -> datafusion_common::Result<ArrayRef> {
    let sorted_array = array_sort_inner(args)?;
    let result_array = as_list_array(&sorted_array)?.value(0);
    if result_array.is_empty() {
        return exec_err!("array_max needs one argument as non-empty array");
    }
    let max_result = result_array.slice(0, 1);
    Ok(max_result)
}
