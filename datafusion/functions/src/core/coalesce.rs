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

use arrow::array::{new_null_array, BooleanArray};
use arrow::compute::kernels::zip::zip;
use arrow::compute::{and, is_not_null, is_null};
use arrow::datatypes::DataType;
use datafusion_common::{exec_err, internal_err, Result};
use datafusion_expr::binary::try_type_union_resolution;
use datafusion_expr::{ColumnarValue, Documentation, ReturnInfo, ReturnTypeArgs};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;
use itertools::Itertools;
use std::any::Any;

#[user_doc(
    doc_section(label = "Conditional Functions"),
    description = "Returns the first of its arguments that is not _null_. Returns _null_ if all arguments are _null_. This function is often used to substitute a default value for _null_ values.",
    syntax_example = "coalesce(expression1[, ..., expression_n])",
    sql_example = r#"```sql
> select coalesce(null, null, 'datafusion');
+----------------------------------------+
| coalesce(NULL,NULL,Utf8("datafusion")) |
+----------------------------------------+
| datafusion                             |
+----------------------------------------+
```"#,
    argument(
        name = "expression1, expression_n",
        description = "Expression to use if previous expressions are _null_. Can be a constant, column, or function, and any combination of arithmetic operators. Pass as many expression arguments as necessary."
    )
)]
#[derive(Debug)]
pub struct CoalesceFunc {
    signature: Signature,
}

impl Default for CoalesceFunc {
    fn default() -> Self {
        CoalesceFunc::new()
    }
}

impl CoalesceFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CoalesceFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "coalesce"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_type_from_args should be called instead")
    }

    fn return_type_from_args(&self, args: ReturnTypeArgs) -> Result<ReturnInfo> {
        // If any the arguments in coalesce is non-null, the result is non-null
        let nullable = args.nullables.iter().all(|&nullable| nullable);
        let return_type = args
            .arg_types
            .iter()
            .find_or_first(|d| !d.is_null())
            .unwrap()
            .clone();
        Ok(ReturnInfo::new(return_type, nullable))
    }

    /// coalesce evaluates to the first value which is not NULL
    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        // do not accept 0 arguments.
        if args.is_empty() {
            return exec_err!(
                "coalesce was called with {} arguments. It requires at least 1.",
                args.len()
            );
        }

        let return_type = args[0].data_type();
        let mut return_array = args.iter().filter_map(|x| match x {
            ColumnarValue::Array(array) => Some(array.len()),
            _ => None,
        });

        if let Some(size) = return_array.next() {
            // start with nulls as default output
            let mut current_value = new_null_array(&return_type, size);
            let mut remainder = BooleanArray::from(vec![true; size]);

            for arg in args {
                match arg {
                    ColumnarValue::Array(ref array) => {
                        let to_apply = and(&remainder, &is_not_null(array.as_ref())?)?;
                        current_value = zip(&to_apply, array, &current_value)?;
                        remainder = and(&remainder, &is_null(array)?)?;
                    }
                    ColumnarValue::Scalar(value) => {
                        if value.is_null() {
                            continue;
                        } else {
                            let last_value = value.to_scalar()?;
                            current_value = zip(&remainder, &last_value, &current_value)?;
                            break;
                        }
                    }
                }
                if remainder.iter().all(|x| x == Some(false)) {
                    break;
                }
            }
            Ok(ColumnarValue::Array(current_value))
        } else {
            let result = args
                .iter()
                .filter_map(|x| match x {
                    ColumnarValue::Scalar(s) if !s.is_null() => Some(x.clone()),
                    _ => None,
                })
                .next()
                .unwrap_or_else(|| args[0].clone());
            Ok(result)
        }
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            return exec_err!("coalesce must have at least one argument");
        }

        try_type_union_resolution(arg_types)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
