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

//! [`ArrowCastFunc`]: Implementation of the `arrow_cast`

use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::error::ArrowError;
use datafusion_common::{
    arrow_datafusion_err, exec_err, internal_err, Result, ScalarValue,
};
use datafusion_common::{exec_datafusion_err, utils::take_function_args};
use std::any::Any;

use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

/// Implements casting to arbitrary arrow types (rather than SQL types)
///
/// Note that the `arrow_cast` function is somewhat special in that its
/// return depends only on the *value* of its second argument (not its type)
///
/// It is implemented by calling the same underlying arrow `cast` kernel as
/// normal SQL casts.
///
/// For example to cast to `int` using SQL  (which is then mapped to the arrow
/// type `Int32`)
///
/// ```sql
/// select cast(column_x as int) ...
/// ```
///
/// Use the `arrow_cast` function to cast to a specific arrow type
///
/// For example
/// ```sql
/// select arrow_cast(column_x, 'Float64')
/// ```
#[user_doc(
    doc_section(label = "Other Functions"),
    description = "Casts a value to a specific Arrow data type.",
    syntax_example = "arrow_cast(expression, datatype)",
    sql_example = r#"```sql
> select
  arrow_cast(-5,    'Int8') as a,
  arrow_cast('foo', 'Dictionary(Int32, Utf8)') as b,
  arrow_cast('bar', 'LargeUtf8') as c;

+----+-----+-----+
| a  | b   | c   |
+----+-----+-----+
| -5 | foo | bar |
+----+-----+-----+

> select
  arrow_cast('2023-01-02T12:53:02', 'Timestamp(µs, "+08:00")') as d,
  arrow_cast('2023-01-02T12:53:02', 'Timestamp(µs)') as e;

+---------------------------+---------------------+
| d                         | e                   |
+---------------------------+---------------------+
| 2023-01-02T12:53:02+08:00 | 2023-01-02T12:53:02 |
+---------------------------+---------------------+
```"#,
    argument(
        name = "expression",
        description = "Expression to cast. The expression can be a constant, column, or function, and any combination of operators."
    ),
    argument(
        name = "datatype",
        description = "[Arrow data type](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html) name to cast to, as a string. The format is the same as that returned by [`arrow_typeof`]"
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrowCastFunc {
    signature: Signature,
}

impl Default for ArrowCastFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrowCastFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrowCastFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "arrow_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());

        let [_, type_arg] = take_function_args(self.name(), args.scalar_arguments)?;

        type_arg
            .and_then(|sv| sv.try_as_str().flatten().filter(|s| !s.is_empty()))
            .map_or_else(
                || {
                    exec_err!(
                        "{} requires its second argument to be a non-empty constant string",
                        self.name()
                    )
                },
                |casted_type| match casted_type.parse::<DataType>() {
                    Ok(data_type) => Ok(Field::new(self.name(), data_type, nullable).into()),
                    Err(ArrowError::ParseError(e)) => Err(exec_datafusion_err!("{e}")),
                    Err(e) => Err(arrow_datafusion_err!(e)),
                },
            )
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("arrow_cast should have been simplified to cast")
    }

    fn simplify(
        &self,
        mut args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        // convert this into a real cast
        let target_type = data_type_from_args(&args)?;
        // remove second (type) argument
        args.pop().unwrap();
        let arg = args.pop().unwrap();

        let source_type = info.get_data_type(&arg)?;
        let new_expr = if source_type == target_type {
            // the argument's data type is already the correct type
            arg
        } else {
            // Use an actual cast to get the correct type
            Expr::Cast(datafusion_expr::Cast {
                expr: Box::new(arg),
                data_type: target_type,
            })
        };
        // return the newly written argument to DataFusion
        Ok(ExprSimplifyResult::Simplified(new_expr))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Returns the requested type from the arguments
fn data_type_from_args(args: &[Expr]) -> Result<DataType> {
    let [_, type_arg] = take_function_args("arrow_cast", args)?;

    let Expr::Literal(ScalarValue::Utf8(Some(val)), _) = type_arg else {
        return exec_err!(
            "arrow_cast requires its second argument to be a constant string, got {:?}",
            type_arg
        );
    };

    val.parse().map_err(|e| match e {
        // If the data type cannot be parsed, return a Plan error to signal an
        // error in the input rather than a more general ArrowError
        ArrowError::ParseError(e) => exec_datafusion_err!("{e}"),
        e => arrow_datafusion_err!(e),
    })
}
