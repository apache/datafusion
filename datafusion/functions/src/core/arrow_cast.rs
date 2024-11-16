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

use arrow::datatypes::DataType;
use datafusion_common::{
    arrow_datafusion_err, internal_err, plan_datafusion_err, plan_err, DataFusionError,
    ExprSchema, Result, ScalarValue,
};
use std::any::Any;
use std::sync::OnceLock;

use datafusion_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ExprSchemable, ScalarUDFImpl, Signature,
    Volatility,
};

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
#[derive(Debug)]
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
        // should be using return_type_from_exprs and not calling the default
        // implementation
        internal_err!("arrow_cast should return type from exprs")
    }

    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        args.iter().any(|e| e.nullable(schema).ok().unwrap_or(true))
    }

    fn return_type_from_exprs(
        &self,
        args: &[Expr],
        _schema: &dyn ExprSchema,
        _arg_types: &[DataType],
    ) -> Result<DataType> {
        data_type_from_args(args)
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
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
        Some(get_arrow_cast_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_arrow_cast_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_OTHER)
            .with_description("Casts a value to a specific Arrow data type.")
            .with_syntax_example("arrow_cast(expression, datatype)")
            .with_sql_example(
                r#"```sql
> select arrow_cast(-5, 'Int8') as a,
  arrow_cast('foo', 'Dictionary(Int32, Utf8)') as b,
  arrow_cast('bar', 'LargeUtf8') as c,
  arrow_cast('2023-01-02T12:53:02', 'Timestamp(Microsecond, Some("+08:00"))') as d
  ;
+----+-----+-----+---------------------------+
| a  | b   | c   | d                         |
+----+-----+-----+---------------------------+
| -5 | foo | bar | 2023-01-02T12:53:02+08:00 |
+----+-----+-----+---------------------------+
```"#,
            )
            .with_argument("expression", "Expression to cast. The expression can be a constant, column, or function, and any combination of operators.")
            .with_argument("datatype", "[Arrow data type](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html) name to cast to, as a string. The format is the same as that returned by [`arrow_typeof`]")
            .build()
            .unwrap()
    })
}

/// Returns the requested type from the arguments
fn data_type_from_args(args: &[Expr]) -> Result<DataType> {
    if args.len() != 2 {
        return plan_err!("arrow_cast needs 2 arguments, {} provided", args.len());
    }
    let Expr::Literal(ScalarValue::Utf8(Some(val))) = &args[1] else {
        return plan_err!(
            "arrow_cast requires its second argument to be a constant string, got {:?}",
            &args[1]
        );
    };

    val.parse().map_err(|e| match e {
        // If the data type cannot be parsed, return a Plan error to signal an
        // error in the input rather than a more general ArrowError
        arrow::error::ArrowError::ParseError(e) => plan_datafusion_err!("{e}"),
        e => arrow_datafusion_err!(e),
    })
}
