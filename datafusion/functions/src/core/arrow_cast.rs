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

use std::any::Any;

use arrow::datatypes::DataType;
use datafusion_common::{
    arrow_datafusion_err, internal_err, plan_datafusion_err, plan_err, DataFusionError,
    ExprSchema, Result, ScalarValue,
};

use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{ColumnarValue, Expr, ScalarUDFImpl, Signature, Volatility};

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
