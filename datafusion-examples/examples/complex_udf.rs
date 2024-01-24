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

use std::any::Any;

use arrow_schema::{Field, Schema};
use datafusion::{arrow::datatypes::DataType, logical_expr::Volatility};

use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::{
    internal_err, DFSchema, DataFusionError, ScalarValue, ToDFSchema,
};
use datafusion_expr::{
    expr::ScalarFunction, ColumnarValue, ExprSchemable, ScalarUDF, ScalarUDFImpl,
    Signature,
};

#[derive(Debug)]
struct UDFWithExprReturn {
    signature: Signature,
}

impl UDFWithExprReturn {
    fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

//Implement the ScalarUDFImpl trait for UDFWithExprReturn
impl ScalarUDFImpl for UDFWithExprReturn {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "udf_with_expr_return"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }
    // An example of how to use the exprs to determine the return type
    // If the third argument is '0', return the type of the first argument
    // If the third argument is '1', return the type of the second argument
    fn return_type_from_exprs(
        &self,
        arg_exprs: &[Expr],
        schema: &DFSchema,
    ) -> Result<DataType> {
        if arg_exprs.len() != 3 {
            return internal_err!("The size of the args must be 3.");
        }
        let take_idx = match arg_exprs.get(2).unwrap() {
            Expr::Literal(ScalarValue::Int64(Some(idx))) if (idx == &0 || idx == &1) => {
                *idx as usize
            }
            _ => unreachable!(),
        };
        arg_exprs.get(take_idx).unwrap().get_type(schema)
    }
    // The actual implementation would add one to the argument
    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
        unimplemented!()
    }
}

#[derive(Debug)]
struct UDFDefault {
    signature: Signature,
}

impl UDFDefault {
    fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

// Implement the ScalarUDFImpl trait for UDFDefault
// This is the same as UDFWithExprReturn, except without return_type_from_exprs
impl ScalarUDFImpl for UDFDefault {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "udf_default"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    // The actual implementation would add one to the argument
    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
        unimplemented!()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Create a new ScalarUDF from the implementation
    let udf_with_expr_return = ScalarUDF::from(UDFWithExprReturn::new());

    // Call 'return_type' to get the return type of the function
    let ret = udf_with_expr_return.return_type(&[DataType::Int32])?;
    assert_eq!(ret, DataType::Int32);

    let schema = Schema::new(vec![
        Field::new("a", DataType::Float32, false),
        Field::new("b", DataType::Float64, false),
    ])
    .to_dfschema()?;

    // Set the third argument to 0 to return the type of the first argument
    let expr0 = udf_with_expr_return.call(vec![col("a"), col("b"), lit(0_i64)]);
    let args = match expr0 {
        Expr::ScalarFunction(ScalarFunction { func_def: _, args }) => args,
        _ => panic!("Expected ScalarFunction"),
    };
    let ret = udf_with_expr_return.return_type_from_exprs(&args, &schema)?;
    // The return type should be the same as the first argument
    assert_eq!(ret, DataType::Float32);

    // Set the third argument to 1 to return the type of the second argument
    let expr1 = udf_with_expr_return.call(vec![col("a"), col("b"), lit(1_i64)]);
    let args1 = match expr1 {
        Expr::ScalarFunction(ScalarFunction { func_def: _, args }) => args,
        _ => panic!("Expected ScalarFunction"),
    };
    let ret = udf_with_expr_return.return_type_from_exprs(&args1, &schema)?;
    // The return type should be the same as the second argument
    assert_eq!(ret, DataType::Float64);

    // Create a new ScalarUDF from the implementation
    let udf_default = ScalarUDF::from(UDFDefault::new());
    // Call 'return_type' to get the return type of the function
    let ret = udf_default.return_type(&[DataType::Int32])?;
    assert_eq!(ret, DataType::Boolean);

    // Set the third argument to 0 to return the type of the first argument
    let expr2 = udf_default.call(vec![col("a"), col("b"), lit(0_i64)]);
    let args = match expr2 {
        Expr::ScalarFunction(ScalarFunction { func_def: _, args }) => args,
        _ => panic!("Expected ScalarFunction"),
    };
    let ret = udf_default.return_type_from_exprs(&args, &schema)?;
    assert_eq!(ret, DataType::Boolean);

    Ok(())
}
