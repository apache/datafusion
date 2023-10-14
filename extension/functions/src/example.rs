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

use arrow::array::{ArrayRef, Float64Array};
use arrow::datatypes::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::Volatility;
use datafusion_common::cast::as_float64_array;
use datafusion_expr::{
    ReturnTypeFunction, ScalarFunctionDef, ScalarFunctionPackage, Signature,
};
use std::sync::Arc;

#[derive(Debug)]
pub struct AddOneFunction;

impl ScalarFunctionDef for AddOneFunction {
    fn name(&self) -> &str {
        "add_one"
    }

    fn signature(&self) -> Signature {
        Signature::exact(vec![DataType::Float64], Volatility::Immutable)
    }

    fn return_type(&self) -> ReturnTypeFunction {
        let return_type = Arc::new(DataType::Float64);
        Arc::new(move |_| Ok(return_type.clone()))
    }

    fn execute(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        assert_eq!(args.len(), 1);
        let input = as_float64_array(&args[0]).expect("cast failed");
        let array = input
            .iter()
            .map(|value| match value {
                Some(value) => Some(value + 1.0),
                _ => None,
            })
            .collect::<Float64Array>();
        Ok(Arc::new(array) as ArrayRef)
    }
}

#[derive(Debug)]
pub struct MultiplyTwoFunction;

impl ScalarFunctionDef for MultiplyTwoFunction {
    fn name(&self) -> &str {
        "multiply_two"
    }

    fn signature(&self) -> Signature {
        Signature::exact(vec![DataType::Float64], Volatility::Immutable)
    }

    fn return_type(&self) -> ReturnTypeFunction {
        let return_type = Arc::new(DataType::Float64);
        Arc::new(move |_| Ok(return_type.clone()))
    }

    fn execute(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        assert_eq!(args.len(), 1);
        let input = as_float64_array(&args[0]).expect("cast failed");
        let array = input
            .iter()
            .map(|value| match value {
                Some(value) => Some(value * 2.0),
                _ => None,
            })
            .collect::<Float64Array>();
        Ok(Arc::new(array) as ArrayRef)
    }
}

// Function package declaration
pub struct FunctionPackage;

impl ScalarFunctionPackage for FunctionPackage {
    fn functions(&self) -> Vec<Box<dyn ScalarFunctionDef>> {
        vec![Box::new(AddOneFunction), Box::new(MultiplyTwoFunction)]
    }
}

#[cfg(test)]
mod test {
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;
    use tokio;

    use crate::utils::{execute, test_expression};

    use super::FunctionPackage;

    #[tokio::test]
    async fn test_add_one() -> Result<()> {
        test_expression!("add_one(1)", "2.0");
        test_expression!("add_one(-1)", "0.0");
        Ok(())
    }

    #[tokio::test]
    async fn test_multiply_two() -> Result<()> {
        test_expression!("multiply_two(1)", "2.0");
        test_expression!("multiply_two(-1)", "-2.0");
        Ok(())
    }
}
