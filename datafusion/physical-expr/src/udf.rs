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

//! UDF support
use crate::{PhysicalExpr, ScalarFunctionExpr};
use arrow_schema::DataType;
use datafusion_common::Result;
pub use datafusion_expr::ScalarUDF;
use std::sync::Arc;

/// Create a physical expression of the UDF.
///
/// Arguments:
pub fn create_physical_expr(
    fun: &ScalarUDF,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    return_type: DataType,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(ScalarFunctionExpr::new(
        fun.name(),
        fun.fun(),
        input_phy_exprs.to_vec(),
        return_type,
        fun.monotonicity()?,
        fun.signature().type_signature.supports_zero_argument(),
    )))
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;
    use datafusion_common::Result;
    use datafusion_expr::{
        ColumnarValue, FuncMonotonicity, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    use crate::ScalarFunctionExpr;

    use super::create_physical_expr;

    #[test]
    fn test_functions() -> Result<()> {
        #[derive(Debug, Clone)]
        struct TestScalarUDF {
            signature: Signature,
        }

        impl TestScalarUDF {
            fn new() -> Self {
                let signature =
                    Signature::exact(vec![DataType::Float64], Volatility::Immutable);

                Self { signature }
            }
        }

        impl ScalarUDFImpl for TestScalarUDF {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn name(&self) -> &str {
                "my_fn"
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(DataType::Float64)
            }

            fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
                unimplemented!("my_fn is not implemented")
            }

            fn monotonicity(&self) -> Result<Option<FuncMonotonicity>> {
                Ok(Some(vec![Some(true)]))
            }
        }

        // create and register the udf
        let udf = ScalarUDF::from(TestScalarUDF::new());

        let p_expr = create_physical_expr(&udf, &[], DataType::Float64)?;

        assert_eq!(
            p_expr
                .as_any()
                .downcast_ref::<ScalarFunctionExpr>()
                .unwrap()
                .monotonicity(),
            &Some(vec![Some(true)])
        );

        Ok(())
    }
}
