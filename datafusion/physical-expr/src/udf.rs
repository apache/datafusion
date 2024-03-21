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
use arrow_schema::Schema;
use datafusion_common::{DFSchema, Result};
pub use datafusion_expr::ScalarUDF;
use datafusion_expr::{
    type_coercion::functions::data_types, Expr, ScalarFunctionDefinition,
};
use std::sync::Arc;

/// Create a physical expression of the UDF.
///
/// Arguments:
pub fn create_physical_expr(
    fun: &ScalarUDF,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    args: &[Expr],
    input_dfschema: &DFSchema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let input_expr_types = input_phy_exprs
        .iter()
        .map(|e| e.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;

    // verify that input data types is consistent with function's `TypeSignature`
    data_types(&input_expr_types, fun.signature())?;

    // Since we have arg_types, we dont need args and schema.
    let return_type =
        fun.return_type_from_exprs(args, input_dfschema, &input_expr_types)?;

    let fun_def = ScalarFunctionDefinition::UDF(Arc::new(fun.clone()));
    Ok(Arc::new(ScalarFunctionExpr::new(
        fun.name(),
        fun_def,
        input_phy_exprs.to_vec(),
        return_type,
        fun.monotonicity()?,
        fun.signature().type_signature.supports_zero_argument(),
    )))
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Schema};
    use datafusion_common::{DFSchema, Result};
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

        let e = crate::expressions::lit(1.1);
        let p_expr =
            create_physical_expr(&udf, &[e], &Schema::empty(), &[], &DFSchema::empty())?;

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
