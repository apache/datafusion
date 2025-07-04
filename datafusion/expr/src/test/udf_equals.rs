use crate::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use arrow::datatypes::DataType;
use datafusion_common::{not_impl_err, Result};
use std::any::Any;

#[derive(Debug)]
#[allow(dead_code)]
struct ParamUdf {
    param: i32,
    signature: Signature,
}

impl ParamUdf {
    fn new(param: i32) -> Self {
        Self {
            param,
            signature: Signature::exact(vec![DataType::Int32], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ParamUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "param_udf"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!("not used")
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct AnotherParamUdf {
    signature: Signature,
}

impl AnotherParamUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Int32], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for AnotherParamUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "param_udf"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!("not used")
    }
}

#[test]
fn different_instances_not_equal() {
    let udf1 = ScalarUDF::from(ParamUdf::new(1));
    let udf2 = ScalarUDF::from(ParamUdf::new(2));
    assert_ne!(udf1, udf2);
}

#[test]
fn different_types_not_equal() {
    let udf1 = ScalarUDF::from(ParamUdf::new(1));
    let udf2 = ScalarUDF::from(AnotherParamUdf::new());
    assert_ne!(udf1, udf2);
}

#[test]
fn same_instances_equal() {
    let udf1 = ScalarUDF::from(ParamUdf::new(1));
    let udf2 = ScalarUDF::from(ParamUdf::new(1));
    assert_eq!(udf1, udf2);
}

#[test]
fn same_types_equal() {
    let udf1 = ScalarUDF::from(AnotherParamUdf::new());
    let udf2 = ScalarUDF::from(AnotherParamUdf::new());
    assert_eq!(udf1, udf2);
}