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
    fn equals(&self, other: &dyn ScalarUDFImpl) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<ParamUdf>() {
            self.param == other.param && self.type_id() == other.type_id()
        } else {
            false
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct SignatureUdf {
    signature: Signature,
}

impl SignatureUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Int32], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SignatureUdf {
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
    fn equals(&self, other: &dyn ScalarUDFImpl) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<SignatureUdf>() {
            self.type_id() == other.type_id()
        } else {
            false
        }
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
    let udf2 = ScalarUDF::from(SignatureUdf::new());
    assert_ne!(udf1, udf2);
}

#[test]
fn same_state_equal() {
    let udf1 = ScalarUDF::from(ParamUdf::new(1));
    let udf2 = ScalarUDF::from(ParamUdf::new(1));
    assert_eq!(udf1, udf2);
}

#[test]
fn same_types_equal() {
    let udf1 = ScalarUDF::from(SignatureUdf::new());
    let udf2 = ScalarUDF::from(SignatureUdf::new());
    assert_eq!(udf1, udf2);
}
