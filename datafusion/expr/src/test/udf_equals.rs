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

use crate::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use arrow::datatypes::DataType;
use datafusion_common::{not_impl_err, Result};
use std::{
    any::Any,
    hash::{Hash, Hasher},
};
#[derive(Debug, PartialEq)]
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
            self == other
        } else {
            false
        }
    }
    fn hash_value(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.param.hash(&mut hasher);
        self.signature.hash(&mut hasher);
        hasher.finish()
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
        "signature_udf"
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

#[derive(Debug)]
#[allow(dead_code)]
struct DefaultParamUdf {
    param: i32,
    signature: Signature,
}

impl DefaultParamUdf {
    fn new(param: i32) -> Self {
        Self {
            param,
            signature: Signature::exact(vec![DataType::Int32], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DefaultParamUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "default_param_udf"
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

#[test]
fn default_udfs_with_same_param_not_equal() {
    let udf1 = ScalarUDF::from(DefaultParamUdf::new(1));
    let udf2 = ScalarUDF::from(DefaultParamUdf::new(1));
    assert_ne!(udf1, udf2);
}

#[test]
fn default_udfs_with_different_param_not_equal() {
    let udf1 = ScalarUDF::from(DefaultParamUdf::new(1));
    let udf2 = ScalarUDF::from(DefaultParamUdf::new(2));
    assert_ne!(udf1, udf2);
}
