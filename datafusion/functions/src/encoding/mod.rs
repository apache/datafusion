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

mod inner;

use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::{plan_err, DataFusionError, Result};
use datafusion_expr::TypeSignature::*;
use datafusion_expr::{
    ColumnarValue, FunctionImplementation, ScalarUDF, Signature, Volatility,
};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use DataType::*;

// TODO make stub implementations when feature is not activated
//#[cfg(feature = "encoding_expressions")]
//pub mod encoding_expressions;

pub fn encode_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(Arc::new(EncodeFunc {}))
}

pub fn decode_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(Arc::new(DecodeFunc {}))
}

fn insert(registry: &mut HashMap<String, Arc<ScalarUDF>>, udf: ScalarUDF) {
    registry.insert(udf.name().to_string(), Arc::new(udf));
}

/// Registers the `encode` and `decode` functions with the function registry
pub fn register(registry: &mut HashMap<String, Arc<ScalarUDF>>) {
    insert(registry, encode_udf());
    insert(registry, decode_udf());
}

struct EncodeFunc {}

static ENCODE_SIGNATURE: OnceLock<Signature> = OnceLock::new();

impl FunctionImplementation for EncodeFunc {
    fn name(&self) -> &str {
        "encode"
    }

    fn signature(&self) -> &Signature {
        ENCODE_SIGNATURE.get_or_init(|| {
            Signature::one_of(
                vec![
                    Exact(vec![Utf8, Utf8]),
                    Exact(vec![LargeUtf8, Utf8]),
                    Exact(vec![Binary, Utf8]),
                    Exact(vec![LargeBinary, Utf8]),
                ],
                Volatility::Immutable,
            )
        })
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match arg_types[0] {
            Utf8 => Utf8,
            LargeUtf8 => LargeUtf8,
            Binary => Utf8,
            LargeBinary => LargeUtf8,
            Null => Null,
            _ => {
                return plan_err!("The encode function can only accept utf8 or binary.");
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        // Put a feature flag here to make sure this is only compiled when the feature is activated
        inner::encode(args)
    }
}

struct DecodeFunc {}

static DECODE_SIGNATURE: OnceLock<Signature> = OnceLock::new();

impl FunctionImplementation for DecodeFunc {
    fn name(&self) -> &str {
        "decode"
    }

    fn signature(&self) -> &Signature {
        DECODE_SIGNATURE.get_or_init(|| {
            Signature::one_of(
                vec![
                    Exact(vec![Utf8, Utf8]),
                    Exact(vec![LargeUtf8, Utf8]),
                    Exact(vec![Binary, Utf8]),
                    Exact(vec![LargeBinary, Utf8]),
                ],
                Volatility::Immutable,
            )
        })
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match arg_types[0] {
            Utf8 => Binary,
            LargeUtf8 => LargeBinary,
            Binary => Binary,
            LargeBinary => LargeBinary,
            Null => Null,
            _ => {
                return plan_err!("The decode function can only accept utf8 or binary.");
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        // Put a feature flag here to make sure this is only compiled when the feature is activated
        inner::decode(args)
    }
}
