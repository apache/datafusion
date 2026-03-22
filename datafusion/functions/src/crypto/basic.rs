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

//! "crypto" DataFusion functions

use arrow::array::{Array, ArrayRef, AsArray, BinaryArray, BinaryArrayType};
use arrow::datatypes::DataType;
use blake2::{Blake2b512, Blake2s256, Digest};
use blake3::Hasher as Blake3;

use arrow::compute::StringArrayType;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::ColumnarValue;
use md5::Md5;
use sha2::{Sha224, Sha256, Sha384, Sha512};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub(crate) enum DigestAlgorithm {
    Md5,
    Sha224,
    Sha256,
    Sha384,
    Sha512,
    Blake2s,
    Blake2b,
    Blake3,
}

impl FromStr for DigestAlgorithm {
    type Err = DataFusionError;
    fn from_str(name: &str) -> Result<DigestAlgorithm> {
        Ok(match name {
            "md5" => Self::Md5,
            "sha224" => Self::Sha224,
            "sha256" => Self::Sha256,
            "sha384" => Self::Sha384,
            "sha512" => Self::Sha512,
            "blake2b" => Self::Blake2b,
            "blake2s" => Self::Blake2s,
            "blake3" => Self::Blake3,
            _ => {
                let options = [
                    Self::Md5,
                    Self::Sha224,
                    Self::Sha256,
                    Self::Sha384,
                    Self::Sha512,
                    Self::Blake2s,
                    Self::Blake2b,
                    Self::Blake3,
                ]
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(", ");
                return plan_err!(
                    "There is no built-in digest algorithm named '{name}', currently supported algorithms are: {options}"
                );
            }
        })
    }
}

impl fmt::Display for DigestAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", format!("{self:?}").to_lowercase())
    }
}

macro_rules! digest_to_array {
    ($METHOD:ident, $INPUT:expr) => {{
        let binary_array: BinaryArray = $INPUT
            .iter()
            .map(|x| x.map(|x| $METHOD::digest(x)))
            .collect();
        Arc::new(binary_array)
    }};
}

macro_rules! digest_to_scalar {
    ($METHOD: ident, $INPUT:expr) => {{ ScalarValue::Binary($INPUT.map(|v| $METHOD::digest(v).as_slice().to_vec())) }};
}

impl DigestAlgorithm {
    /// digest an optional string to its hash value, null values are returned as is
    fn digest_scalar(self, value: Option<&[u8]>) -> ColumnarValue {
        ColumnarValue::Scalar(match self {
            Self::Md5 => digest_to_scalar!(Md5, value),
            Self::Sha224 => digest_to_scalar!(Sha224, value),
            Self::Sha256 => digest_to_scalar!(Sha256, value),
            Self::Sha384 => digest_to_scalar!(Sha384, value),
            Self::Sha512 => digest_to_scalar!(Sha512, value),
            Self::Blake2b => digest_to_scalar!(Blake2b512, value),
            Self::Blake2s => digest_to_scalar!(Blake2s256, value),
            Self::Blake3 => ScalarValue::Binary(value.map(|v| {
                let mut digest = Blake3::default();
                digest.update(v);
                Blake3::finalize(&digest).as_bytes().to_vec()
            })),
        })
    }

    fn digest_utf8_array_impl<'a, StringArrType>(
        self,
        input_value: &StringArrType,
    ) -> ArrayRef
    where
        StringArrType: StringArrayType<'a>,
    {
        match self {
            Self::Md5 => digest_to_array!(Md5, input_value),
            Self::Sha224 => digest_to_array!(Sha224, input_value),
            Self::Sha256 => digest_to_array!(Sha256, input_value),
            Self::Sha384 => digest_to_array!(Sha384, input_value),
            Self::Sha512 => digest_to_array!(Sha512, input_value),
            Self::Blake2b => digest_to_array!(Blake2b512, input_value),
            Self::Blake2s => digest_to_array!(Blake2s256, input_value),
            Self::Blake3 => {
                let binary_array: BinaryArray = input_value
                    .iter()
                    .map(|opt| {
                        opt.map(|x| {
                            let mut digest = Blake3::default();
                            digest.update(x.as_bytes());
                            Blake3::finalize(&digest).as_bytes().to_vec()
                        })
                    })
                    .collect();
                Arc::new(binary_array)
            }
        }
    }

    fn digest_binary_array_impl<'a, BinaryArrType>(
        self,
        input_value: &BinaryArrType,
    ) -> ArrayRef
    where
        BinaryArrType: BinaryArrayType<'a>,
    {
        match self {
            Self::Md5 => digest_to_array!(Md5, input_value),
            Self::Sha224 => digest_to_array!(Sha224, input_value),
            Self::Sha256 => digest_to_array!(Sha256, input_value),
            Self::Sha384 => digest_to_array!(Sha384, input_value),
            Self::Sha512 => digest_to_array!(Sha512, input_value),
            Self::Blake2b => digest_to_array!(Blake2b512, input_value),
            Self::Blake2s => digest_to_array!(Blake2s256, input_value),
            Self::Blake3 => {
                let binary_array: BinaryArray = input_value
                    .iter()
                    .map(|opt| {
                        opt.map(|x| {
                            let mut digest = Blake3::default();
                            digest.update(x);
                            Blake3::finalize(&digest).as_bytes().to_vec()
                        })
                    })
                    .collect();
                Arc::new(binary_array)
            }
        }
    }
}

pub(crate) fn digest_process(
    value: &ColumnarValue,
    digest_algorithm: DigestAlgorithm,
) -> Result<ColumnarValue> {
    match value {
        ColumnarValue::Array(a) => {
            let output = match a.data_type() {
                DataType::Utf8View => {
                    digest_algorithm.digest_utf8_array_impl(&a.as_string_view())
                }
                DataType::Utf8 => {
                    digest_algorithm.digest_utf8_array_impl(&a.as_string::<i32>())
                }
                DataType::LargeUtf8 => {
                    digest_algorithm.digest_utf8_array_impl(&a.as_string::<i64>())
                }
                DataType::Binary => {
                    digest_algorithm.digest_binary_array_impl(&a.as_binary::<i32>())
                }
                DataType::LargeBinary => {
                    digest_algorithm.digest_binary_array_impl(&a.as_binary::<i64>())
                }
                DataType::BinaryView => {
                    digest_algorithm.digest_binary_array_impl(&a.as_binary_view())
                }
                other => {
                    return exec_err!(
                        "Unsupported data type {other:?} for function {digest_algorithm}"
                    );
                }
            };
            Ok(ColumnarValue::Array(output))
        }
        ColumnarValue::Scalar(scalar) => {
            match scalar {
                ScalarValue::Utf8View(a)
                | ScalarValue::Utf8(a)
                | ScalarValue::LargeUtf8(a) => Ok(digest_algorithm
                    .digest_scalar(a.as_ref().map(|s: &String| s.as_bytes()))),
                ScalarValue::Binary(a)
                | ScalarValue::LargeBinary(a)
                | ScalarValue::BinaryView(a) => Ok(digest_algorithm
                    .digest_scalar(a.as_ref().map(|v: &Vec<u8>| v.as_slice()))),
                other => exec_err!(
                    "Unsupported data type {other:?} for function {digest_algorithm}"
                ),
            }
        }
    }
}
