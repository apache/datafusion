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

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryArrayType, BinaryViewArray, GenericBinaryArray,
    OffsetSizeTrait,
};
use arrow::array::{AsArray, GenericStringArray, StringArray, StringViewArray};
use arrow::datatypes::DataType;
use blake2::{Blake2b512, Blake2s256, Digest};
use blake3::Hasher as Blake3;
use datafusion_common::cast::as_binary_array;

use arrow::compute::StringArrayType;
use datafusion_common::{
    exec_err, internal_err, plan_err, utils::take_function_args, DataFusionError, Result,
    ScalarValue,
};
use datafusion_expr::ColumnarValue;
use md5::Md5;
use sha2::{Sha224, Sha256, Sha384, Sha512};
use std::fmt::{self, Write};
use std::str::FromStr;
use std::sync::Arc;

macro_rules! define_digest_function {
    ($NAME: ident, $METHOD: ident, $DOC: expr) => {
        #[doc = $DOC]
        pub fn $NAME(args: &[ColumnarValue]) -> Result<ColumnarValue> {
            let [data] = take_function_args(&DigestAlgorithm::$METHOD.to_string(), args)?;
            digest_process(data, DigestAlgorithm::$METHOD)
        }
    };
}
define_digest_function!(
    sha224,
    Sha224,
    "computes sha224 hash digest of the given input"
);
define_digest_function!(
    sha256,
    Sha256,
    "computes sha256 hash digest of the given input"
);
define_digest_function!(
    sha384,
    Sha384,
    "computes sha384 hash digest of the given input"
);
define_digest_function!(
    sha512,
    Sha512,
    "computes sha512 hash digest of the given input"
);
define_digest_function!(
    blake2b,
    Blake2b,
    "computes blake2b hash digest of the given input"
);
define_digest_function!(
    blake2s,
    Blake2s,
    "computes blake2s hash digest of the given input"
);
define_digest_function!(
    blake3,
    Blake3,
    "computes blake3 hash digest of the given input"
);

macro_rules! digest_to_scalar {
    ($METHOD: ident, $INPUT:expr) => {{
        ScalarValue::Binary($INPUT.as_ref().map(|v| {
            let mut digest = $METHOD::default();
            digest.update(v);
            digest.finalize().as_slice().to_vec()
        }))
    }};
}

#[derive(Debug, Copy, Clone)]
pub enum DigestAlgorithm {
    Md5,
    Sha224,
    Sha256,
    Sha384,
    Sha512,
    Blake2s,
    Blake2b,
    Blake3,
}

/// Digest computes a binary hash of the given data, accepts Utf8 or LargeUtf8 and returns a [`ColumnarValue`].
/// Second argument is the algorithm to use.
/// Standard algorithms are md5, sha1, sha224, sha256, sha384 and sha512.
pub fn digest(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let [data, digest_algorithm] = take_function_args("digest", args)?;
    let digest_algorithm = match digest_algorithm {
        ColumnarValue::Scalar(scalar) => match scalar.try_as_str() {
            Some(Some(method)) => method.parse::<DigestAlgorithm>(),
            _ => exec_err!("Unsupported data type {scalar:?} for function digest"),
        },
        ColumnarValue::Array(_) => {
            internal_err!("Digest using dynamically decided method is not yet supported")
        }
    }?;
    digest_process(data, digest_algorithm)
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

/// computes md5 hash digest of the given input
pub fn md5(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let [data] = take_function_args("md5", args)?;
    let value = digest_process(data, DigestAlgorithm::Md5)?;

    // md5 requires special handling because of its unique utf8 return type
    Ok(match value {
        ColumnarValue::Array(array) => {
            let binary_array = as_binary_array(&array)?;
            let string_array: StringArray = binary_array
                .iter()
                .map(|opt| opt.map(hex_encode::<_>))
                .collect();
            ColumnarValue::Array(Arc::new(string_array))
        }
        ColumnarValue::Scalar(ScalarValue::Binary(opt)) => {
            ColumnarValue::Scalar(ScalarValue::Utf8(opt.map(hex_encode::<_>)))
        }
        _ => return exec_err!("Impossibly got invalid results from digest"),
    })
}

/// this function exists so that we do not need to pull in the crate hex. it is only used by md5
/// function below
#[inline]
fn hex_encode<T: AsRef<[u8]>>(data: T) -> String {
    let mut s = String::with_capacity(data.as_ref().len() * 2);
    for b in data.as_ref() {
        // Writing to a string never errors, so we can unwrap here.
        write!(&mut s, "{b:02x}").unwrap();
    }
    s
}
pub fn utf8_or_binary_to_binary_type(
    arg_type: &DataType,
    name: &str,
) -> Result<DataType> {
    Ok(match arg_type {
        DataType::Utf8View
        | DataType::LargeUtf8
        | DataType::Utf8
        | DataType::Binary
        | DataType::BinaryView
        | DataType::LargeBinary => DataType::Binary,
        DataType::Null => DataType::Null,
        _ => {
            return plan_err!(
                "The {name:?} function can only accept strings or binary arrays."
            );
        }
    })
}
macro_rules! digest_to_array {
    ($METHOD:ident, $INPUT:expr) => {{
        let binary_array: BinaryArray = $INPUT
            .iter()
            .map(|x| {
                x.map(|x| {
                    let mut digest = $METHOD::default();
                    digest.update(x);
                    digest.finalize()
                })
            })
            .collect();
        Arc::new(binary_array)
    }};
}
impl DigestAlgorithm {
    /// digest an optional string to its hash value, null values are returned as is
    pub fn digest_scalar(self, value: Option<&[u8]>) -> ColumnarValue {
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

    /// digest a binary array to their hash values
    pub fn digest_binary_array<T>(self, value: &dyn Array) -> Result<ColumnarValue>
    where
        T: OffsetSizeTrait,
    {
        let array = match value.data_type() {
            DataType::Binary | DataType::LargeBinary => {
                let v = value.as_binary::<T>();
                self.digest_binary_array_impl::<&GenericBinaryArray<T>>(v)
            }
            DataType::BinaryView => {
                let v = value.as_binary_view();
                self.digest_binary_array_impl::<&BinaryViewArray>(v)
            }
            other => {
                return exec_err!("unsupported type for digest_utf_array: {other:?}")
            }
        };
        Ok(ColumnarValue::Array(array))
    }

    /// digest a string array to their hash values
    pub fn digest_utf8_array<T>(self, value: &dyn Array) -> Result<ColumnarValue>
    where
        T: OffsetSizeTrait,
    {
        let array = match value.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                let v = value.as_string::<T>();
                self.digest_utf8_array_impl::<&GenericStringArray<T>>(v)
            }
            DataType::Utf8View => {
                let v = value.as_string_view();
                self.digest_utf8_array_impl::<&StringViewArray>(v)
            }
            other => {
                return exec_err!("unsupported type for digest_utf_array: {other:?}")
            }
        };
        Ok(ColumnarValue::Array(array))
    }

    pub fn digest_utf8_array_impl<'a, StringArrType>(
        self,
        input_value: StringArrType,
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

    pub fn digest_binary_array_impl<'a, BinaryArrType>(
        self,
        input_value: BinaryArrType,
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
pub fn digest_process(
    value: &ColumnarValue,
    digest_algorithm: DigestAlgorithm,
) -> Result<ColumnarValue> {
    match value {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8View => digest_algorithm.digest_utf8_array::<i32>(a.as_ref()),
            DataType::Utf8 => digest_algorithm.digest_utf8_array::<i32>(a.as_ref()),
            DataType::LargeUtf8 => digest_algorithm.digest_utf8_array::<i64>(a.as_ref()),
            DataType::Binary => digest_algorithm.digest_binary_array::<i32>(a.as_ref()),
            DataType::LargeBinary => {
                digest_algorithm.digest_binary_array::<i64>(a.as_ref())
            }
            DataType::BinaryView => {
                digest_algorithm.digest_binary_array::<i32>(a.as_ref())
            }
            other => exec_err!(
                "Unsupported data type {other:?} for function {digest_algorithm}"
            ),
        },
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
