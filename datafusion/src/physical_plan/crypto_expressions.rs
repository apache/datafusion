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

//! Crypto expressions
use super::ColumnarValue;
use crate::{
    error::{DataFusionError, Result},
    scalar::ScalarValue,
};
use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, GenericStringArray, StringArray,
        StringOffsetSizeTrait,
    },
    datatypes::DataType,
};
use blake2::{Blake2b, Blake2s, Digest};
use blake3::Hasher as Blake3;
use md5::Md5;
use sha2::{Sha224, Sha256, Sha384, Sha512};
use std::any::type_name;
use std::fmt::Write;
use std::sync::Arc;
use std::{fmt, str::FromStr};

/// Digest algorithms.
///
/// Note that by default all digest algorithms return BinaryArray or Binary scalar data.
/// However md5 when invoked by its name (rather than digest(value, 'md5')) would return
/// hex encoded utf8 values, due to historical reasons. You are advised to prefer to use
/// digest(utf8, utf8) function.
#[derive(Debug, Copy, Clone)]
enum DigestAlgorithm {
    Md5,
    Sha224,
    Sha256,
    Sha384,
    Sha512,
    Blake2s,
    Blake2b,
    Blake3,
}

fn digest_process(
    value: &ColumnarValue,
    digest_algorithm: DigestAlgorithm,
) -> Result<ColumnarValue> {
    match value {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8 => digest_algorithm.digest_array::<i32>(a.as_ref()),
            DataType::LargeUtf8 => digest_algorithm.digest_array::<i64>(a.as_ref()),
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function {}",
                other, digest_algorithm,
            ))),
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(a) | ScalarValue::LargeUtf8(a) => {
                Ok(digest_algorithm.digest_scalar(a))
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function {}",
                other, digest_algorithm,
            ))),
        },
    }
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

macro_rules! digest_to_scalar {
    ($METHOD: ident, $INPUT:expr) => {{
        ScalarValue::Binary($INPUT.as_ref().map(|v| {
            let mut digest = $METHOD::default();
            digest.update(v);
            digest.finalize().as_slice().to_vec()
        }))
    }};
}

impl DigestAlgorithm {
    /// digest an optional string to its hash value, null values are returned as is
    fn digest_scalar(self, value: &Option<String>) -> ColumnarValue {
        ColumnarValue::Scalar(match self {
            Self::Md5 => digest_to_scalar!(Md5, value),
            Self::Sha224 => digest_to_scalar!(Sha224, value),
            Self::Sha256 => digest_to_scalar!(Sha256, value),
            Self::Sha384 => digest_to_scalar!(Sha384, value),
            Self::Sha512 => digest_to_scalar!(Sha512, value),
            Self::Blake2b => digest_to_scalar!(Blake2b, value),
            Self::Blake2s => digest_to_scalar!(Blake2s, value),
            Self::Blake3 => ScalarValue::Binary(value.as_ref().map(|v| {
                let mut digest = Blake3::default();
                digest.update(v.as_bytes());
                digest.finalize().as_bytes().to_vec()
            })),
        })
    }

    /// digest a string array to their hash values
    fn digest_array<T>(self, value: &dyn Array) -> Result<ColumnarValue>
    where
        T: StringOffsetSizeTrait,
    {
        let input_value = value
            .as_any()
            .downcast_ref::<GenericStringArray<T>>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast value to {}",
                    type_name::<GenericStringArray<T>>()
                ))
            })?;
        let array: ArrayRef = match self {
            Self::Md5 => digest_to_array!(Md5, input_value),
            Self::Sha224 => digest_to_array!(Sha224, input_value),
            Self::Sha256 => digest_to_array!(Sha256, input_value),
            Self::Sha384 => digest_to_array!(Sha384, input_value),
            Self::Sha512 => digest_to_array!(Sha512, input_value),
            Self::Blake2b => digest_to_array!(Blake2b, input_value),
            Self::Blake2s => digest_to_array!(Blake2s, input_value),
            Self::Blake3 => {
                let binary_array: BinaryArray = input_value
                    .iter()
                    .map(|opt| {
                        opt.map(|x| {
                            let mut digest = Blake3::default();
                            digest.update(x.as_bytes());
                            digest.finalize().as_bytes().to_vec()
                        })
                    })
                    .collect();
                Arc::new(binary_array)
            }
        };
        Ok(ColumnarValue::Array(array))
    }
}

impl fmt::Display for DigestAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
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
                return Err(DataFusionError::Plan(format!(
                    "There is no built-in digest algorithm named '{}', currently supported algorithms are: {}",
                    name,
                    options,
                )));
            }
        })
    }
}

macro_rules! define_digest_function {
    ($NAME: ident, $METHOD: ident, $DOC: expr) => {
        #[doc = $DOC]
        pub fn $NAME(args: &[ColumnarValue]) -> Result<ColumnarValue> {
            if args.len() != 1 {
                return Err(DataFusionError::Internal(format!(
                    "{:?} args were supplied but {} takes exactly one argument",
                    args.len(),
                    DigestAlgorithm::$METHOD.to_string(),
                )));
            }
            digest_process(&args[0], DigestAlgorithm::$METHOD)
        }
    };
}

/// this function exists so that we do not need to pull in the crate hex. it is only used by md5
/// function below
#[inline]
fn hex_encode<T: AsRef<[u8]>>(data: T) -> String {
    let mut s = String::with_capacity(data.as_ref().len() * 2);
    for b in data.as_ref() {
        // Writing to a string never errors, so we can unwrap here.
        write!(&mut s, "{:02x}", b).unwrap();
    }
    s
}

/// computes md5 hash digest of the given input
pub fn md5(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 1 {
        return Err(DataFusionError::Internal(format!(
            "{:?} args were supplied but {} takes exactly one argument",
            args.len(),
            DigestAlgorithm::Md5,
        )));
    }
    let value = digest_process(&args[0], DigestAlgorithm::Md5)?;
    // md5 requires special handling because of its unique utf8 return type
    Ok(match value {
        ColumnarValue::Array(array) => {
            let binary_array = array
                .as_ref()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Impossibly got non-binary array data from digest".into(),
                    )
                })?;
            let string_array: StringArray = binary_array
                .iter()
                .map(|opt| opt.map(hex_encode::<_>))
                .collect();
            ColumnarValue::Array(Arc::new(string_array))
        }
        ColumnarValue::Scalar(ScalarValue::Binary(opt)) => {
            ColumnarValue::Scalar(ScalarValue::Utf8(opt.map(hex_encode::<_>)))
        }
        _ => {
            return Err(DataFusionError::Internal(
                "Impossibly got invalid results from digest".into(),
            ))
        }
    })
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

/// Digest computes a binary hash of the given data, accepts Utf8 or LargeUtf8 and returns a [`ColumnarValue`].
/// Second argument is the algorithm to use.
/// Standard algorithms are md5, sha1, sha224, sha256, sha384 and sha512.
pub fn digest(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Internal(format!(
            "{:?} args were supplied but digest takes exactly two arguments",
            args.len(),
        )));
    }
    let digest_algorithm = match &args[1] {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(Some(method)) | ScalarValue::LargeUtf8(Some(method)) => {
                method.parse::<DigestAlgorithm>()
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function digest",
                other,
            ))),
        },
        ColumnarValue::Array(_) => Err(DataFusionError::Internal(
            "Digest using dynamically decided method is not yet supported".into(),
        )),
    }?;
    digest_process(&args[0], digest_algorithm)
}
