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

use datafusion_expr::ScalarUDF;
use std::sync::Arc;

pub mod basic;
pub mod digest;
pub mod md5;
pub mod sha;
make_udf_function!(digest::DigestFunc, digest);
make_udf_function!(md5::Md5Func, md5);
make_udf_function!(sha::SHAFunc, sha224, sha::SHAFunc::sha224);
make_udf_function!(sha::SHAFunc, sha256, sha::SHAFunc::sha256);
make_udf_function!(sha::SHAFunc, sha384, sha::SHAFunc::sha384);
make_udf_function!(sha::SHAFunc, sha512, sha::SHAFunc::sha512);

#[cfg(test)]
mod strict_tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{
        ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    };
    use std::sync::Arc;

    #[test]
    fn strict_crypto_functions_propagate_nulls() {
        let cases = vec![
            (md5(), vec![ScalarValue::from("foo")]),
            (sha224(), vec![ScalarValue::from("foo")]),
            (sha256(), vec![ScalarValue::from("foo")]),
            (sha384(), vec![ScalarValue::from("foo")]),
            (sha512(), vec![ScalarValue::from("foo")]),
        ];

        for (func, valid_args) in cases {
            assert!(func.is_strict(), "{} should be marked strict", func.name());

            for null_mask in 0..(1 << valid_args.len()) {
                let mut args = valid_args.clone();
                for (arg_idx, arg) in args.iter_mut().enumerate() {
                    if null_mask & (1 << arg_idx) != 0 {
                        *arg = ScalarValue::try_new_null(&arg.data_type()).unwrap();
                    }
                }

                let result =
                    invoke_with_scalar_args(&func, args).unwrap_or_else(|error| {
                        panic!(
                            "{} failed for NULL mask {null_mask:b}: {error}",
                            func.name()
                        )
                    });
                let expected_null = null_mask != 0;
                let result = result.into_array(1).unwrap();
                assert_eq!(
                    result.null_count() == result.len(),
                    expected_null,
                    "{} returned {result:?} for NULL mask {null_mask:0width$b}",
                    func.name(),
                    width = valid_args.len(),
                );
            }
        }
    }

    fn invoke_with_scalar_args(
        func: &ScalarUDF,
        args: Vec<ScalarValue>,
    ) -> Result<ColumnarValue> {
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Arc::new(Field::new(
                    format!("arg_{idx}"),
                    arg.data_type(),
                    arg.is_null(),
                ))
            })
            .collect::<Vec<_>>();
        let scalar_arguments = args.iter().map(Some).collect::<Vec<_>>();
        let return_field = func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        })?;
        func.invoke_with_args(ScalarFunctionArgs {
            args: args.into_iter().map(ColumnarValue::Scalar).collect(),
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Arc::new(Default::default()),
        })
    }
}

pub mod expr_fn {
    export_functions!((
        digest,
        "Computes the binary hash of an expression using the specified algorithm.",
        input_arg1 input_arg2
    ),(
        md5,
        "Computes an MD5 128-bit checksum for a string expression.",
        input_arg
    ),(
        sha224,
        "Computes the SHA-224 hash of a binary string.",
        input_arg1
    ),(
        sha256,
        "Computes the SHA-256 hash of a binary string.",
        input_arg1
    ),(
        sha384,
        "Computes the SHA-384 hash of a binary string.",
        input_arg1
    ),(
        sha512,
        "Computes the SHA-512 hash of a binary string.",
        input_arg1
    ));
}

/// Returns all DataFusion functions defined in this package
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![digest(), md5(), sha224(), sha256(), sha384(), sha512()]
}
