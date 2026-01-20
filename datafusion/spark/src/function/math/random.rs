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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::types::{NativeType, logical_int32, logical_int64};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err, internal_err, plan_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    Coercion, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, TypeSignature,
    TypeSignatureClass,
};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::expr_fn::random;
use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng, rng};
use rand_distr::{Alphanumeric, StandardNormal, Uniform};

/// <https://spark.apache.org/docs/latest/api/sql/index.html#random>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkRandom {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkRandom {
    fn default() -> Self {
        SparkRandom::new()
    }
}

impl SparkRandom {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Nullary,
                    TypeSignature::Coercible(vec![Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_int64()),
                        vec![TypeSignatureClass::Integer],
                        NativeType::Int64,
                    )]),
                ],
                Volatility::Volatile,
            ),
            aliases: vec!["rand".to_string()],
        }
    }
}

impl ScalarUDFImpl for SparkRandom {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "random"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Float64, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [seed] = take_function_args(self.name(), args.args)?;

        let seed = match seed {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(val))) => val as u64,
            ColumnarValue::Scalar(ScalarValue::Int64(None)) => 0,
            _ => {
                return exec_err!(
                    "`{}` function expects an Int64 or Int32 seed argument",
                    self.name()
                );
            }
        };

        let mut rng = SmallRng::seed_from_u64(seed);
        let uniform =
            Uniform::new(0.0, 1.0).expect("Failed to create uniform distribution");

        let array: Float64Array = (0..args.number_rows)
            .map(|_| Some(rng.sample(uniform)))
            .collect();

        Ok(ColumnarValue::Array(Arc::new(array)))
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        // if no seed is provided, we can simplify to Datafusion built-in random()
        match args.len() {
            0 => Ok(ExprSimplifyResult::Simplified(random())),
            1 => Ok(ExprSimplifyResult::Original(args)),
            _ => plan_err!("`{}` function expects 0 or 1 argument(s)", self.name()),
        }
    }
}

/// <https://spark.apache.org/docs/latest/api/sql/index.html#randn>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkRandN {
    signature: Signature,
}

impl Default for SparkRandN {
    fn default() -> Self {
        SparkRandN::new()
    }
}

impl SparkRandN {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Nullary,
                    TypeSignature::Coercible(vec![Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_int64()),
                        vec![TypeSignatureClass::Integer],
                        NativeType::Int64,
                    )]),
                ],
                Volatility::Volatile,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkRandN {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "randn"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Float64, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let seed = match args.args.len() {
            // Apache Spark uses a random seed when none is provided
            0 => rng().next_u64(),
            1 => match args.args[0] {
                ColumnarValue::Scalar(ScalarValue::Int64(Some(val))) => val as u64,
                // Apache Spark uses a seed of 0 when NULL is provided
                ColumnarValue::Scalar(ScalarValue::Int64(None)) => 0,
                _ => {
                    return exec_err!(
                        "`{}` function expects an Int64 seed argument",
                        self.name()
                    );
                }
            },
            _ => {
                return exec_err!(
                    "`{}` function expects 0 or 1 argument(s)",
                    self.name()
                );
            }
        };

        let mut rng = SmallRng::seed_from_u64(seed);
        let array: Float64Array = (0..args.number_rows)
            .map(|_| Some(rng.sample(StandardNormal)))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}

/// <https://spark.apache.org/docs/latest/api/sql/index.html#randstr>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkRandStr {
    signature: Signature,
}

impl Default for SparkRandStr {
    fn default() -> Self {
        SparkRandStr::new()
    }
}

impl SparkRandStr {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_int32()),
                        vec![TypeSignatureClass::Integer],
                        NativeType::Int32,
                    )]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_implicit(
                            TypeSignatureClass::Native(logical_int32()),
                            vec![TypeSignatureClass::Integer],
                            NativeType::Int32,
                        ),
                        Coercion::new_implicit(
                            TypeSignatureClass::Native(logical_int64()),
                            vec![TypeSignatureClass::Integer],
                            NativeType::Int64,
                        ),
                    ]),
                ],
                Volatility::Volatile,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkRandStr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "randstr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let length = match args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(val))) if val > 0 => {
                val as usize
            }
            ColumnarValue::Scalar(ScalarValue::Int32(None)) => 10,
            _ => {
                return exec_err!(
                    "`{}` function expects a positive Int32 length argument",
                    self.name()
                );
            }
        };

        let seed = match args.args.len() {
            // Apache Spark uses a random seed when none is provided
            1 => rng().next_u64(),
            2 => match args.args[1] {
                ColumnarValue::Scalar(ScalarValue::Int64(Some(val))) => val as u64,
                // Apache Spark uses a seed of 0 when NULL is provided
                ColumnarValue::Scalar(ScalarValue::Int64(None)) => 0,
                _ => {
                    return exec_err!(
                        "`{}` function expects an Int64 seed argument",
                        self.name()
                    );
                }
            },
            _ => {
                return exec_err!(
                    "`{}` function expects 1 or 2 argument(s)",
                    self.name()
                );
            }
        };

        let mut rng = SmallRng::seed_from_u64(seed);
        let values: StringArray = (0..args.number_rows)
            .map(|_| {
                let s: String = (0..length)
                    .map(|_| rng.sample(Alphanumeric) as char)
                    .collect();
                Some(s)
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(values)))
    }
}
