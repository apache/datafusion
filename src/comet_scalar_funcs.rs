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

use crate::hash_funcs::*;
use crate::scalar_funcs::{
    spark_ceil, spark_decimal_div, spark_floor, spark_hex, spark_isnan, spark_make_decimal,
    spark_round, spark_unhex, spark_unscaled_value, SparkChrFunc,
};
use crate::{spark_date_add, spark_date_sub, spark_read_side_padding};
use arrow_schema::DataType;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionImplementation, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

macro_rules! make_comet_scalar_udf {
    ($name:expr, $func:ident, $data_type:ident) => {{
        let scalar_func = CometScalarFunction::new(
            $name.to_string(),
            Signature::variadic_any(Volatility::Immutable),
            $data_type.clone(),
            Arc::new(move |args| $func(args, &$data_type)),
        );
        Ok(Arc::new(ScalarUDF::new_from_impl(scalar_func)))
    }};
    ($name:expr, $func:expr, without $data_type:ident) => {{
        let scalar_func = CometScalarFunction::new(
            $name.to_string(),
            Signature::variadic_any(Volatility::Immutable),
            $data_type,
            $func,
        );
        Ok(Arc::new(ScalarUDF::new_from_impl(scalar_func)))
    }};
}

/// Create a physical scalar function.
pub fn create_comet_physical_fun(
    fun_name: &str,
    data_type: DataType,
    registry: &dyn FunctionRegistry,
) -> Result<Arc<ScalarUDF>, DataFusionError> {
    match fun_name {
        "ceil" => {
            make_comet_scalar_udf!("ceil", spark_ceil, data_type)
        }
        "floor" => {
            make_comet_scalar_udf!("floor", spark_floor, data_type)
        }
        "read_side_padding" => {
            let func = Arc::new(spark_read_side_padding);
            make_comet_scalar_udf!("read_side_padding", func, without data_type)
        }
        "round" => {
            make_comet_scalar_udf!("round", spark_round, data_type)
        }
        "unscaled_value" => {
            let func = Arc::new(spark_unscaled_value);
            make_comet_scalar_udf!("unscaled_value", func, without data_type)
        }
        "make_decimal" => {
            make_comet_scalar_udf!("make_decimal", spark_make_decimal, data_type)
        }
        "hex" => {
            let func = Arc::new(spark_hex);
            make_comet_scalar_udf!("hex", func, without data_type)
        }
        "unhex" => {
            let func = Arc::new(spark_unhex);
            make_comet_scalar_udf!("unhex", func, without data_type)
        }
        "decimal_div" => {
            make_comet_scalar_udf!("decimal_div", spark_decimal_div, data_type)
        }
        "murmur3_hash" => {
            let func = Arc::new(spark_murmur3_hash);
            make_comet_scalar_udf!("murmur3_hash", func, without data_type)
        }
        "xxhash64" => {
            let func = Arc::new(spark_xxhash64);
            make_comet_scalar_udf!("xxhash64", func, without data_type)
        }
        "chr" => Ok(Arc::new(ScalarUDF::new_from_impl(SparkChrFunc::default()))),
        "isnan" => {
            let func = Arc::new(spark_isnan);
            make_comet_scalar_udf!("isnan", func, without data_type)
        }
        "sha224" => {
            let func = Arc::new(spark_sha224);
            make_comet_scalar_udf!("sha224", func, without data_type)
        }
        "sha256" => {
            let func = Arc::new(spark_sha256);
            make_comet_scalar_udf!("sha256", func, without data_type)
        }
        "sha384" => {
            let func = Arc::new(spark_sha384);
            make_comet_scalar_udf!("sha384", func, without data_type)
        }
        "sha512" => {
            let func = Arc::new(spark_sha512);
            make_comet_scalar_udf!("sha512", func, without data_type)
        }
        "date_add" => {
            let func = Arc::new(spark_date_add);
            make_comet_scalar_udf!("date_add", func, without data_type)
        }
        "date_sub" => {
            let func = Arc::new(spark_date_sub);
            make_comet_scalar_udf!("date_sub", func, without data_type)
        }
        _ => registry.udf(fun_name).map_err(|e| {
            DataFusionError::Execution(format!(
                "Function {fun_name} not found in the registry: {e}",
            ))
        }),
    }
}

struct CometScalarFunction {
    name: String,
    signature: Signature,
    data_type: DataType,
    func: ScalarFunctionImplementation,
}

impl Debug for CometScalarFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CometScalarFunction")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("data_type", &self.data_type)
            .finish()
    }
}

impl CometScalarFunction {
    fn new(
        name: String,
        signature: Signature,
        data_type: DataType,
        func: ScalarFunctionImplementation,
    ) -> Self {
        Self {
            name,
            signature,
            data_type,
            func,
        }
    }
}

impl ScalarUDFImpl for CometScalarFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> {
        Ok(self.data_type.clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        (self.func)(args)
    }
}
