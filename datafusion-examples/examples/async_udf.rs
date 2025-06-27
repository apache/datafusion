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

use arrow::array::{ArrayIter, ArrayRef, AsArray, Int64Array, RecordBatch, StringArray};
use arrow::compute::kernels::cmp::eq;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::common::error::Result;
use datafusion::common::types::{logical_int64, logical_string};
use datafusion::common::utils::take_function_args;
use datafusion::common::{internal_err, not_impl_err};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};
use datafusion::logical_expr_common::signature::Coercion;
use datafusion::physical_expr_common::datum::apply_cmp;
use datafusion::prelude::SessionContext;
use log::trace;
use std::any::Any;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx: SessionContext = SessionContext::new();

    let async_upper = AsyncUpper::new();
    let udf = AsyncScalarUDF::new(Arc::new(async_upper));
    ctx.register_udf(udf.into_scalar_udf());
    let async_equal = AsyncEqual::new();
    let udf = AsyncScalarUDF::new(Arc::new(async_equal));
    ctx.register_udf(udf.into_scalar_udf());
    ctx.register_batch("animal", animal()?)?;

    // use Async UDF in the projection
    // +---------------+----------------------------------------------------------------------------------------+
    // | plan_type     | plan                                                                                   |
    // +---------------+----------------------------------------------------------------------------------------+
    // | logical_plan  | Projection: async_equal(a.id, Int64(1))                                                |
    // |               |   SubqueryAlias: a                                                                     |
    // |               |     TableScan: animal projection=[id]                                                  |
    // | physical_plan | ProjectionExec: expr=[__async_fn_0@1 as async_equal(a.id,Int64(1))]                    |
    // |               |   AsyncFuncExec: async_expr=[async_expr(name=__async_fn_0, expr=async_equal(id@0, 1))] |
    // |               |     CoalesceBatchesExec: target_batch_size=8192                                        |
    // |               |       DataSourceExec: partitions=1, partition_sizes=[1]                                |
    // |               |                                                                                        |
    // +---------------+----------------------------------------------------------------------------------------+
    ctx.sql("explain select async_equal(a.id, 1) from animal a")
        .await?
        .show()
        .await?;

    // +----------------------------+
    // | async_equal(a.id,Int64(1)) |
    // +----------------------------+
    // | true                       |
    // | false                      |
    // | false                      |
    // | false                      |
    // | false                      |
    // +----------------------------+
    ctx.sql("select async_equal(a.id, 1) from animal a")
        .await?
        .show()
        .await?;

    // use Async UDF in the filter
    // +---------------+--------------------------------------------------------------------------------------------+
    // | plan_type     | plan                                                                                       |
    // +---------------+--------------------------------------------------------------------------------------------+
    // | logical_plan  | SubqueryAlias: a                                                                           |
    // |               |   Filter: async_equal(animal.id, Int64(1))                                                 |
    // |               |     TableScan: animal projection=[id, name]                                                |
    // | physical_plan | CoalesceBatchesExec: target_batch_size=8192                                                |
    // |               |   FilterExec: __async_fn_0@2, projection=[id@0, name@1]                                    |
    // |               |     RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=1                  |
    // |               |       AsyncFuncExec: async_expr=[async_expr(name=__async_fn_0, expr=async_equal(id@0, 1))] |
    // |               |         CoalesceBatchesExec: target_batch_size=8192                                        |
    // |               |           DataSourceExec: partitions=1, partition_sizes=[1]                                |
    // |               |                                                                                            |
    // +---------------+--------------------------------------------------------------------------------------------+
    ctx.sql("explain select * from animal a where async_equal(a.id, 1)")
        .await?
        .show()
        .await?;

    // +----+------+
    // | id | name |
    // +----+------+
    // | 1  | cat  |
    // +----+------+
    ctx.sql("select * from animal a where async_equal(a.id, 1)")
        .await?
        .show()
        .await?;

    Ok(())
}

fn animal() -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let id_array = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
    let name_array = Arc::new(StringArray::from(vec![
        "cat", "dog", "fish", "bird", "snake",
    ]));

    Ok(RecordBatch::try_new(schema, vec![id_array, name_array])?)
}

#[derive(Debug)]
pub struct AsyncUpper {
    signature: Signature,
}

impl Default for AsyncUpper {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncUpper {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Coercible(vec![Coercion::Exact {
                    desired_type: TypeSignatureClass::Native(logical_string()),
                }]),
                Volatility::Volatile,
            ),
        }
    }
}

#[async_trait]
impl ScalarUDFImpl for AsyncUpper {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "async_upper"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!("AsyncUpper can only be called from async contexts")
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for AsyncUpper {
    fn ideal_batch_size(&self) -> Option<usize> {
        Some(10)
    }

    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
        _option: &ConfigOptions,
    ) -> Result<ArrayRef> {
        trace!("Invoking async_upper with args: {:?}", args);
        let value = &args.args[0];
        let result = match value {
            ColumnarValue::Array(array) => {
                let string_array = array.as_string::<i32>();
                let iter = ArrayIter::new(string_array);
                let result = iter
                    .map(|string| string.map(|s| s.to_uppercase()))
                    .collect::<StringArray>();
                Arc::new(result) as ArrayRef
            }
            _ => return internal_err!("Expected a string argument, got {:?}", value),
        };
        Ok(result)
    }
}

#[derive(Debug)]
struct AsyncEqual {
    signature: Signature,
}

impl Default for AsyncEqual {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncEqual {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Coercible(vec![
                    Coercion::Exact {
                        desired_type: TypeSignatureClass::Native(logical_int64()),
                    },
                    Coercion::Exact {
                        desired_type: TypeSignatureClass::Native(logical_int64()),
                    },
                ]),
                Volatility::Volatile,
            ),
        }
    }
}

#[async_trait]
impl ScalarUDFImpl for AsyncEqual {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "async_equal"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!("AsyncEqual can only be called from async contexts")
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for AsyncEqual {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
        _option: &ConfigOptions,
    ) -> Result<ArrayRef> {
        let [arg1, arg2] = take_function_args(self.name(), &args.args)?;
        apply_cmp(arg1, arg2, eq)?.to_array(args.number_rows)
    }
}
