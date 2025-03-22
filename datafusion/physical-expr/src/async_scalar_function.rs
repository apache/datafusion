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

use crate::ScalarFunctionExpr;
use arrow::array::{make_array, MutableArrayData, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::Result;
use datafusion_common::{internal_err, not_impl_err};
use datafusion_expr::async_udf::{AsyncScalarFunctionArgs, AsyncScalarUDF};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::any::Any;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Wrapper around a scalar function that can be evaluated asynchronously
#[derive(Debug, Clone, Eq)]
pub struct AsyncFuncExpr {
    /// The name of the output column this function will generate
    pub name: String,
    /// The actual function (always `ScalarFunctionExpr`)
    pub func: Arc<dyn PhysicalExpr>,
}

impl Display for AsyncFuncExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "async_expr(name={}, expr={})", self.name, self.func)
    }
}

impl PartialEq for AsyncFuncExpr {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.func == Arc::clone(&other.func)
    }
}

impl Hash for AsyncFuncExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.func.as_ref().hash(state);
    }
}

impl AsyncFuncExpr {
    /// create a new AsyncFuncExpr
    pub fn try_new(name: impl Into<String>, func: Arc<dyn PhysicalExpr>) -> Result<Self> {
        let Some(_) = func.as_any().downcast_ref::<ScalarFunctionExpr>() else {
            return internal_err!(
                "unexpected function type, expected ScalarFunctionExpr, got: {:?}",
                func
            );
        };

        Ok(Self {
            name: name.into(),
            func,
        })
    }

    /// return the name of the output column
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the output field generated by evaluating this function
    pub fn field(&self, input_schema: &Schema) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.func.data_type(input_schema)?,
            self.func.nullable(input_schema)?,
        ))
    }

    /// Return the ideal batch size for this function
    pub fn ideal_batch_size(&self) -> Result<Option<usize>> {
        if let Some(expr) = self.func.as_any().downcast_ref::<ScalarFunctionExpr>() {
            if let Some(udf) =
                expr.fun().inner().as_any().downcast_ref::<AsyncScalarUDF>()
            {
                return Ok(udf.ideal_batch_size());
            }
        }
        not_impl_err!("Can't get ideal_batch_size from {:?}", self.func)
    }

    /// This (async) function is called for each record batch to evaluate the LLM expressions
    ///
    /// The output is the output of evaluating the async expression and the input record batch
    pub async fn invoke_with_args(
        &self,
        batch: &RecordBatch,
        option: &ConfigOptions,
    ) -> Result<ColumnarValue> {
        let Some(scalar_function_expr) = self.func.as_any().downcast_ref::<ScalarFunctionExpr>()
        else {
            return internal_err!(
                "unexpected function type, expected ScalarFunctionExpr, got: {:?}",
                self.func
            );
        };

        let Some(async_udf) = scalar_function_expr
            .fun()
            .inner()
            .as_any()
            .downcast_ref::<AsyncScalarUDF>()
        else {
            return not_impl_err!(
                "Don't know how to evaluate async function: {:?}",
                scalar_function_expr
            );
        };

        let mut result_batches = vec![];
        if let Some(ideal_batch_size) = self.ideal_batch_size()? {
            let mut remainder = batch.clone();
            while remainder.num_rows() > 0 {
                let size = if ideal_batch_size > remainder.num_rows() {
                    remainder.num_rows()
                } else {
                    ideal_batch_size
                };

                let current_batch = remainder.slice(0, size); // get next 10 rows
                remainder = remainder.slice(size, remainder.num_rows() - size);
                let args = scalar_function_expr
                    .args()
                    .iter()
                    .map(|e| e.evaluate(&current_batch))
                    .collect::<Result<Vec<_>>>()?;
                result_batches.push(
                    async_udf
                        .invoke_async_with_args(
                            AsyncScalarFunctionArgs {
                                args: args.to_vec(),
                                number_rows: current_batch.num_rows(),
                                schema: current_batch.schema(),
                            },
                            option,
                        )
                        .await?,
                );
            }
        } else {
            let args = scalar_function_expr
                .args()
                .iter()
                .map(|e| e.evaluate(batch))
                .collect::<Result<Vec<_>>>()?;

            result_batches.push(
                async_udf
                    .invoke_async_with_args(
                        AsyncScalarFunctionArgs {
                            args: args.to_vec(),
                            number_rows: batch.num_rows(),
                            schema: batch.schema(),
                        },
                        option,
                    )
                    .await?,
            );
        }

        let datas = result_batches
            .iter()
            .map(|b| b.to_data())
            .collect::<Vec<_>>();
        let total_len = datas.iter().map(|d| d.len()).sum();
        let mut mutable = MutableArrayData::new(datas.iter().collect(), false, total_len);
        datas.iter().enumerate().for_each(|(i, data)| {
            mutable.extend(i, 0, data.len());
        });
        let array_ref = make_array(mutable.freeze());
        Ok(ColumnarValue::Array(array_ref))
    }
}

impl PhysicalExpr for AsyncFuncExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        self.func.data_type(_input_schema)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        self.func.nullable(_input_schema)
    }

    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        // TODO: implement this for scalar value input
        not_impl_err!("AsyncFuncExpr.evaluate")
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.func.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let new_func = Arc::clone(&self.func).with_new_children(children)?;
        Ok(Arc::new(AsyncFuncExpr {
            name: self.name.clone(),
            func: new_func,
        }))
    }
}
