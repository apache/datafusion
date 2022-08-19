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

//! IS TRUE expression

use std::{any::Any, sync::Arc};

use crate::PhysicalExpr;
use arrow::{
    array::{Array, BooleanArray},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;

/// IS TRUE expression
#[derive(Debug)]
pub struct IsTrueExpr {
    /// The input expression
    arg: Arc<dyn PhysicalExpr>,
}

impl IsTrueExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl std::fmt::Display for IsTrueExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} IS TRUE", self.arg)
    }
}

impl PhysicalExpr for IsTrueExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg = self.arg.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => {
                let array_len = array.len();
                let my_array = ColumnarValue::Array(array).into_array(array_len);
                let true_array = BooleanArray::from(vec![Some(true)]);
                let false_array = BooleanArray::from(vec![Some(false)]);
                let null_array = BooleanArray::from(vec![None]);
                let mut result_vec = vec![];
                for i in 0..array_len {
                    let current = (*my_array).slice(i, 1);
                    if (*current).eq(&true_array) {
                        result_vec.push(Some(true));
                    } else if (*current).eq(&false_array) || (*current).eq(&null_array) {
                        result_vec.push(Some(false));
                    } else {
                        return Err(DataFusionError::Execution(format!("Cannot apply 'IS TRUE' to arguments of type '<{:?}> IS TRUE'. Supported form(s): '<BOOLEAN> IS TRUE'", current.data_type())))
                    }
                }

                let return_array = BooleanArray::from(result_vec);
                Ok(ColumnarValue::Array(Arc::new(return_array)))
            }
            ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(
                ScalarValue::Boolean(Some(scalar.is_true().unwrap())),
            )),
        }
    }
}

/// Create an IS TRUE expression
pub fn is_true(arg: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(IsTrueExpr::new(arg)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use arrow::{
        array::BooleanArray,
        datatypes::*,
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

    #[test]
    fn is_true_op() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let a = BooleanArray::from(vec![Some(true), Some(false), None]);
        let expr = is_true(col("a", &schema)?).unwrap();
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // expression: "a is true"
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("failed to downcast to BooleanArray");

        let expected = &BooleanArray::from(vec![true, false, false]);

        assert_eq!(expected, result);

        Ok(())
    }
}
