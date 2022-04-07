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

//! Values execution plan

use super::expressions::PhysicalSortExpr;
use super::{common, SendableRecordBatchStream, Statistics};
use crate::error::{DataFusionError, Result};
use crate::execution::context::TaskContext;
use crate::physical_plan::{
    memory::MemoryStream, ColumnarValue, DisplayFormatType, Distribution, ExecutionPlan,
    Partitioning, PhysicalExpr,
};
use crate::scalar::ScalarValue;
use arrow::array::new_null_array;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::any::Any;
use std::sync::Arc;

/// Execution plan for values list based relation (produces constant rows)
#[derive(Debug)]
pub struct ValuesExec {
    /// The schema
    schema: SchemaRef,
    /// The data
    data: Vec<RecordBatch>,
}

impl ValuesExec {
    /// create a new values exec from data as expr
    pub fn try_new(
        schema: SchemaRef,
        data: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    ) -> Result<Self> {
        if data.is_empty() {
            return Err(DataFusionError::Plan("Values list cannot be empty".into()));
        }
        let n_row = data.len();
        let n_col = schema.fields().len();
        // we have this single row, null, typed batch as a placeholder to satisfy evaluation argument
        let batch = RecordBatch::try_new(
            schema.clone(),
            schema
                .fields()
                .iter()
                .map(|field| new_null_array(field.data_type(), 1))
                .collect::<Vec<_>>(),
        )?;
        let arr = (0..n_col)
            .map(|j| {
                (0..n_row)
                    .map(|i| {
                        let r = data[i][j].evaluate(&batch);
                        match r {
                            Ok(ColumnarValue::Scalar(scalar)) => Ok(scalar),
                            Ok(ColumnarValue::Array(a)) if a.len() == 1 => {
                                ScalarValue::try_from_array(&a, 0)
                            }
                            Ok(ColumnarValue::Array(a)) => {
                                Err(DataFusionError::Plan(format!(
                                    "Cannot have array values {:?} in a values list",
                                    a
                                )))
                            }
                            Err(err) => Err(err),
                        }
                    })
                    .collect::<Result<Vec<_>>>()
                    .and_then(ScalarValue::iter_to_array)
            })
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new(schema.clone(), arr)?;
        let data: Vec<RecordBatch> = vec![batch];
        Ok(Self { schema, data })
    }

    /// provides the data
    fn data(&self) -> Vec<RecordBatch> {
        self.data.clone()
    }
}

#[async_trait]
impl ExecutionPlan for ValuesExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ValuesExec {
            schema: self.schema.clone(),
            data: self.data.clone(),
        }))
    }

    async fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // GlobalLimitExec has a single output partition
        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "ValuesExec invalid partition {} (expected 0)",
                partition
            )));
        }

        Ok(Box::pin(MemoryStream::try_new(
            self.data(),
            self.schema.clone(),
            None,
        )?))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "ValuesExec")
            }
        }
    }

    fn statistics(&self) -> Statistics {
        let batch = self.data();
        common::compute_record_batch_statistics(&[batch], &self.schema, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util;

    #[tokio::test]
    async fn values_empty_case() -> Result<()> {
        let schema = test_util::aggr_test_schema();
        let empty = ValuesExec::try_new(schema, vec![]);
        assert!(empty.is_err());
        Ok(())
    }
}
