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

use std::any::Any;
use std::sync::Arc;

use super::{
    common, DisplayAs, ExecutionMode, PlanProperties, SendableRecordBatchStream,
    Statistics,
};
use crate::{
    memory::MemoryStream, ColumnarValue, DisplayFormatType, ExecutionPlan, Partitioning,
    PhysicalExpr,
};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion_common::{internal_err, plan_err, Result, ScalarValue};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;

/// Execution plan for values list based relation (produces constant rows)
#[derive(Debug)]
pub struct ValuesExec {
    /// The schema
    schema: SchemaRef,
    /// The exprs
    exprs: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl ValuesExec {
    /// create a new values exec from data as expr
    pub fn try_new(
        schema: SchemaRef,
        exprs: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    ) -> Result<Self> {
        if exprs.is_empty() {
            return plan_err!("Values list cannot be empty");
        }
        let cache = Self::compute_properties(Arc::clone(&schema));
        Ok(ValuesExec {
            schema,
            exprs,
            cache,
        })
    }

    /// provides the exprs
    pub fn exprs(&self) -> &Vec<Vec<Arc<dyn PhysicalExpr>>> {
        &self.exprs
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);

        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        )
    }
}

impl DisplayAs for ValuesExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ValuesExec")
            }
        }
    }
}

impl ExecutionPlan for ValuesExec {
    fn name(&self) -> &'static str {
        "ValuesExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        ValuesExec::try_new(Arc::clone(&self.schema), self.exprs.clone())
            .map(|e| Arc::new(e) as _)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // ValuesExec has a single output partition
        if 0 != partition {
            return internal_err!(
                "ValuesExec invalid partition {partition} (expected 0)"
            );
        }

        let n_row = self.exprs.len();
        let n_col = self.schema.fields().len();
        // we have this single row batch as a placeholder to satisfy evaluation argument
        // and generate a single output row
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )?;

        let arr = (0..n_col)
            .map(|j| {
                (0..n_row)
                    .map(|i| {
                        let r = self.exprs[i][j].evaluate(&batch);

                        match r {
                            Ok(ColumnarValue::Scalar(scalar)) => Ok(scalar),
                            Ok(ColumnarValue::Array(a)) if a.len() == 1 => {
                                ScalarValue::try_from_array(&a, 0)
                            }
                            Ok(ColumnarValue::Array(a)) => {
                                plan_err!(
                                    "Cannot have array values {a:?} in a values list"
                                )
                            }
                            Err(err) => Err(err),
                        }
                    })
                    .collect::<Result<Vec<_>>>()
                    .and_then(ScalarValue::iter_to_array)
            })
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arr)?;
        let data: Vec<RecordBatch> = vec![batch];

        Ok(Box::pin(MemoryStream::try_new(
            data,
            Arc::clone(&self.schema),
            None,
        )?))
    }

    fn statistics(&self) -> Result<Statistics> {
        let n_row = self.exprs.len();
        let n_col = self.schema.fields().len();
        // we have this single row batch as a placeholder to satisfy evaluation argument
        // and generate a single output row
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )?;

        let arr = (0..n_col)
            .map(|j| {
                (0..n_row)
                    .map(|i| {
                        let r = self.exprs[i][j].evaluate(&batch);

                        match r {
                            Ok(ColumnarValue::Scalar(scalar)) => Ok(scalar),
                            Ok(ColumnarValue::Array(a)) if a.len() == 1 => {
                                ScalarValue::try_from_array(&a, 0)
                            }
                            Ok(ColumnarValue::Array(a)) => {
                                plan_err!(
                                    "Cannot have array values {a:?} in a values list"
                                )
                            }
                            Err(err) => Err(err),
                        }
                    })
                    .collect::<Result<Vec<_>>>()
                    .and_then(ScalarValue::iter_to_array)
            })
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arr)?;
        Ok(common::compute_record_batch_statistics(
            &[vec![batch]],
            &self.schema,
            None,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::lit;
    use crate::test::{self};

    use arrow_schema::{DataType, Field};

    #[tokio::test]
    async fn values_empty_case() -> Result<()> {
        let schema = test::aggr_test_schema();
        let empty = ValuesExec::try_new(schema, vec![]);
        assert!(empty.is_err());
        Ok(())
    }

    #[test]
    fn new_exec_with_exprs() {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
        let exprs = vec![vec![lit(1i32)], vec![lit(2i32)]];
        let _exec = ValuesExec::try_new(schema, exprs).unwrap();
    }

    // #[test]
    // fn new_exec_with_batches_invalid_schema() {
    //     let batch = make_partition(7);
    //     let batches = vec![batch.clone(), batch];
    //
    //     let invalid_schema = Arc::new(Schema::new(vec![
    //         Field::new("col0", DataType::UInt32, false),
    //         Field::new("col1", DataType::Utf8, false),
    //     ]));
    //     let _ = ValuesExec::try_new_from_batches(invalid_schema, batches).unwrap_err();
    // }

    // Test issue: https://github.com/apache/datafusion/issues/8763
    #[test]
    fn new_exec_with_non_nullable_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col0",
            DataType::UInt32,
            false,
        )]));
        let _ = ValuesExec::try_new(Arc::clone(&schema), vec![vec![lit(1u32)]]).unwrap();
        // Test that a null value is rejected
        let _ = ValuesExec::try_new(schema, vec![vec![lit(ScalarValue::UInt32(None))]])
            .unwrap_err();
    }
}
