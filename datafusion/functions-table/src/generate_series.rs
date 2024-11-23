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

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_catalog::Session;
use datafusion_catalog::TableFunctionImpl;
use datafusion_catalog::TableProvider;
use datafusion_common::{not_impl_err, plan_err, Result, ScalarValue};
use datafusion_expr::{Expr, TableType};
use datafusion_physical_plan::memory::{StreamingBatchGenerator, StreamingMemoryExec};
use datafusion_physical_plan::ExecutionPlan;
use std::fmt;
use std::sync::Arc;

/// Table that generates a series of integers from `start`(inclusive) to `end`(inclusive)
#[derive(Debug, Clone)]
struct GenerateSeriesTable {
    schema: SchemaRef,
    start: i64,
    end: i64,
}

#[derive(Debug, Clone)]
struct GenerateSeriesState {
    schema: SchemaRef,
    _start: i64,
    end: i64,
    batch_size: usize,

    /// Tracks current position when generating table
    current: i64,
}

/// Detail to display for 'Explain' plan
impl fmt::Display for GenerateSeriesState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "generate_series: start={}, end={}, batch_size={}",
            self._start, self.end, self.batch_size
        )
    }
}

impl StreamingBatchGenerator for GenerateSeriesState {
    fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        // Check if we've reached the end
        if self.current > self.end {
            return Ok(None);
        }

        // Construct batch
        let batch_end = (self.current + self.batch_size as i64 - 1).min(self.end);
        let array = Int64Array::from_iter_values(self.current..=batch_end);
        let batch = RecordBatch::try_new(self.schema.clone(), vec![Arc::new(array)])?;

        // Update current position for next batch
        self.current = batch_end + 1;

        Ok(Some(batch))
    }

    fn clone_box(&self) -> Box<dyn StreamingBatchGenerator> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl TableProvider for GenerateSeriesTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batch_size = state.config_options().execution.batch_size;

        Ok(Arc::new(StreamingMemoryExec::try_new(
            self.schema.clone(),
            vec![Box::new(GenerateSeriesState {
                schema: self.schema.clone(),
                _start: self.start,
                end: self.end,
                current: self.start,
                batch_size,
            })],
        )?))
    }
}

#[derive(Debug)]
pub struct GenerateSeriesFunc {}

impl TableFunctionImpl for GenerateSeriesFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if exprs.len() != 2 {
            return plan_err!("generate_series expects 2 arguments");
        }

        let start = match &exprs[0] {
            Expr::Literal(ScalarValue::Int64(Some(n))) => *n,
            _ => return plan_err!("First argument must be an integer literal"),
        };

        let end = match &exprs[1] {
            Expr::Literal(ScalarValue::Int64(Some(n))) => *n,
            _ => return plan_err!("Second argument must be an integer literal"),
        };

        if end < start {
            return not_impl_err!(
                "End value must be greater than or equal to start value"
            );
        }

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));

        Ok(Arc::new(GenerateSeriesTable { schema, start, end }))
    }
}
