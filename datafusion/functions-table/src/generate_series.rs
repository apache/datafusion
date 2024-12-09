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
use datafusion_physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};
use datafusion_physical_plan::ExecutionPlan;
use parking_lot::RwLock;
use std::fmt;
use std::sync::Arc;

/// Table that generates a series of integers from `start`(inclusive) to `end`(inclusive)
#[derive(Debug, Clone)]
struct GenerateSeriesTable {
    schema: SchemaRef,
    // None if input is Null
    start: Option<i64>,
    // None if input is Null
    end: Option<i64>,
}

/// Table state that generates a series of integers from `start`(inclusive) to `end`(inclusive)
#[derive(Debug, Clone)]
struct GenerateSeriesState {
    schema: SchemaRef,
    start: i64, // Kept for display
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
            self.start, self.end, self.batch_size
        )
    }
}

impl LazyBatchGenerator for GenerateSeriesState {
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
        match (self.start, self.end) {
            (Some(start), Some(end)) => {
                if start > end {
                    return plan_err!(
                        "End value must be greater than or equal to start value"
                    );
                }

                Ok(Arc::new(LazyMemoryExec::try_new(
                    self.schema.clone(),
                    vec![Arc::new(RwLock::new(GenerateSeriesState {
                        schema: self.schema.clone(),
                        start,
                        end,
                        current: start,
                        batch_size,
                    }))],
                )?))
            }
            _ => {
                // Either start or end is None, return a generator that outputs 0 rows
                Ok(Arc::new(LazyMemoryExec::try_new(
                    self.schema.clone(),
                    vec![Arc::new(RwLock::new(GenerateSeriesState {
                        schema: self.schema.clone(),
                        start: 0,
                        end: 0,
                        current: 1,
                        batch_size,
                    }))],
                )?))
            }
        }
    }
}

#[derive(Debug)]
pub struct GenerateSeriesFunc {}

impl TableFunctionImpl for GenerateSeriesFunc {
    // Check input `exprs` type and number. Input validity check (e.g. start <= end)
    // will be performed in `TableProvider::scan`
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        // TODO: support 1 or 3 arguments following DuckDB:
        // <https://duckdb.org/docs/sql/functions/list#generate_series>
        if exprs.len() == 3 || exprs.len() == 1 {
            return not_impl_err!("generate_series does not support 1 or 3 arguments");
        }

        if exprs.len() != 2 {
            return plan_err!("generate_series expects 2 arguments");
        }

        let start = match &exprs[0] {
            Expr::Literal(ScalarValue::Null) => None,
            Expr::Literal(ScalarValue::Int64(Some(n))) => Some(*n),
            _ => return plan_err!("First argument must be an integer literal"),
        };

        let end = match &exprs[1] {
            Expr::Literal(ScalarValue::Null) => None,
            Expr::Literal(ScalarValue::Int64(Some(n))) => Some(*n),
            _ => return plan_err!("Second argument must be an integer literal"),
        };

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));

        Ok(Arc::new(GenerateSeriesTable { schema, start, end }))
    }
}
