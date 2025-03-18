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
use datafusion_common::{plan_err, Result, ScalarValue};
use datafusion_expr::{Expr, TableType};
use datafusion_physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};
use datafusion_physical_plan::ExecutionPlan;
use parking_lot::RwLock;
use std::fmt;
use std::sync::Arc;

/// Indicates the arguments used for generating a series.
#[derive(Debug, Clone)]
enum GenSeriesArgs {
    /// ContainsNull signifies that at least one argument(start, end, step) was null, thus no series will be generated.
    ContainsNull {
        include_end: bool,
        name: &'static str,
    },
    /// AllNotNullArgs holds the start, end, and step values for generating the series when all arguments are not null.
    AllNotNullArgs {
        start: i64,
        end: i64,
        step: i64,
        /// Indicates whether the end value should be included in the series.
        include_end: bool,
        name: &'static str,
    },
}

/// Table that generates a series of integers from `start`(inclusive) to `end`(inclusive), incrementing by step
#[derive(Debug, Clone)]
struct GenerateSeriesTable {
    schema: SchemaRef,
    args: GenSeriesArgs,
}

/// Table state that generates a series of integers from `start`(inclusive) to `end`(inclusive), incrementing by step
#[derive(Debug, Clone)]
struct GenerateSeriesState {
    schema: SchemaRef,
    start: i64, // Kept for display
    end: i64,
    step: i64,
    batch_size: usize,

    /// Tracks current position when generating table
    current: i64,
    /// Indicates whether the end value should be included in the series.
    include_end: bool,
    name: &'static str,
}

impl GenerateSeriesState {
    fn reach_end(&self, val: i64) -> bool {
        if self.step > 0 {
            if self.include_end {
                return val > self.end;
            } else {
                return val >= self.end;
            }
        }

        if self.include_end {
            val < self.end
        } else {
            val <= self.end
        }
    }
}

/// Detail to display for 'Explain' plan
impl fmt::Display for GenerateSeriesState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}: start={}, end={}, batch_size={}",
            self.name, self.start, self.end, self.batch_size
        )
    }
}

impl LazyBatchGenerator for GenerateSeriesState {
    fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let mut buf = Vec::with_capacity(self.batch_size);
        while buf.len() < self.batch_size && !self.reach_end(self.current) {
            buf.push(self.current);
            self.current += self.step;
        }
        let array = Int64Array::from(buf);

        if array.is_empty() {
            return Ok(None);
        }

        let batch =
            RecordBatch::try_new(Arc::clone(&self.schema), vec![Arc::new(array)])?;

        Ok(Some(batch))
    }
}

#[async_trait]
impl TableProvider for GenerateSeriesTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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

        let state = match self.args {
            // if args have null, then return 0 row
            GenSeriesArgs::ContainsNull { include_end, name } => GenerateSeriesState {
                schema: self.schema(),
                start: 0,
                end: 0,
                step: 1,
                current: 1,
                batch_size,
                include_end,
                name,
            },
            GenSeriesArgs::AllNotNullArgs {
                start,
                end,
                step,
                include_end,
                name,
            } => GenerateSeriesState {
                schema: self.schema(),
                start,
                end,
                step,
                current: start,
                batch_size,
                include_end,
                name,
            },
        };

        Ok(Arc::new(LazyMemoryExec::try_new(
            self.schema(),
            vec![Arc::new(RwLock::new(state))],
        )?))
    }
}

#[derive(Debug)]
struct GenerateSeriesFuncImpl {
    name: &'static str,
    include_end: bool,
}

impl TableFunctionImpl for GenerateSeriesFuncImpl {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if exprs.is_empty() || exprs.len() > 3 {
            return plan_err!("{} function requires 1 to 3 arguments", self.name);
        }

        let mut normalize_args = Vec::new();
        for expr in exprs {
            match expr {
                Expr::Literal(ScalarValue::Null) => {}
                Expr::Literal(ScalarValue::Int64(Some(n))) => normalize_args.push(*n),
                _ => return plan_err!("First argument must be an integer literal"),
            };
        }

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));

        if normalize_args.len() != exprs.len() {
            // contain null
            return Ok(Arc::new(GenerateSeriesTable {
                schema,
                args: GenSeriesArgs::ContainsNull {
                    include_end: self.include_end,
                    name: self.name,
                },
            }));
        }

        let (start, end, step) = match &normalize_args[..] {
            [end] => (0, *end, 1),
            [start, end] => (*start, *end, 1),
            [start, end, step] => (*start, *end, *step),
            _ => {
                return plan_err!("{} function requires 1 to 3 arguments", self.name);
            }
        };

        if start > end && step > 0 {
            return plan_err!("start is bigger than end, but increment is positive: cannot generate infinite series");
        }

        if start < end && step < 0 {
            return plan_err!("start is smaller than end, but increment is negative: cannot generate infinite series");
        }

        if step == 0 {
            return plan_err!("step cannot be zero");
        }

        Ok(Arc::new(GenerateSeriesTable {
            schema,
            args: GenSeriesArgs::AllNotNullArgs {
                start,
                end,
                step,
                include_end: self.include_end,
                name: self.name,
            },
        }))
    }
}

#[derive(Debug)]
pub struct GenerateSeriesFunc {}

impl TableFunctionImpl for GenerateSeriesFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let impl_func = GenerateSeriesFuncImpl {
            name: "generate_series",
            include_end: true,
        };
        impl_func.call(exprs)
    }
}

#[derive(Debug)]
pub struct RangeFunc {}

impl TableFunctionImpl for RangeFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let impl_func = GenerateSeriesFuncImpl {
            name: "range",
            include_end: false,
        };
        impl_func.call(exprs)
    }
}
