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

//! Execution plan for window functions

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{
    aggregates, window_functions::WindowFunction, AggregateExpr, Distribution,
    ExecutionPlan, Partitioning, PhysicalExpr, SendableRecordBatchStream, WindowExpr,
};
use arrow::datatypes::{Field, Schema, SchemaRef};
use async_trait::async_trait;
use std::any::Any;
use std::sync::Arc;

/// Window execution plan
#[derive(Debug)]
pub struct WindowAggExec {
    /// Input plan
    input: Arc<dyn ExecutionPlan>,
    /// Window function expression
    window_expr: Vec<Arc<dyn WindowExpr>>,
    /// Schema after the window is run
    schema: SchemaRef,
    /// Schema before the window
    input_schema: SchemaRef,
}

/// Create a physical expression for window function
pub fn create_window_expr(
    fun: &WindowFunction,
    args: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    name: String,
) -> Result<Arc<dyn WindowExpr>> {
    match fun {
        WindowFunction::AggregateFunction(fun) => Ok(Arc::new(AggregateWindowExpr {
            aggregate: aggregates::create_aggregate_expr(
                fun,
                false,
                args,
                input_schema,
                name,
            )?,
        })),
        WindowFunction::BuiltInWindowFunction(fun) => {
            Err(DataFusionError::NotImplemented(format!(
                "window function with {:?} not implemented",
                fun
            )))
        }
    }
}

/// A window expr that takes the form of a built in window function
#[derive(Debug)]
pub struct BuiltInWindowExpr {}

/// A window expr that takes the form of an aggregate function
#[derive(Debug)]
pub struct AggregateWindowExpr {
    aggregate: Arc<dyn AggregateExpr>,
}

impl WindowExpr for AggregateWindowExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.aggregate.name()
    }

    fn field(&self) -> Result<Field> {
        self.aggregate.field()
    }
}

fn create_schema(
    input_schema: &Schema,
    window_expr: &[Arc<dyn WindowExpr>],
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(input_schema.fields().len() + window_expr.len());
    for expr in window_expr {
        fields.push(expr.field()?);
    }
    fields.extend_from_slice(input_schema.fields());
    Ok(Schema::new(fields))
}

impl WindowAggExec {
    /// Create a new execution plan for window aggregates
    pub fn try_new(
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
    ) -> Result<Self> {
        let schema = create_schema(&input.schema(), &window_expr)?;
        let schema = Arc::new(schema);
        Ok(WindowAggExec {
            input,
            window_expr,
            schema,
            input_schema,
        })
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Get the input schema before any aggregates are applied
    pub fn input_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }
}

#[async_trait]
impl ExecutionPlan for WindowAggExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::SinglePartition
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(WindowAggExec::try_new(
                self.window_expr.clone(),
                children[0].clone(),
                children[0].schema(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "WindowAggExec wrong number of children".to_owned(),
            )),
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "WindowAggExec invalid partition {}",
                partition
            )));
        }

        // window needs to operate on a single partition currently
        if 1 != self.input.output_partitioning().partition_count() {
            return Err(DataFusionError::Internal(
                "WindowAggExec requires a single input partition".to_owned(),
            ));
        }

        // let input = self.input.execute(0).await?;

        Err(DataFusionError::NotImplemented(
            "WindowAggExec::execute".to_owned(),
        ))
    }
}
