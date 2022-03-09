//
//
// hybrid.rs
// Copyright (C) 2022 Author zombie <zombie@zombie-ub2104>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use core::fmt;
use std::any::Any;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::expressions::PhysicalSortExpr;
use super::file_format::FileScanConfig;
use super::file_format::ParquetExec;
use super::memory::MemoryExec;
use super::{
    common, project_schema, DisplayFormatType, ExecutionPlan, Partitioning,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use crate::error::{DataFusionError, Result};
use crate::execution::runtime_env::RuntimeEnv;
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_expr::Expr;

/// Execution plan for combining memory and parquet
pub struct HybridExec {
    /// data in parquet
    parquet_exec: ParquetExec,
    /// data in memory
    memory_exec: MemoryExec,
    base_config: FileScanConfig,
    projected_schema: SchemaRef,
    partition_cnt: usize,
}

impl HybridExec {
    pub fn try_new(
        memory_partition: &[Vec<RecordBatch>],
        base_config: FileScanConfig,
        predicate: Option<Expr>,
    ) -> Result<Self> {
        let pcnt = 1 + base_config.file_groups.len();
        let projection = base_config.projection.clone();
        let input_schema = base_config.file_schema.clone();
        let projected_schema = project_schema(&input_schema, projection.as_ref())?;
        let memory_exec = MemoryExec::try_new(
            &memory_partition.to_vec(),
            base_config.file_schema.clone(),
            projection,
        )?;
        let base_config2 = base_config.clone();
        let parquet_exec = ParquetExec::new(base_config, predicate);
        Ok(Self {
            parquet_exec,
            memory_exec,
            base_config: base_config2,
            projected_schema: projected_schema.clone(),
            partition_cnt: pcnt,
        })
    }
}

#[async_trait]
impl ExecutionPlan for HybridExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partition_cnt)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!(
            "Children cannot be replaced in {:?}",
            self
        )))
    }
    async fn execute(
        &self,
        partition_index: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        match partition_index {
            0 => self.memory_exec.execute(partition_index, runtime).await,

            _ => {
                self.parquet_exec
                    .execute(partition_index - 1, runtime)
                    .await
            }
        }
    }
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "HybridExec:p size {:?}", self.parquet_exec)
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.parquet_exec.statistics()
    }
}

impl fmt::Debug for HybridExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "partitions: [...]")
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
