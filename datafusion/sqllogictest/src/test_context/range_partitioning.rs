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

use std::fmt;
use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{Result, ScalarValue, project_schema};
use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::expressions::col as physical_col;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::SchedulingType;
use datafusion::physical_plan::projection::ProjectionExprs;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RangePartitioning,
    SendableRecordBatchStream, SplitPoint, Statistics,
};
use datafusion::prelude::SessionContext;
use datafusion_datasource::memory::MemorySourceConfig;

// ==============================================================================
// Range Partitioned Table (sqllogictest-only)
// ==============================================================================

/// Simple range-partitioned table for testing before declaring such tables is
/// supported via SQL.
#[derive(Debug)]
struct RangePartitionedTable {
    schema: SchemaRef,
    partitions: Vec<Vec<RecordBatch>>,
    range_column_index: usize,
    split_points: Vec<SplitPoint>,
}

#[async_trait]
impl TableProvider for RangePartitionedTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema, projection)?;
        let mut source = MemorySourceConfig::try_new(
            &self.partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        source = source.with_show_sizes(state.config_options().explain.show_sizes);

        let output_partitioning =
            self.output_partitioning(projection, &projected_schema)?;
        let source = RangePartitionedSource {
            inner: source,
            output_partitioning,
        };

        Ok(DataSourceExec::from_data_source(source))
    }
}

impl RangePartitionedTable {
    fn output_partitioning(
        &self,
        projection: Option<&Vec<usize>>,
        projected_schema: &SchemaRef,
    ) -> Result<Partitioning> {
        let Some(projected_range_index) =
            projected_index(self.range_column_index, projection)
        else {
            return Ok(Partitioning::UnknownPartitioning(self.partitions.len()));
        };

        let range_column = projected_schema.field(projected_range_index).name();
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            physical_col(range_column, projected_schema)?,
            SortOptions::default(),
        )])
        .expect("range ordering should not be empty");

        Ok(Partitioning::Range(RangePartitioning::try_new(
            ordering,
            self.split_points.clone(),
        )?))
    }
}

fn projected_index(
    column_index: usize,
    projection: Option<&Vec<usize>>,
) -> Option<usize> {
    projection
        .map(|projection| projection.iter().position(|idx| *idx == column_index))
        .unwrap_or(Some(column_index))
}

#[derive(Clone, Debug)]
struct RangePartitionedSource {
    inner: MemorySourceConfig,
    output_partitioning: Partitioning,
}

impl DataSource for RangePartitionedSource {
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.open(partition, context)
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt_as(t, f)?;
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, ", output_partitioning={}", self.output_partitioning)
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }

    fn output_partitioning(&self) -> Partitioning {
        self.output_partitioning.clone()
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        self.inner.eq_properties()
    }

    fn scheduling_type(&self) -> SchedulingType {
        self.inner.scheduling_type()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.inner.partition_statistics(partition)
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        Some(Arc::new(Self {
            inner: self.inner.clone().with_limit(limit),
            output_partitioning: self.output_partitioning.clone(),
        }))
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        // Range partitioning metadata is projection-sensitive. This fixture
        // computes it in TableProvider::scan, so do not rewrite later
        // ProjectionExec nodes into the source.
        Ok(None)
    }
}

pub(super) fn register_range_partitioned_table(ctx: &SessionContext) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("range_key", DataType::Int32, false),
        Field::new("non_range_key", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let partitions = vec![
        vec![range_partition_batch(&schema, &[1, 5], &[1, 2], &[10, 50])],
        vec![range_partition_batch(
            &schema,
            &[10, 15],
            &[1, 2],
            &[100, 150],
        )],
        vec![range_partition_batch(
            &schema,
            &[20, 25],
            &[1, 2],
            &[200, 250],
        )],
        vec![range_partition_batch(
            &schema,
            &[30, 35],
            &[1, 2],
            &[300, 350],
        )],
    ];
    let split_points = vec![
        SplitPoint::new(vec![ScalarValue::Int32(Some(10))]),
        SplitPoint::new(vec![ScalarValue::Int32(Some(20))]),
        SplitPoint::new(vec![ScalarValue::Int32(Some(30))]),
    ];
    let table = RangePartitionedTable {
        schema,
        partitions,
        range_column_index: 0,
        split_points,
    };

    ctx.register_table("range_partitioned", Arc::new(table))
        .expect("range partitioned table registration should succeed");
}

fn range_partition_batch(
    schema: &SchemaRef,
    range_key: &[i32],
    non_range_key: &[i32],
    value: &[i32],
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int32Array::from(range_key.to_vec())),
            Arc::new(Int32Array::from(non_range_key.to_vec())),
            Arc::new(Int32Array::from(value.to_vec())),
        ],
    )
    .expect("range partition batch should be valid")
}
