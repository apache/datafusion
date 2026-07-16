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

//! A simplified [`TableProvider`] for streaming partitioned datasets

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion_common::{DFSchema, Result, plan_err};
use datafusion_expr::execution_props::SubqueryContext;
use datafusion_expr::{Expr, SortExpr, TableType};
use datafusion_physical_expr::equivalence::project_ordering;
use datafusion_physical_expr::projection::ProjectionMapping;
use datafusion_physical_expr::{
    EquivalenceProperties, LexOrdering, Partitioning, create_physical_sort_exprs,
};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::streaming::{PartitionStream, StreamingTableExec};
use log::debug;

use crate::{Session, TableProvider};

/// A [`TableProvider`] that streams a set of [`PartitionStream`]
#[derive(Debug)]
pub struct StreamingTable {
    schema: SchemaRef,
    partitions: Vec<Arc<dyn PartitionStream>>,
    infinite: bool,
    sort_order: Vec<SortExpr>,
    output_partitioning: Option<Partitioning>,
}

impl StreamingTable {
    /// Try to create a new [`StreamingTable`] returning an error if the schema is incorrect
    pub fn try_new(
        schema: SchemaRef,
        partitions: Vec<Arc<dyn PartitionStream>>,
    ) -> Result<Self> {
        for x in partitions.iter() {
            let partition_schema = x.schema();
            if !schema.contains(partition_schema) {
                debug!(
                    "target schema does not contain partition schema. \
                        Target_schema: {schema:?}. Partition Schema: {partition_schema:?}"
                );
                return plan_err!("Mismatch between schema and batches");
            }
        }

        Ok(Self {
            schema,
            partitions,
            infinite: false,
            sort_order: vec![],
            output_partitioning: None,
        })
    }

    /// Sets streaming table can be infinite.
    pub fn with_infinite_table(mut self, infinite: bool) -> Self {
        self.infinite = infinite;
        self
    }

    /// Sets the existing ordering of streaming table.
    pub fn with_sort_order(mut self, sort_order: Vec<SortExpr>) -> Self {
        self.sort_order = sort_order;
        self
    }

    /// Declares the output partitioning of this streaming table.
    ///
    /// The partitioning expressions refer to the table schema before scan
    /// projection. If a scan projection removes a partitioning expression, the
    /// physical plan reports unknown partitioning.
    pub fn with_output_partitioning(mut self, output_partitioning: Partitioning) -> Self {
        self.output_partitioning = Some(output_partitioning);
        self
    }

    fn output_partitioning(
        &self,
        projection: Option<&Vec<usize>>,
    ) -> Result<Partitioning> {
        let Some(output_partitioning) = &self.output_partitioning else {
            return Ok(Partitioning::UnknownPartitioning(self.partitions.len()));
        };
        let Some(projection) = projection else {
            return Ok(output_partitioning.clone());
        };

        let projection_mapping =
            ProjectionMapping::from_indices(projection, &self.schema)?;
        let eq_properties = EquivalenceProperties::new(Arc::clone(&self.schema));
        Ok(output_partitioning.project(&projection_mapping, &eq_properties))
    }
}

#[async_trait]
impl TableProvider for StreamingTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_sort = if !self.sort_order.is_empty() {
            let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
            let eqp = state.execution_props();

            let original_sort_exprs = create_physical_sort_exprs(
                &self.sort_order,
                &df_schema,
                eqp,
                &SubqueryContext::default(),
            )?;

            if let Some(p) = projection {
                // When performing a projection, the output columns will not match
                // the original physical sort expression indices. Also the sort columns
                // may not be in the output projection. To correct for these issues
                // we need to project the ordering based on the output schema.
                let schema = Arc::new(self.schema.project(p)?);
                LexOrdering::new(original_sort_exprs)
                    .and_then(|lex_ordering| project_ordering(&lex_ordering, &schema))
                    .map(|lex_ordering| lex_ordering.to_vec())
                    .unwrap_or_default()
            } else {
                original_sort_exprs
            }
        } else {
            vec![]
        };

        let exec = StreamingTableExec::try_new(
            Arc::clone(&self.schema),
            self.partitions.clone(),
            projection,
            LexOrdering::new(physical_sort),
            self.infinite,
            limit,
        )?
        .with_output_partitioning(self.output_partitioning(projection)?)?;

        Ok(Arc::new(exec))
    }
}
