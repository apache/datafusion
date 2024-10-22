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

use arrow::array::AsArray;
use arrow::compute::filter_record_batch;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion_common::{exec_err, DataFusionError};
use datafusion_expr::Accumulator;
use datafusion_expr::Operator;
use datafusion_functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion_physical_expr::PhysicalExpr;
use hashbrown::HashSet;
use parking_lot::Mutex;
use std::fmt;
use std::sync::Arc;

pub struct DynamicFilterInfo {
    columns: Vec<Arc<Column>>,
    build_side_names: Vec<String>,
    inner: Mutex<DynamicFilterInfoInner>,
}

struct DynamicFilterInfoInner {
    max_accumulators: Vec<MaxAccumulator>,
    min_accumulators: Vec<MinAccumulator>,
    final_expr: Option<Arc<dyn PhysicalExpr>>,
    batch_count: usize,
    processed_partitions: HashSet<usize>,
}

impl fmt::Debug for DynamicFilterInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamicFilterInfo")
            .field("columns", &self.columns)
            .field("inner", &"<locked>")
            .finish()
    }
}

impl DynamicFilterInfo {
    // Create a new DynamicFilterInfo instance
    pub fn try_new(
        columns: Vec<Arc<Column>>,
        build_side_names: Vec<String>,
        data_types: Vec<&DataType>,
        total_batches: usize,
    ) -> Result<Self, DataFusionError> {
        let (max_accumulators, min_accumulators) = data_types
            .into_iter()
            .try_fold::<_, _, Result<_, DataFusionError>>(
                (Vec::new(), Vec::new()),
                |(mut max_acc, mut min_acc), data_type| {
                    max_acc.push(MaxAccumulator::try_new(data_type)?);
                    min_acc.push(MinAccumulator::try_new(data_type)?);
                    Ok((max_acc, min_acc))
                },
            )?;

        Ok(Self {
            columns,
            build_side_names,
            inner: Mutex::new(DynamicFilterInfoInner {
                max_accumulators,
                min_accumulators,
                final_expr: None,
                batch_count: total_batches,
                processed_partitions: HashSet::with_capacity(total_batches),
            }),
        })
    }

    // Set the final expression for the filter
    pub fn with_final_expr(self, expr: Arc<dyn PhysicalExpr>) -> Self {
        self.inner.lock().final_expr = Some(expr);
        self
    }

    // Merge a new batch of data and update the max and min accumulators
    pub fn merge_batch_and_check_finalized(
        &self,
        records: &RecordBatch,
        partition: usize,
    ) -> Result<bool, DataFusionError> {
        let mut inner = self.inner.lock();

        if inner.final_expr.is_some() {
            return Ok(true);
        }

        if !inner.processed_partitions.insert(partition) {
            return Ok(false);
        }

        inner.batch_count = inner.batch_count.saturating_sub(1);
        if records.num_rows() == 0 {
            return Ok(false);
        }

        let finalize = inner.batch_count == 0;

        let schema = records.schema();
        let columns = records.columns();
        for (i, _) in self.columns.iter().enumerate() {
            let index = schema.index_of(&self.build_side_names[i])?;
            let column_data = &columns[index];
            inner.max_accumulators[i]
                .update_batch(&[Arc::<dyn arrow_array::Array>::clone(column_data)])?;
            inner.min_accumulators[i]
                .update_batch(&[Arc::<dyn arrow_array::Array>::clone(column_data)])?;
        }

        if finalize {
            drop(inner);
            self.finalize_filter()?;
            return Ok(true);
        }

        Ok(false)
    }

    // Finalize the filter by creating the final expression
    fn finalize_filter(&self) -> Result<(), DataFusionError> {
        let mut inner = self.inner.lock();
        let filter_expr =
            self.columns.iter().enumerate().try_fold::<_, _, Result<
                Option<Arc<dyn PhysicalExpr>>,
                DataFusionError,
            >>(None, |acc, (i, column)| {
                let max_value = inner.max_accumulators[i].evaluate()?;
                let min_value = inner.min_accumulators[i].evaluate()?;

                let max_scalar = max_value.clone();
                let min_scalar = min_value.clone();

                let max_expr: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(max_scalar));
                let min_expr: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(min_scalar));

                let range_condition: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::<dyn datafusion_physical_expr::PhysicalExpr>::clone(
                            &min_expr,
                        ),
                        Operator::LtEq,
                        Arc::<datafusion_physical_expr::expressions::Column>::clone(
                            column,
                        ),
                    )),
                    Operator::And,
                    Arc::new(BinaryExpr::new(
                        Arc::<datafusion_physical_expr::expressions::Column>::clone(
                            column,
                        ),
                        Operator::LtEq,
                        Arc::<dyn datafusion_physical_expr::PhysicalExpr>::clone(
                            &max_expr,
                        ),
                    )),
                ));

                match acc {
                    Some(expr) => Ok(Some(Arc::new(BinaryExpr::new(
                        expr,
                        Operator::And,
                        range_condition,
                    ))
                        as Arc<dyn PhysicalExpr>)),
                    None => Ok(Some(range_condition)),
                }
            })?;

        let filter_expr = filter_expr.expect("Filter expression should be built");
        inner.final_expr = Some(filter_expr);
        Ok(())
    }

    // Apply the filter to a batch of records
    pub fn filter_batch(
        &self,
        records: &RecordBatch,
    ) -> Result<RecordBatch, DataFusionError> {
        let filter_expr = match self.inner.lock().final_expr.as_ref() {
            Some(expr) => Arc::<dyn datafusion_physical_expr::PhysicalExpr>::clone(expr),
            None => {
                return exec_err!(
                "Filter expression should have been created before calling filter_batch"
            )
            }
        };

        let boolean_array = filter_expr
            .evaluate(records)?
            .into_array(records.num_rows())?;
        let filtered_batch =
            filter_record_batch(records, boolean_array.as_ref().as_boolean())?;

        Ok(filtered_batch)
    }

    // Check if the filter is empty (i.e., if the final expression has been created)
    pub fn is_empty(&self) -> bool {
        self.inner.lock().final_expr.is_some()
    }

    // get the final expr
    pub fn has_final_expr(&self) -> bool {
        let inner = self.inner.lock();
        inner.final_expr.is_some()
    }

    // merge the predicate
    pub fn final_predicate(
        &self,
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> Option<Arc<dyn PhysicalExpr>> {
        let inner = self.inner.lock();

        match (inner.final_expr.clone(), predicate) {
            (Some(self_expr), Some(input_expr)) => Some(Arc::new(BinaryExpr::new(
                self_expr,
                Operator::And,
                input_expr,
            ))),
            (Some(self_expr), None) => Some(self_expr),
            (None, Some(input_expr)) => Some(input_expr),
            (None, None) => None,
        }
    }
}

/// used in partition mode
pub struct PartitionedDynamicFilterInfo {
    partition: usize,
    dynamic_filter_info: Arc<DynamicFilterInfo>,
}

impl PartitionedDynamicFilterInfo {
    pub fn new(partition: usize, dynamic_filter_info: Arc<DynamicFilterInfo>) -> Self {
        Self {
            partition,
            dynamic_filter_info,
        }
    }

    pub fn merge_batch_and_check_finalized(
        &self,
        records: &RecordBatch,
    ) -> Result<bool, DataFusionError> {
        self.dynamic_filter_info
            .merge_batch_and_check_finalized(records, self.partition)
    }
}
