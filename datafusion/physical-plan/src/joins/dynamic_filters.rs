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
use arrow::array::PrimitiveArray;
use arrow::array::{
    Decimal128Array, Decimal256Array, Float16Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::aggregate::{max, min};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow_array::ArrowNativeTypeOp;
use arrow_array::{make_array, Array};
use datafusion_common::{exec_err, DataFusionError, ScalarValue};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion_physical_expr::PhysicalExpr;
use hashbrown::HashSet;
use parking_lot::Mutex;
use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

pub struct DynamicFilterInfo {
    columns: Vec<Arc<Column>>,
    build_side_names: Vec<String>,
    inner: Mutex<DynamicFilterInfoInner>,
}

struct DynamicFilterInfoInner {
    batches: Vec<Vec<Arc<dyn Array>>>,
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
        let batches = vec![Vec::new(); columns.len()];

        Ok(Self {
            columns,
            build_side_names,
            inner: Mutex::new(DynamicFilterInfoInner {
                batches,
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

        // that indicates this partition of stream may contains no data, so we return
        // or we have already have the final_expr
        if inner.final_expr.is_some()
            || (inner.processed_partitions.contains(&partition)
                && records.num_rows() == 0)
        {
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
            inner.batches[i].push(Arc::<dyn Array>::clone(column_data));
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
                // Compute min and max from batches[i]
                let (min_value, max_value) =
                    compute_min_max_from_batches(&inner.batches[i])?;

                let max_scalar = max_value.clone();
                let min_scalar = min_value.clone();

                let max_expr: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(max_scalar));
                let min_expr: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(min_scalar));

                let range_condition: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::<dyn PhysicalExpr>::clone(&min_expr),
                        Operator::LtEq,
                        Arc::<Column>::clone(column),
                    )),
                    Operator::And,
                    Arc::new(BinaryExpr::new(
                        Arc::<Column>::clone(column),
                        Operator::LtEq,
                        Arc::<dyn PhysicalExpr>::clone(&max_expr),
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
        println!("final expr is {:?}", filter_expr);
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

macro_rules! process_min_max {
    ($ARRAYS:expr, $ARRAY_TYPE:ty, $SCALAR_TY:ident, $NATIVE_TYPE:ty) => {{
        let mut min_val: Option<$NATIVE_TYPE> = None;
        let mut max_val: Option<$NATIVE_TYPE> = None;

        for array in $ARRAYS {
            if let Some(primitive_array) = array.as_any().downcast_ref::<$ARRAY_TYPE>() {
                let batch_min = min(primitive_array);
                let batch_max = max(primitive_array);

                min_val = match (min_val, batch_min) {
                    (Some(a), Some(b)) => Some(if a.is_lt(b) { a } else { b }),
                    (None, Some(b)) => Some(b),
                    (Some(a), None) => Some(a),
                    (None, None) => None,
                };

                max_val = match (max_val, batch_max) {
                    (Some(a), Some(b)) => Some(if a.is_gt(b) { a } else { b }),
                    (None, Some(b)) => Some(b),
                    (Some(a), None) => Some(a),
                    (None, None) => None,
                };
            }
        }
        Ok((
            ScalarValue::$SCALAR_TY(min_val),
            ScalarValue::$SCALAR_TY(max_val),
        ))
    }};
}

/// Currently only support numeric data types so generate a range filter
fn compute_min_max_from_batches(
    arrays: &[Arc<dyn Array>],
) -> Result<(ScalarValue, ScalarValue), DataFusionError> {
    if arrays.is_empty() {
        return exec_err!("should not be an empty array");
    }

    let data_type = arrays[0].data_type();
    match data_type {
        DataType::Int8 => process_min_max!(arrays, Int8Array, Int8, i8),
        DataType::Int16 => process_min_max!(arrays, Int16Array, Int16, i16),
        DataType::Int32 => process_min_max!(arrays, Int32Array, Int32, i32),
        DataType::Int64 => process_min_max!(arrays, Int64Array, Int64, i64),
        DataType::UInt8 => process_min_max!(arrays, UInt8Array, UInt8, u8),
        DataType::UInt16 => process_min_max!(arrays, UInt16Array, UInt16, u16),
        DataType::UInt32 => process_min_max!(arrays, UInt32Array, UInt32, u32),
        DataType::UInt64 => process_min_max!(arrays, UInt64Array, UInt64, u64),
        DataType::Float32 => process_min_max!(arrays, Float32Array, Float32, f32),
        DataType::Float64 => process_min_max!(arrays, Float64Array, Float64, f64),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Min/Max not implemented for type {}",
            data_type
        ))),
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
