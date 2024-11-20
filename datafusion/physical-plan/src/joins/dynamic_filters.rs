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

use super::utils::JoinHashMap;
use arrow::array::{AsArray, BooleanBuilder};
use arrow::array::{
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::aggregate::{max, max_string, min, min_string};

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow_array::StringArray;
use arrow_array::{Array, ArrayRef};
use datafusion_common::{arrow_err, DataFusionError, ScalarValue};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{
    BinaryExpr, Column, InListExpr, IsNullExpr, Literal,
};
use datafusion_physical_expr::PhysicalExpr;
use hashbrown::HashSet;
use parking_lot::Mutex;
use std::fmt;
use std::sync::Arc;

const UNIQUE_VALUES_THRESHOLD: usize = 0;

macro_rules! process_unique_values {
    ($array:expr, $array_type:ty, $scalar_type:ident, $unique_set:expr, $should_track:expr) => {{
        if let Some(array) = $array.as_any().downcast_ref::<$array_type>() {
            if $should_track {
                let mut has_null = false;
                for value in array.iter() {
                    match value {
                        Some(value) => {
                            $unique_set.insert(ScalarValue::$scalar_type(Some(value)));
                            if $unique_set.len() > UNIQUE_VALUES_THRESHOLD {
                                $unique_set.clear();
                                return Ok(false);
                            }
                        }
                        None => {
                            has_null = true;
                        }
                    }
                }
                if has_null {
                    $unique_set.insert(ScalarValue::$scalar_type(None));
                }
            }
        }
        Ok(true)
    }};
}
macro_rules! process_min_max {
    ($array:expr, $array_type:ty, $scalar_type:ident) => {{
        if let Some(array) = $array.as_any().downcast_ref::<$array_type>() {
            let min = min(array)
                .ok_or_else(|| DataFusionError::Internal("Empty array".to_string()))?;
            let max = max(array)
                .ok_or_else(|| DataFusionError::Internal("Empty array".to_string()))?;
            Ok((
                ScalarValue::$scalar_type(Some(min)),
                ScalarValue::$scalar_type(Some(max)),
            ))
        } else {
            Err(DataFusionError::Internal("Invalid array type".to_string()))
        }
    }};
}

struct DynamicFilterInfoInner {
    unique_values: Vec<HashSet<ScalarValue>>,
    value_ranges: Vec<Option<(ScalarValue, ScalarValue)>>,
    batches: Vec<Vec<Arc<dyn Array>>>,
    final_expr: Option<Arc<dyn PhysicalExpr>>,
    batch_count: usize,
    processed_partitions: HashSet<usize>,
    should_track_unique: Vec<bool>,
}

pub struct DynamicFilterInfo {
    columns: Vec<Arc<Column>>,
    build_side_names: Vec<String>,
    inner: Mutex<DynamicFilterInfoInner>,
}

impl DynamicFilterInfo {
    pub fn try_new(
        columns: Vec<Arc<Column>>,
        build_side_names: Vec<String>,
    ) -> Result<Self, DataFusionError> {
        let col_count = columns.len();
        Ok(Self {
            columns,
            build_side_names,
            inner: Mutex::new(DynamicFilterInfoInner {
                unique_values: vec![HashSet::new(); col_count],
                value_ranges: vec![None; col_count],
                batches: vec![Vec::new(); col_count],
                final_expr: None,
                batch_count: 0,
                processed_partitions: HashSet::new(),
                should_track_unique: vec![true; col_count],
            }),
        })
    }

    pub fn merge_batch_and_check_finalized(
        &self,
        records: &RecordBatch,
        partition: usize,
    ) -> Result<bool, DataFusionError> {
        let mut inner = self.inner.lock();
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
        let finalize = inner.batch_count == 0;

        let schema = records.schema();
        let columns = records.columns();

        for (i, _) in self.columns.iter().enumerate() {
            let index = schema.index_of(&self.build_side_names[i])?;
            let column_data = &columns[index];

            let should_track = inner.should_track_unique[i];

            if should_track {
                let still_tracking = update_unique_values(
                    &mut inner.unique_values[i],
                    column_data,
                    should_track,
                )?;

                if !still_tracking {
                    inner.should_track_unique[i] = false;
                }
            }

            update_range_stats(&mut inner.value_ranges[i], column_data)?;
            inner.batches[i].push(Arc::clone(column_data));
        }

        if finalize {
            drop(inner);
            self.finalize_filter()?;
            return Ok(true);
        }

        Ok(false)
    }

    fn finalize_filter(&self) -> Result<(), DataFusionError> {
        let mut inner = self.inner.lock();
        let filter_expr =
            self.columns.iter().enumerate().try_fold::<_, _, Result<
                Option<Arc<dyn PhysicalExpr>>,
                DataFusionError,
            >>(None, |acc, (i, column)| {
                let unique_values = &inner.unique_values[i];
                let value_range = &inner.value_ranges[i];
                let should_track_unique = inner.should_track_unique[i];

                let use_unique_list = should_track_unique
                    && !unique_values.is_empty()
                    && !is_continuous_type(inner.batches[i][0].data_type());

                let column_expr = if use_unique_list {
                    let values: Vec<Arc<dyn PhysicalExpr>> = unique_values
                        .iter()
                        .cloned()
                        .map(|value| {
                            Arc::new(Literal::new(value)) as Arc<dyn PhysicalExpr>
                        })
                        .collect();
                    Arc::new(InListExpr::new(
                        Arc::<Column>::clone(column),
                        values,
                        false,
                        None,
                    )) as Arc<dyn PhysicalExpr>
                } else {
                    match value_range {
                        Some((min_value, max_value)) => Arc::new(BinaryExpr::new(
                            Arc::new(BinaryExpr::new(
                                Arc::new(Literal::new(min_value.clone())),
                                Operator::LtEq,
                                Arc::<Column>::clone(column),
                            )),
                            Operator::And,
                            Arc::new(BinaryExpr::new(
                                Arc::<Column>::clone(column),
                                Operator::LtEq,
                                Arc::new(Literal::new(max_value.clone())),
                            )),
                        ))
                            as Arc<dyn PhysicalExpr>,
                        None => Arc::new(BinaryExpr::new(
                            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
                            Operator::Eq,
                            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
                        )) as Arc<dyn PhysicalExpr>,
                    }
                };

                match acc {
                    Some(expr) => Ok(Some(Arc::new(BinaryExpr::new(
                        expr,
                        Operator::And,
                        column_expr,
                    )))),
                    None => Ok(Some(column_expr)),
                }
            })?;
        let final_expr = match filter_expr {
            Some(expr) => Arc::new(BinaryExpr::new(
                expr,
                Operator::Or,
                Arc::new(IsNullExpr::new(Arc::<Column>::clone(&self.columns[0]))),
            )) as Arc<dyn PhysicalExpr>,
            None => Arc::new(BinaryExpr::new(
                Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
                Operator::Eq,
                Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
            )) as Arc<dyn PhysicalExpr>,
        };

        inner.final_expr = Some(final_expr);
        Ok(())
    }

    pub fn add_count(&self) -> Result<(), DataFusionError> {
        let mut inner = self.inner.lock();
        inner.batch_count = inner.batch_count.saturating_add(1);
        Ok(())
    }

    pub fn filter_batch(
        &self,
        records: &RecordBatch,
    ) -> Result<RecordBatch, DataFusionError> {
        let filter_expr = match self.inner.lock().final_expr.as_ref() {
            Some(expr) => Arc::clone(expr),
            None => return Err(DataFusionError::Internal(
                "Filter expression should have been created before calling filter_batch"
                    .to_string(),
            )),
        };
        let boolean_array = filter_expr
            .evaluate(records)?
            .into_array(records.num_rows())?;

        Ok(filter_record_batch(
            records,
            boolean_array.as_ref().as_boolean(),
        )?)
    }

    pub fn has_final_expr(&self) -> bool {
        self.inner.lock().final_expr.is_some()
    }

    pub fn final_predicate(
        &self,
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> (Option<Arc<dyn PhysicalExpr>>, String) {
        let inner = self.inner.lock();

        let result = match (inner.final_expr.clone(), predicate) {
            (Some(self_expr), Some(input_expr)) => {
                Some(
                    Arc::new(BinaryExpr::new(self_expr, Operator::And, input_expr))
                        as Arc<dyn PhysicalExpr>,
                )
            }
            (Some(self_expr), None) => Some(self_expr),
            (None, Some(input_expr)) => Some(input_expr),
            (None, None) => None,
        };

        let debug_info = inner
            .final_expr
            .as_ref()
            .map(|expr| format!("{}", expr))
            .unwrap_or_default();

        (result, debug_info)
    }
}

fn update_unique_values(
    unique_set: &mut HashSet<ScalarValue>,
    array: &dyn Array,
    should_track: bool,
) -> Result<bool, DataFusionError> {
    if !should_track {
        return Ok(false);
    }

    match array.data_type() {
        DataType::Int8 => {
            process_unique_values!(array, Int8Array, Int8, unique_set, should_track)
        }
        DataType::Int16 => {
            process_unique_values!(array, Int16Array, Int16, unique_set, should_track)
        }
        DataType::Int32 => {
            process_unique_values!(array, Int32Array, Int32, unique_set, should_track)
        }
        DataType::Int64 => {
            process_unique_values!(array, Int64Array, Int64, unique_set, should_track)
        }
        DataType::UInt8 => {
            process_unique_values!(array, UInt8Array, UInt8, unique_set, should_track)
        }
        DataType::UInt16 => {
            process_unique_values!(array, UInt16Array, UInt16, unique_set, should_track)
        }
        DataType::UInt32 => {
            process_unique_values!(array, UInt32Array, UInt32, unique_set, should_track)
        }
        DataType::UInt64 => {
            process_unique_values!(array, UInt64Array, UInt64, unique_set, should_track)
        }
        DataType::Float32 => {
            process_unique_values!(array, Float32Array, Float32, unique_set, should_track)
        }
        DataType::Float64 => {
            process_unique_values!(array, Float64Array, Float64, unique_set, should_track)
        }
        DataType::Utf8 => {
            if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
                if should_track {
                    for value in array.iter().flatten() {
                        unique_set.insert(ScalarValue::Utf8(Some(value.to_string())));
                        if unique_set.len() > UNIQUE_VALUES_THRESHOLD {
                            unique_set.clear();
                            return Ok(false);
                        }
                    }
                }
            }
            Ok(true)
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unique value tracking not implemented for type {}",
            array.data_type()
        ))),
    }
}

fn compute_min_max(
    array: &dyn Array,
) -> Result<(ScalarValue, ScalarValue), DataFusionError> {
    match array.data_type() {
        DataType::Int8 => process_min_max!(array, Int8Array, Int8),
        DataType::Int16 => process_min_max!(array, Int16Array, Int16),
        DataType::Int32 => process_min_max!(array, Int32Array, Int32),
        DataType::Int64 => process_min_max!(array, Int64Array, Int64),
        DataType::UInt8 => process_min_max!(array, UInt8Array, UInt8),
        DataType::UInt16 => process_min_max!(array, UInt16Array, UInt16),
        DataType::UInt32 => process_min_max!(array, UInt32Array, UInt32),
        DataType::UInt64 => process_min_max!(array, UInt64Array, UInt64),
        DataType::Float32 => process_min_max!(array, Float32Array, Float32),
        DataType::Float64 => process_min_max!(array, Float64Array, Float64),
        DataType::Utf8 => {
            if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
                let min = min_string(array).ok_or_else(|| {
                    DataFusionError::Internal("Empty array".to_string())
                })?;
                let max = max_string(array).ok_or_else(|| {
                    DataFusionError::Internal("Empty array".to_string())
                })?;
                Ok((
                    ScalarValue::Utf8(Some(min.to_string())),
                    ScalarValue::Utf8(Some(max.to_string())),
                ))
            } else {
                Err(DataFusionError::Internal("Invalid array type".to_string()))
            }
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "Min/Max not implemented for type {}",
            array.data_type()
        ))),
    }
}

fn update_range_stats(
    range: &mut Option<(ScalarValue, ScalarValue)>,
    array: &dyn Array,
) -> Result<(), DataFusionError> {
    if array.is_empty() {
        return Ok(());
    }
    let (min, max) = compute_min_max(array)?;

    *range = match range.take() {
        Some((curr_min, curr_max)) => {
            let min_value = match curr_min.partial_cmp(&min) {
                Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal) => {
                    curr_min
                }
                _ => min,
            };
            let max_value = match curr_max.partial_cmp(&max) {
                Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal) => {
                    curr_max
                }
                _ => max,
            };
            Some((min_value, max_value))
        }
        None => Some((min, max)),
    };

    Ok(())
}

fn is_continuous_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Float32 | DataType::Float64)
}

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

    pub fn _filter_probe_batch(
        &self,
        batch: &RecordBatch,
        hashes: &[u64],
        hash_map: &JoinHashMap,
    ) -> Result<(RecordBatch, Vec<u64>), DataFusionError> {
        let left_hash_set = hash_map.extract_unique_keys();

        let mut mask_builder = BooleanBuilder::new();
        for hash in hashes.iter() {
            mask_builder.append_value(left_hash_set.contains(hash));
        }
        let mask = mask_builder.finish();

        let filtered_columns = batch
            .columns()
            .iter()
            .map(|col| match arrow::compute::filter(col, &mask) {
                Ok(array) => Ok(array),
                Err(e) => arrow_err!(e),
            })
            .collect::<Result<Vec<ArrayRef>, DataFusionError>>()?;

        let filtered_batch = RecordBatch::try_new(batch.schema(), filtered_columns)?;

        let filtered_hashes = hashes
            .iter()
            .zip(mask.iter())
            .filter_map(|(hash, keep)| {
                keep.and_then(|k| if k { Some(*hash) } else { None })
            })
            .collect();

        Ok((filtered_batch, filtered_hashes))
    }
}

impl fmt::Debug for PartitionedDynamicFilterInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionedDynamicFilterInfo")
            .field("partition", &self.partition)
            .field("dynamic_filter_info", &"<DynamicFilterInfo>")
            .finish()
    }
}

impl fmt::Debug for DynamicFilterInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamicFilterInfo")
            .field("columns", &self.columns)
            .field("build_side_names", &self.build_side_names)
            .field("inner", &"<locked>")
            .finish()
    }
}
