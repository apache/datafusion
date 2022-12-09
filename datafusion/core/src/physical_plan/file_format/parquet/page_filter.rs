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

//! Contains code to filter entire pages

use arrow::array::{
    BooleanArray, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray,
};
use arrow::datatypes::DataType;
use arrow::{array::ArrayRef, datatypes::SchemaRef, error::ArrowError};
use datafusion_common::{Column, DataFusionError, Result};
use datafusion_optimizer::utils::split_conjunction;
use log::{debug, trace};
use parquet::schema::types::ColumnDescriptor;
use parquet::{
    arrow::arrow_reader::{RowSelection, RowSelector},
    errors::ParquetError,
    file::{
        metadata::{ParquetMetaData, RowGroupMetaData},
        page_index::index::Index,
    },
    format::PageLocation,
};
use std::sync::Arc;

use crate::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use crate::physical_plan::file_format::parquet::{
    from_bytes_to_i128, parquet_to_arrow_decimal_type,
};

use super::metrics::ParquetFileMetrics;

/// Create a RowSelection that may rule out ranges of rows based on
/// parquet page level statistics, if any.
///
/// For example, given a row group with two column (chunks) for `A`
/// and `B` with the following with page level statistics:
///
/// ```text
/// ┏━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━
///    ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ┃
/// ┃     ┌──────────────┐  │     ┌──────────────┐  │  ┃
/// ┃  │  │              │     │  │              │     ┃
/// ┃     │              │  │     │     Page     │  │
///    │  │              │     │  │      3       │     ┃
/// ┃     │              │  │     │   min: "A"   │  │  ┃
/// ┃  │  │              │     │  │   max: "C"   │     ┃
/// ┃     │     Page     │  │     │ first_row: 0 │  │
///    │  │      1       │     │  │              │     ┃
/// ┃     │   min: 10    │  │     └──────────────┘  │  ┃
/// ┃  │  │   max: 20    │     │  ┌──────────────┐     ┃
/// ┃     │ first_row: 0 │  │     │              │  │
///    │  │              │     │  │     Page     │     ┃
/// ┃     │              │  │     │      4       │  │  ┃
/// ┃  │  │              │     │  │   min: "D"   │     ┃
/// ┃     │              │  │     │   max: "G"   │  │
///    │  │              │     │  │first_row: 100│     ┃
/// ┃     └──────────────┘  │     │              │  │  ┃
/// ┃  │  ┌──────────────┐     │  │              │     ┃
/// ┃     │              │  │     └──────────────┘  │
///    │  │     Page     │     │  ┌──────────────┐     ┃
/// ┃     │      2       │  │     │              │  │  ┃
/// ┃  │  │   min: 30    │     │  │     Page     │     ┃
/// ┃     │   max: 40    │  │     │      5       │  │
///    │  │first_row: 200│     │  │   min: "H"   │     ┃
/// ┃     │              │  │     │   max: "Z"   │  │  ┃
/// ┃  │  │              │     │  │first_row: 250│     ┃
/// ┃     └──────────────┘  │     │              │  │
///    │                       │  └──────────────┘     ┃
/// ┃   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘  ┃
/// ┃       ColumnChunk            ColumnChunk         ┃
/// ┃            A                      B
///  ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━┛
///
///   Total rows: 300
///
/// ```
///
/// Given the predicate `A > 35 AND B = 'F'`:
///
/// Using `A > 35`: can rule out all of values in Page 1 (rows 0 -> 199)
///
/// Using `B = 'F'`: can rule out all vaues in Page 3 and Page 5 (rows 0 -> 99, and 250 -> 299)
///
/// So we can entirely skip rows 0->199 and 250->299 as we know they
/// can not contain rows that match the predicate.
pub(crate) fn build_page_filter(
    pruning_predicate: Option<&PruningPredicate>,
    schema: SchemaRef,
    row_groups: &[usize],
    file_metadata: &ParquetMetaData,
    file_metrics: &ParquetFileMetrics,
) -> Result<Option<RowSelection>> {
    // scoped timer updates on drop
    let _timer_guard = file_metrics.page_index_eval_time.timer();
    let page_index_predicates =
        extract_page_index_push_down_predicates(pruning_predicate, schema)?;

    if page_index_predicates.is_empty() {
        return Ok(None);
    }

    let groups = file_metadata.row_groups();

    let file_offset_indexes = file_metadata.offset_indexes();
    let file_page_indexes = file_metadata.page_indexes();
    if let (Some(file_offset_indexes), Some(file_page_indexes)) =
        (file_offset_indexes, file_page_indexes)
    {
        let mut row_selections = Vec::with_capacity(page_index_predicates.len());
        for predicate in page_index_predicates {
            // `extract_page_index_push_down_predicates` only return predicate with one col.
            //  when building `PruningPredicate`, some single column filter like `abs(i) = 1`
            //  will be rewrite to `lit(true)`, so may have an empty required_columns.
            if let Some(&col_id) = predicate.need_input_columns_ids().iter().next() {
                let mut selectors = Vec::with_capacity(row_groups.len());
                for r in row_groups.iter() {
                    let rg_offset_indexes = file_offset_indexes.get(*r);
                    let rg_page_indexes = file_page_indexes.get(*r);
                    if let (Some(rg_page_indexes), Some(rg_offset_indexes)) =
                        (rg_page_indexes, rg_offset_indexes)
                    {
                        selectors.extend(
                            prune_pages_in_one_row_group(
                                &groups[*r],
                                &predicate,
                                rg_offset_indexes.get(col_id),
                                rg_page_indexes.get(col_id),
                                groups[*r].column(col_id).column_descr(),
                                file_metrics,
                            )
                            .map_err(|e| {
                                ArrowError::ParquetError(format!(
                                    "Fail in prune_pages_in_one_row_group: {}",
                                    e
                                ))
                            }),
                        );
                    } else {
                        trace!(
                        "Did not have enough metadata to prune with page indexes, falling back, falling back to all rows",
                    );
                        // fallback select all rows
                        let all_selected =
                            vec![RowSelector::select(groups[*r].num_rows() as usize)];
                        selectors.push(all_selected);
                    }
                }
                debug!(
                    "Use filter and page index create RowSelection {:?} from predicate: {:?}",
                    &selectors,
                    predicate.predicate_expr(),
                );
                row_selections.push(selectors.into_iter().flatten().collect::<Vec<_>>());
            }
        }
        let final_selection = combine_multi_col_selection(row_selections);
        let total_skip =
            final_selection.iter().fold(
                0,
                |acc, x| {
                    if x.skip {
                        acc + x.row_count
                    } else {
                        acc
                    }
                },
            );
        file_metrics.page_index_rows_filtered.add(total_skip);
        Ok(Some(final_selection))
    } else {
        Ok(None)
    }
}
/// Intersects the [`RowSelector`]s
///
/// For exampe, given:
/// * `RowSelector1: [ Skip(0~199), Read(200~299)]`
/// * `RowSelector2: [ Skip(0~99), Read(100~249), Skip(250~299)]`
///
/// The final selection is the intersection of these  `RowSelector`s:
/// * `final_selection:[ Skip(0~199), Read(200~249), Skip(250~299)]`
fn combine_multi_col_selection(row_selections: Vec<Vec<RowSelector>>) -> RowSelection {
    row_selections
        .into_iter()
        .map(RowSelection::from)
        .reduce(|s1, s2| s1.intersection(&s2))
        .unwrap()
}

// Extract single col pruningPredicate from input predicate for evaluating page Index.
fn extract_page_index_push_down_predicates(
    predicate: Option<&PruningPredicate>,
    schema: SchemaRef,
) -> Result<Vec<PruningPredicate>> {
    let mut one_col_predicates = vec![];
    if let Some(predicate) = predicate {
        let expr = predicate.logical_expr();
        // todo try use CNF rewrite when ready
        let predicates = split_conjunction(expr);
        let mut one_col_expr = vec![];
        predicates
            .into_iter()
            .try_for_each::<_, Result<()>>(|predicate| {
                let columns = predicate.to_columns()?;
                if columns.len() == 1 {
                    one_col_expr.push(predicate);
                }
                Ok(())
            })?;
        one_col_predicates = one_col_expr
            .into_iter()
            .map(|e| PruningPredicate::try_new(e.clone(), schema.clone()))
            .collect::<Result<Vec<_>>>()
            .unwrap_or_default();
    }
    Ok(one_col_predicates)
}

fn prune_pages_in_one_row_group(
    group: &RowGroupMetaData,
    predicate: &PruningPredicate,
    col_offset_indexes: Option<&Vec<PageLocation>>,
    col_page_indexes: Option<&Index>,
    col_desc: &ColumnDescriptor,
    metrics: &ParquetFileMetrics,
) -> Result<Vec<RowSelector>> {
    let num_rows = group.num_rows() as usize;
    if let (Some(col_offset_indexes), Some(col_page_indexes)) =
        (col_offset_indexes, col_page_indexes)
    {
        let target_type = parquet_to_arrow_decimal_type(col_desc);
        let pruning_stats = PagesPruningStatistics {
            col_page_indexes,
            col_offset_indexes,
            target_type: &target_type,
        };

        match predicate.prune(&pruning_stats) {
            Ok(values) => {
                let mut vec = Vec::with_capacity(values.len());
                let row_vec = create_row_count_in_each_page(col_offset_indexes, num_rows);
                assert_eq!(row_vec.len(), values.len());
                let mut sum_row = *row_vec.first().unwrap();
                let mut selected = *values.first().unwrap();
                trace!("Pruned to to {:?} using {:?}", values, pruning_stats);
                for (i, &f) in values.iter().skip(1).enumerate() {
                    if f == selected {
                        sum_row += *row_vec.get(i).unwrap();
                    } else {
                        let selector = if selected {
                            RowSelector::select(sum_row)
                        } else {
                            RowSelector::skip(sum_row)
                        };
                        vec.push(selector);
                        sum_row = *row_vec.get(i).unwrap();
                        selected = f;
                    }
                }

                let selector = if selected {
                    RowSelector::select(sum_row)
                } else {
                    RowSelector::skip(sum_row)
                };
                vec.push(selector);
                return Ok(vec);
            }
            // stats filter array could not be built
            // return a result which will not filter out any pages
            Err(e) => {
                debug!("Error evaluating page index predicate values {}", e);
                metrics.predicate_evaluation_errors.add(1);
                return Ok(vec![RowSelector::select(group.num_rows() as usize)]);
            }
        }
    }
    Err(DataFusionError::ParquetError(ParquetError::General(
        "Got some error in prune_pages_in_one_row_group, plz try open the debuglog mode"
            .to_string(),
    )))
}

fn create_row_count_in_each_page(
    location: &Vec<PageLocation>,
    num_rows: usize,
) -> Vec<usize> {
    let mut vec = Vec::with_capacity(location.len());
    location.windows(2).for_each(|x| {
        let start = x[0].first_row_index as usize;
        let end = x[1].first_row_index as usize;
        vec.push(end - start);
    });
    vec.push(num_rows - location.last().unwrap().first_row_index as usize);
    vec
}

/// Wraps one col page_index in one rowGroup statistics in a way
/// that implements [`PruningStatistics`]
#[derive(Debug)]
struct PagesPruningStatistics<'a> {
    col_page_indexes: &'a Index,
    col_offset_indexes: &'a Vec<PageLocation>,
    // target_type means the logical type in schema: like 'DECIMAL' is the logical type, but the
    // real physical type in parquet file may be `INT32, INT64, FIXED_LEN_BYTE_ARRAY`
    target_type: &'a Option<DataType>,
}

// Extract the min or max value calling `func` from page idex
macro_rules! get_min_max_values_for_page_index {
    ($self:expr, $func:ident) => {{
        match $self.col_page_indexes {
            Index::NONE => None,
            Index::INT32(index) => {
                match $self.target_type {
                    // int32 to decimal with the precision and scale
                    Some(DataType::Decimal128(precision, scale)) => {
                        let vec = &index.indexes;
                        let vec: Vec<Option<i128>> = vec
                            .iter()
                            .map(|x| x.$func().and_then(|x| Some(*x as i128)))
                            .collect();
                        Decimal128Array::from(vec)
                            .with_precision_and_scale(*precision, *scale)
                            .ok()
                            .map(|arr| Arc::new(arr) as ArrayRef)
                    }
                    _ => {
                        let vec = &index.indexes;
                        Some(Arc::new(Int32Array::from_iter(
                            vec.iter().map(|x| x.$func().cloned()),
                        )))
                    }
                }
            }
            Index::INT64(index) => {
                match $self.target_type {
                    // int64 to decimal with the precision and scale
                    Some(DataType::Decimal128(precision, scale)) => {
                        let vec = &index.indexes;
                        let vec: Vec<Option<i128>> = vec
                            .iter()
                            .map(|x| x.$func().and_then(|x| Some(*x as i128)))
                            .collect();
                        Decimal128Array::from(vec)
                            .with_precision_and_scale(*precision, *scale)
                            .ok()
                            .map(|arr| Arc::new(arr) as ArrayRef)
                    }
                    _ => {
                        let vec = &index.indexes;
                        Some(Arc::new(Int64Array::from_iter(
                            vec.iter().map(|x| x.$func().cloned()),
                        )))
                    }
                }
            }
            Index::FLOAT(index) => {
                let vec = &index.indexes;
                Some(Arc::new(Float32Array::from_iter(
                    vec.iter().map(|x| x.$func().cloned()),
                )))
            }
            Index::DOUBLE(index) => {
                let vec = &index.indexes;
                Some(Arc::new(Float64Array::from_iter(
                    vec.iter().map(|x| x.$func().cloned()),
                )))
            }
            Index::BOOLEAN(index) => {
                let vec = &index.indexes;
                Some(Arc::new(BooleanArray::from_iter(
                    vec.iter().map(|x| x.$func().cloned()),
                )))
            }
            Index::BYTE_ARRAY(index) => {
                let vec = &index.indexes;
                let array: StringArray = vec
                    .iter()
                    .map(|x| x.$func())
                    .map(|x| x.and_then(|x| std::str::from_utf8(x).ok()))
                    .collect();
                Some(Arc::new(array))
            }
            Index::INT96(_) => {
                //Todo support these type
                None
            }
            Index::FIXED_LEN_BYTE_ARRAY(index) => match $self.target_type {
                Some(DataType::Decimal128(precision, scale)) => {
                    let vec = &index.indexes;
                    Decimal128Array::from(
                        vec.iter()
                            .map(|x| x.$func().and_then(|x| Some(from_bytes_to_i128(x))))
                            .collect::<Vec<Option<i128>>>(),
                    )
                    .with_precision_and_scale(*precision, *scale)
                    .ok()
                    .map(|arr| Arc::new(arr) as ArrayRef)
                }
                _ => None,
            },
        }
    }};
}

impl<'a> PruningStatistics for PagesPruningStatistics<'a> {
    fn min_values(&self, _column: &Column) -> Option<ArrayRef> {
        get_min_max_values_for_page_index!(self, min)
    }

    fn max_values(&self, _column: &Column) -> Option<ArrayRef> {
        get_min_max_values_for_page_index!(self, max)
    }

    fn num_containers(&self) -> usize {
        self.col_offset_indexes.len()
    }

    fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
        match self.col_page_indexes {
            Index::NONE => None,
            Index::BOOLEAN(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
            Index::INT32(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
            Index::INT64(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
            Index::FLOAT(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
            Index::DOUBLE(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
            Index::INT96(_) | Index::BYTE_ARRAY(_) | Index::FIXED_LEN_BYTE_ARRAY(_) => {
                // Todo support these types
                None
            }
        }
    }
}
