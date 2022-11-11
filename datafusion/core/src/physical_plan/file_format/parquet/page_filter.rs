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

use arrow::array::{BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array};
use arrow::{array::ArrayRef, datatypes::SchemaRef, error::ArrowError};
use datafusion_common::{Column, DataFusionError, Result};
use datafusion_optimizer::utils::split_conjunction;
use log::{debug, error, trace};
use parquet::{
    arrow::arrow_reader::{RowSelection, RowSelector},
    errors::ParquetError,
    file::{
        metadata::{ParquetMetaData, RowGroupMetaData},
        page_index::index::Index,
    },
    format::PageLocation,
};
use std::collections::VecDeque;
use std::sync::Arc;

use crate::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};

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
        let mut row_selections = VecDeque::with_capacity(page_index_predicates.len());
        for predicate in page_index_predicates {
            // `extract_page_index_push_down_predicates` only return predicate with one col.
            let col_id = *predicate.need_input_columns_ids().iter().next().unwrap();
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
            row_selections.push_back(selectors.into_iter().flatten().collect::<Vec<_>>());
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
        Ok(Some(final_selection.into()))
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
fn combine_multi_col_selection(
    row_selections: VecDeque<Vec<RowSelector>>,
) -> Vec<RowSelector> {
    row_selections
        .into_iter()
        .reduce(intersect_row_selection)
        .unwrap()
}

/// combine two `RowSelection` return the intersection
/// For example:
/// self:     NNYYYYNNY
/// other:    NYNNNNNNY
///
/// returned: NNNNNNNNY
/// set `need_combine` true will combine result: Select(2) + Select(1) + Skip(2) -> Select(3) + Skip(2)
///
/// Move to arrow-rs: https://github.com/apache/arrow-rs/issues/3003
pub(crate) fn intersect_row_selection(
    left: Vec<RowSelector>,
    right: Vec<RowSelector>,
) -> Vec<RowSelector> {
    let mut res = vec![];
    let mut l_iter = left.into_iter().peekable();
    let mut r_iter = right.into_iter().peekable();

    while let (Some(a), Some(b)) = (l_iter.peek_mut(), r_iter.peek_mut()) {
        if a.row_count == 0 {
            l_iter.next().unwrap();
            continue;
        }
        if b.row_count == 0 {
            r_iter.next().unwrap();
            continue;
        }
        match (a.skip, b.skip) {
            // Keep both ranges
            (false, false) => {
                if a.row_count < b.row_count {
                    res.push(RowSelector::select(a.row_count));
                    b.row_count -= a.row_count;
                    l_iter.next().unwrap();
                } else {
                    res.push(RowSelector::select(b.row_count));
                    a.row_count -= b.row_count;
                    r_iter.next().unwrap();
                }
            }
            // skip at least one
            _ => {
                if a.row_count < b.row_count {
                    res.push(RowSelector::skip(a.row_count));
                    b.row_count -= a.row_count;
                    l_iter.next().unwrap();
                } else {
                    res.push(RowSelector::skip(b.row_count));
                    a.row_count -= b.row_count;
                    r_iter.next().unwrap();
                }
            }
        }
    }
    if l_iter.peek().is_some() {
        res.extend(l_iter);
    }
    if r_iter.peek().is_some() {
        res.extend(r_iter);
    }
    // combine the adjacent same operators and last zero row count
    // TODO: remove when https://github.com/apache/arrow-rs/pull/2994 is released~

    let mut pre = res[0];
    let mut after_combine = vec![];
    for selector in res.iter_mut().skip(1) {
        if selector.skip == pre.skip {
            pre.row_count += selector.row_count;
        } else {
            after_combine.push(pre);
            pre = *selector;
        }
    }
    if pre.row_count != 0 {
        after_combine.push(pre);
    }
    after_combine
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
    metrics: &ParquetFileMetrics,
) -> Result<Vec<RowSelector>> {
    let num_rows = group.num_rows() as usize;
    if let (Some(col_offset_indexes), Some(col_page_indexes)) =
        (col_offset_indexes, col_page_indexes)
    {
        let pruning_stats = PagesPruningStatistics {
            col_page_indexes,
            col_offset_indexes,
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
                error!("Error evaluating page index predicate values {}", e);
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
}

// Extract the min or max value calling `func` from page idex
macro_rules! get_min_max_values_for_page_index {
    ($self:expr, $func:ident) => {{
        match $self.col_page_indexes {
            Index::NONE => None,
            Index::INT32(index) => {
                let vec = &index.indexes;
                Some(Arc::new(Int32Array::from_iter(
                    vec.iter().map(|x| x.$func().cloned()),
                )))
            }
            Index::INT64(index) => {
                let vec = &index.indexes;
                Some(Arc::new(Int64Array::from_iter(
                    vec.iter().map(|x| x.$func().cloned()),
                )))
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
            Index::INT96(_) | Index::BYTE_ARRAY(_) | Index::FIXED_LEN_BYTE_ARRAY(_) => {
                //Todo support these type
                None
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_combine_row_selection() {
        // a size equal b size
        let a = vec![
            RowSelector::select(5),
            RowSelector::skip(4),
            RowSelector::select(1),
        ];
        let b = vec![
            RowSelector::select(8),
            RowSelector::skip(1),
            RowSelector::select(1),
        ];

        let res = intersect_row_selection(a, b);
        assert_eq!(
            res,
            vec![
                RowSelector::select(5),
                RowSelector::skip(4),
                RowSelector::select(1)
            ],
        );

        // a size larger than b size
        let a = vec![
            RowSelector::select(3),
            RowSelector::skip(33),
            RowSelector::select(3),
            RowSelector::skip(33),
        ];
        let b = vec![RowSelector::select(36), RowSelector::skip(36)];
        let res = intersect_row_selection(a, b);
        assert_eq!(res, vec![RowSelector::select(3), RowSelector::skip(69)]);

        // a size less than b size
        let a = vec![RowSelector::select(3), RowSelector::skip(7)];
        let b = vec![
            RowSelector::select(2),
            RowSelector::skip(2),
            RowSelector::select(2),
            RowSelector::skip(2),
            RowSelector::select(2),
        ];
        let res = intersect_row_selection(a, b);
        assert_eq!(res, vec![RowSelector::select(2), RowSelector::skip(8)]);

        let a = vec![RowSelector::select(3), RowSelector::skip(7)];
        let b = vec![
            RowSelector::select(2),
            RowSelector::skip(2),
            RowSelector::select(2),
            RowSelector::skip(2),
            RowSelector::select(2),
        ];
        let res = intersect_row_selection(a, b);
        assert_eq!(res, vec![RowSelector::select(2), RowSelector::skip(8),]);
    }
}
