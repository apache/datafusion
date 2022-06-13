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

use crate::error;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::PhysicalExpr;
use arrow::array::{ArrayRef, DynComparator};
use arrow::compute::SortOptions;
use arrow::record_batch::RecordBatch;
use hashbrown::HashMap;
use parking_lot::RwLock;
use std::borrow::BorrowMut;
use std::cmp::Ordering;
use std::sync::Arc;

/// A `SortKeyCursor` is created from a `RecordBatch`, and a set of
/// `PhysicalExpr` that when evaluated on the `RecordBatch` yield the sort keys.
///
/// Additionally it maintains a row cursor that can be advanced through the rows
/// of the provided `RecordBatch`
///
/// `SortKeyCursor::compare` can then be used to compare the sort key pointed to
/// by this row cursor, with that of another `SortKeyCursor`. A cursor stores
/// a row comparator for each other cursor that it is compared to.
pub struct SortKeyCursor {
    stream_idx: usize,
    sort_columns: Vec<ArrayRef>,
    cur_row: usize,
    num_rows: usize,

    // An id uniquely identifying the record batch scanned by this cursor.
    batch_id: usize,

    // A collection of comparators that compare rows in this cursor's batch to
    // the cursors in other batches. Other batches are uniquely identified by
    // their batch_idx.
    batch_comparators: RwLock<HashMap<usize, Vec<DynComparator>>>,
    sort_options: Arc<Vec<SortOptions>>,
}

impl std::fmt::Debug for SortKeyCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("SortKeyCursor")
            .field("sort_columns", &self.sort_columns)
            .field("cur_row", &self.cur_row)
            .field("num_rows", &self.num_rows)
            .field("batch_id", &self.batch_id)
            .field("batch_comparators", &"<FUNC>")
            .finish()
    }
}

impl SortKeyCursor {
    /// Create a new SortKeyCursor
    pub fn new(
        stream_idx: usize,
        batch_id: usize,
        batch: &RecordBatch,
        sort_key: &[Arc<dyn PhysicalExpr>],
        sort_options: Arc<Vec<SortOptions>>,
    ) -> error::Result<Self> {
        let sort_columns = sort_key
            .iter()
            .map(|expr| Ok(expr.evaluate(batch)?.into_array(batch.num_rows())))
            .collect::<error::Result<_>>()?;
        Ok(Self {
            stream_idx,
            cur_row: 0,
            num_rows: batch.num_rows(),
            sort_columns,
            batch_id,
            batch_comparators: RwLock::new(HashMap::new()),
            sort_options,
        })
    }

    #[inline(always)]
    /// Return the stream index of this cursor
    pub fn stream_idx(&self) -> usize {
        self.stream_idx
    }

    #[inline(always)]
    /// Return the batch id of this cursor
    pub fn batch_id(&self) -> usize {
        self.batch_id
    }

    #[inline(always)]
    /// Return true if the stream is finished
    pub fn is_finished(&self) -> bool {
        self.num_rows == self.cur_row
    }

    #[inline(always)]
    /// Returns the cursor's current row, and advances the cursor to the next row
    pub fn advance(&mut self) -> usize {
        assert!(!self.is_finished());
        let t = self.cur_row;
        self.cur_row += 1;
        t
    }

    /// Compares the sort key pointed to by this instance's row cursor with that of another
    pub fn compare(&self, other: &SortKeyCursor) -> error::Result<Ordering> {
        if self.sort_columns.len() != other.sort_columns.len() {
            return Err(DataFusionError::Internal(format!(
                "SortKeyCursors had inconsistent column counts: {} vs {}",
                self.sort_columns.len(),
                other.sort_columns.len()
            )));
        }

        if self.sort_columns.len() != self.sort_options.len() {
            return Err(DataFusionError::Internal(format!(
                "Incorrect number of SortOptions provided to SortKeyCursor::compare, expected {} got {}",
                self.sort_columns.len(),
                self.sort_options.len()
            )));
        }

        let zipped: Vec<((&ArrayRef, &ArrayRef), &SortOptions)> = self
            .sort_columns
            .iter()
            .zip(other.sort_columns.iter())
            .zip(self.sort_options.iter())
            .collect::<Vec<_>>();

        self.init_cmp_if_needed(other, &zipped)?;
        let map = self.batch_comparators.read();
        let cmp = map.get(&other.batch_id).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Failed to find comparator for {} cmp {}",
                self.batch_id, other.batch_id
            ))
        })?;

        for (i, ((l, r), sort_options)) in zipped.iter().enumerate() {
            match (l.is_valid(self.cur_row), r.is_valid(other.cur_row)) {
                (false, true) if sort_options.nulls_first => return Ok(Ordering::Less),
                (false, true) => return Ok(Ordering::Greater),
                (true, false) if sort_options.nulls_first => {
                    return Ok(Ordering::Greater)
                }
                (true, false) => return Ok(Ordering::Less),
                (false, false) => {}
                (true, true) => match cmp[i](self.cur_row, other.cur_row) {
                    Ordering::Equal => {}
                    o if sort_options.descending => return Ok(o.reverse()),
                    o => return Ok(o),
                },
            }
        }

        // Break ties using stream_idx to ensure a predictable
        // ordering of rows when comparing equal streams.
        Ok(self.stream_idx.cmp(&other.stream_idx))
    }

    /// Initialize a collection of comparators for comparing
    /// columnar arrays of this cursor and "other" if needed.
    fn init_cmp_if_needed(
        &self,
        other: &SortKeyCursor,
        zipped: &[((&ArrayRef, &ArrayRef), &SortOptions)],
    ) -> Result<()> {
        let hm = self.batch_comparators.read();
        if !hm.contains_key(&other.batch_id) {
            drop(hm);
            let mut map = self.batch_comparators.write();
            let cmp = map
                .borrow_mut()
                .entry(other.batch_id)
                .or_insert_with(|| Vec::with_capacity(other.sort_columns.len()));

            for (i, ((l, r), _)) in zipped.iter().enumerate() {
                if i >= cmp.len() {
                    // initialise comparators
                    cmp.push(arrow::array::build_compare(l.as_ref(), r.as_ref())?);
                }
            }
        }
        Ok(())
    }
}

impl Ord for SortKeyCursor {
    /// Needed by min-heap comparison and reverse the order at the same time.
    fn cmp(&self, other: &Self) -> Ordering {
        other.compare(self).unwrap()
    }
}

impl PartialEq for SortKeyCursor {
    fn eq(&self, other: &Self) -> bool {
        other.compare(self).unwrap() == Ordering::Equal
    }
}

impl Eq for SortKeyCursor {}

impl PartialOrd for SortKeyCursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.compare(self).ok()
    }
}
