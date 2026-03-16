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

//! [`BatchedVec`] — a Vec-like structure that stores elements in
//! fixed-size batches for efficient batch-at-a-time emission.

use std::fmt::Debug;
use std::mem::size_of;

/// Fixed batch shift (log2 of batch size). 8192 = 2^13.
const BATCH_SHIFT: u32 = 13;
/// Fixed batch size.
const BATCH_SIZE: usize = 1 << BATCH_SHIFT;
/// Bitmask for extracting offset within a batch.
const BATCH_MASK: usize = BATCH_SIZE - 1;

/// A Vec-like container that stores elements in fixed-size batches.
///
/// Indexed by a flat `usize` which is decomposed into
/// `(batch_index, offset)` via bit-shift and mask on the compile-time
/// constant batch size (8192).
///
/// Each inner batch is always exactly `BATCH_SIZE` elements (except
/// possibly the last one during growth). Emission of a single batch
/// is O(1) via [`Self::take_batch`].
#[derive(Debug)]
pub struct BatchedVec<T> {
    batches: Vec<Vec<T>>,
    len: usize,
}

impl<T: Clone> BatchedVec<T> {
    pub fn new() -> Self {
        Self {
            batches: Vec::new(),
            len: 0,
        }
    }

    #[inline(always)]
    fn decompose(index: usize) -> (usize, usize) {
        (index >> BATCH_SHIFT, index & BATCH_MASK)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Ensure capacity for at least `total` elements, filling new
    /// slots with `default_value`.
    pub fn ensure_capacity(&mut self, total: usize, default_value: T) {
        if total <= self.len {
            return;
        }

        // Fill current last batch up to BATCH_SIZE before adding new ones
        if let Some(last) = self.batches.last_mut() {
            let can_add = BATCH_SIZE - last.len();
            let need = total - self.len;
            let add = can_add.min(need);
            if add > 0 {
                last.resize(last.len() + add, default_value.clone());
                self.len += add;
            }
        }

        // Add new batches as needed
        while self.len < total {
            let batch_len = (total - self.len).min(BATCH_SIZE);
            let mut batch = Vec::with_capacity(batch_len);
            batch.resize(batch_len, default_value.clone());
            self.batches.push(batch);
            self.len += batch_len;
        }
    }

    /// # Safety
    ///
    /// `index` must be in bounds (< self.len).
    #[inline(always)]
    pub unsafe fn get_unchecked_mut(&mut self, index: usize) -> &mut T {
        let (batch, offset) = Self::decompose(index);
        unsafe {
            self.batches
                .get_unchecked_mut(batch)
                .get_unchecked_mut(offset)
        }
    }

    /// Number of batches (including a possible partial last batch).
    pub fn num_batches(&self) -> usize {
        self.batches.len()
    }

    /// Take one batch by index, leaving an empty Vec in its place.
    /// This is O(1).
    pub fn take_batch(&mut self, batch_idx: usize) -> Option<Vec<T>> {
        self.batches.get_mut(batch_idx).map(|batch| {
            self.len = self.len.saturating_sub(batch.len());
            std::mem::take(batch)
        })
    }

    /// Take all elements as a flat Vec (for `EmitTo::All`).
    pub fn take_all(&mut self) -> Vec<T> {
        self.len = 0;
        std::mem::take(&mut self.batches)
            .into_iter()
            .flatten()
            .collect()
    }

    /// Take the first `n` elements (for `EmitTo::First(n)`).
    ///
    /// When `n` equals `BATCH_SIZE`, this is zero-copy (just `mem::take`
    /// on the first batch). Otherwise, full batches are drained via
    /// `mem::take` (O(1) each) and only the split-point batch is copied.
    pub fn take_first(&mut self, n: usize) -> Vec<T> {
        assert!(n <= self.len);
        let full_batches = n >> BATCH_SHIFT;
        let remainder = n & BATCH_MASK;

        // Fast path: exactly one full batch, no remainder — zero copy
        if full_batches == 1 && remainder == 0 {
            self.len -= n;
            return self.batches.drain(..1).next().unwrap();
        }

        let mut result = Vec::with_capacity(n);

        // Take full batches — O(1) each
        for batch in self.batches.drain(..full_batches) {
            result.extend(batch);
        }

        // Split the next batch if there's a remainder
        if remainder > 0 && !self.batches.is_empty() {
            let batch = &mut self.batches[0];
            let mut tail = batch.split_off(remainder);
            std::mem::swap(batch, &mut tail);
            result.extend(tail); // tail has [0..remainder]
        }

        self.len -= n;
        result
    }

    /// Total heap memory used by this structure.
    pub fn size(&self) -> usize {
        self.batches
            .iter()
            .map(|b| b.capacity() * size_of::<T>())
            .sum::<usize>()
            + self.batches.capacity() * size_of::<Vec<T>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut v = BatchedVec::new();
        assert_eq!(v.len(), 0);

        v.ensure_capacity(10, 0i32);
        assert_eq!(v.len(), 10);
        assert_eq!(v.num_batches(), 1); // all fit in one batch

        // Write via composite index
        for i in 0..10 {
            unsafe {
                *v.get_unchecked_mut(i) = i as i32;
            }
        }

        // take_all flattens
        let all = v.take_all();
        assert_eq!(&all[..10], &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(v.len(), 0);
    }

    #[test]
    fn test_multi_batch() {
        let mut v = BatchedVec::new();
        // Force multiple batches
        let n = BATCH_SIZE * 2 + 100;
        v.ensure_capacity(n, 0i32);
        assert_eq!(v.len(), n);
        assert_eq!(v.num_batches(), 3);

        for i in 0..n {
            unsafe {
                *v.get_unchecked_mut(i) = i as i32;
            }
        }

        let all = v.take_all();
        assert_eq!(all.len(), n);
        for i in 0..n {
            assert_eq!(all[i], i as i32);
        }
    }

    #[test]
    fn test_take_batch() {
        let mut v = BatchedVec::new();
        let n = BATCH_SIZE * 2 + 5;
        v.ensure_capacity(n, 0i32);
        for i in 0..n {
            unsafe {
                *v.get_unchecked_mut(i) = i as i32;
            }
        }

        let b0 = v.take_batch(0).unwrap();
        assert_eq!(b0.len(), BATCH_SIZE);
        assert_eq!(b0[0], 0);
        assert_eq!(b0[BATCH_SIZE - 1], (BATCH_SIZE - 1) as i32);

        let b1 = v.take_batch(1).unwrap();
        assert_eq!(b1.len(), BATCH_SIZE);
        assert_eq!(b1[0], BATCH_SIZE as i32);

        let b2 = v.take_batch(2).unwrap();
        assert_eq!(b2.len(), 5);
    }

    #[test]
    fn test_take_first() {
        let mut v = BatchedVec::new();
        let n = BATCH_SIZE * 2 + 100;
        v.ensure_capacity(n, 0i32);
        for i in 0..n {
            unsafe {
                *v.get_unchecked_mut(i) = i as i32;
            }
        }

        let first = v.take_first(BATCH_SIZE + 50);
        assert_eq!(first.len(), BATCH_SIZE + 50);
        for i in 0..first.len() {
            assert_eq!(first[i], i as i32);
        }

        let rest = v.take_all();
        assert_eq!(rest.len(), BATCH_SIZE + 50);
        for i in 0..rest.len() {
            assert_eq!(rest[i], (BATCH_SIZE + 50 + i) as i32);
        }
    }

    #[test]
    fn test_ensure_capacity_grows() {
        let mut v = BatchedVec::new();
        v.ensure_capacity(3, 42i32);
        assert_eq!(v.len(), 3);
        assert_eq!(v.num_batches(), 1);
        assert_eq!(v.take_batch(0).unwrap().len(), 3);
    }
}
