// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations under
// the License.

//! An implementation of the [TDigest sketch algorithm] providing approximate
//! quantile calculations.
//!
//! The TDigest code in this module is modified from
//! <https://github.com/MnO2/t-digest>, itself a rust reimplementation of
//! [Facebook's Folly TDigest] implementation.
//!
//! Alterations include reduction of runtime heap allocations, broader type
//! support, (de-)serialisation support, reduced type conversions and null value
//! tolerance.
//!
//! [TDigest sketch algorithm]: https://arxiv.org/abs/1902.04023
//! [Facebook's Folly TDigest]: https://github.com/facebook/folly/blob/main/folly/stats/TDigest.h

use arrow::datatypes::DataType;
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use ordered_float::OrderedFloat;
use std::cmp::Ordering;

pub const DEFAULT_MAX_SIZE: usize = 100;

// Cast a non-null [`ScalarValue::Float64`] to an [`OrderedFloat<f64>`], or
// panic.
macro_rules! cast_scalar_f64 {
    ($value:expr ) => {
        match &$value {
            ScalarValue::Float64(Some(v)) => OrderedFloat::from(*v),
            v => panic!("invalid type {:?}", v),
        }
    };
}

/// This trait is implemented for each type a [`TDigest`] can operate on,
/// allowing it to support both numerical rust types (obtained from
/// `PrimitiveArray` instances), and [`ScalarValue`] instances.
pub(crate) trait TryIntoOrderedF64 {
    /// A fallible conversion of a possibly null `self` into a [`OrderedFloat<f64>`].
    ///
    /// If `self` is null, this method must return `Ok(None)`.
    ///
    /// If `self` cannot be coerced to the desired type, this method must return
    /// an `Err` variant.
    fn try_as_f64(&self) -> Result<Option<OrderedFloat<f64>>>;
}

/// Generate an infallible conversion from `type` to an [`OrderedFloat<f64>`].
macro_rules! impl_try_ordered_f64 {
    ($type:ty) => {
        impl TryIntoOrderedF64 for $type {
            fn try_as_f64(&self) -> Result<Option<OrderedFloat<f64>>> {
                Ok(Some(OrderedFloat::from(*self as f64)))
            }
        }
    };
}

impl_try_ordered_f64!(f64);
impl_try_ordered_f64!(f32);
impl_try_ordered_f64!(i64);
impl_try_ordered_f64!(i32);
impl_try_ordered_f64!(i16);
impl_try_ordered_f64!(i8);
impl_try_ordered_f64!(u64);
impl_try_ordered_f64!(u32);
impl_try_ordered_f64!(u16);
impl_try_ordered_f64!(u8);

impl TryIntoOrderedF64 for ScalarValue {
    fn try_as_f64(&self) -> Result<Option<OrderedFloat<f64>>> {
        match self {
            ScalarValue::Float32(v) => Ok(v.map(|v| OrderedFloat::from(v as f64))),
            ScalarValue::Float64(v) => Ok(v.map(|v| OrderedFloat::from(v as f64))),
            ScalarValue::Int8(v) => Ok(v.map(|v| OrderedFloat::from(v as f64))),
            ScalarValue::Int16(v) => Ok(v.map(|v| OrderedFloat::from(v as f64))),
            ScalarValue::Int32(v) => Ok(v.map(|v| OrderedFloat::from(v as f64))),
            ScalarValue::Int64(v) => Ok(v.map(|v| OrderedFloat::from(v as f64))),
            ScalarValue::UInt8(v) => Ok(v.map(|v| OrderedFloat::from(v as f64))),
            ScalarValue::UInt16(v) => Ok(v.map(|v| OrderedFloat::from(v as f64))),
            ScalarValue::UInt32(v) => Ok(v.map(|v| OrderedFloat::from(v as f64))),
            ScalarValue::UInt64(v) => Ok(v.map(|v| OrderedFloat::from(v as f64))),

            got => Err(DataFusionError::NotImplemented(format!(
                "Support for 'TryIntoOrderedF64' for data type {} is not implemented",
                got
            ))),
        }
    }
}

/// Centroid implementation to the cluster mentioned in the paper.
#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct Centroid {
    mean: OrderedFloat<f64>,
    weight: OrderedFloat<f64>,
}

impl PartialOrd for Centroid {
    fn partial_cmp(&self, other: &Centroid) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Centroid {
    fn cmp(&self, other: &Centroid) -> Ordering {
        self.mean.cmp(&other.mean)
    }
}

impl Centroid {
    pub(crate) fn new(
        mean: impl Into<OrderedFloat<f64>>,
        weight: impl Into<OrderedFloat<f64>>,
    ) -> Self {
        Centroid {
            mean: mean.into(),
            weight: weight.into(),
        }
    }

    #[inline]
    pub(crate) fn mean(&self) -> OrderedFloat<f64> {
        self.mean
    }

    #[inline]
    pub(crate) fn weight(&self) -> OrderedFloat<f64> {
        self.weight
    }

    pub(crate) fn add(
        &mut self,
        sum: impl Into<OrderedFloat<f64>>,
        weight: impl Into<OrderedFloat<f64>>,
    ) -> f64 {
        let new_sum = sum.into() + self.weight * self.mean;
        let new_weight = self.weight + weight.into();
        self.weight = new_weight;
        self.mean = new_sum / new_weight;
        new_sum.into_inner()
    }
}

impl Default for Centroid {
    fn default() -> Self {
        Centroid {
            mean: OrderedFloat::from(0.0),
            weight: OrderedFloat::from(1.0),
        }
    }
}

/// T-Digest to be operated on.
#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct TDigest {
    centroids: Vec<Centroid>,
    max_size: usize,
    sum: OrderedFloat<f64>,
    count: OrderedFloat<f64>,
    max: OrderedFloat<f64>,
    min: OrderedFloat<f64>,
}

impl TDigest {
    pub(crate) fn new(max_size: usize) -> Self {
        TDigest {
            centroids: Vec::new(),
            max_size,
            sum: OrderedFloat::from(0.0),
            count: OrderedFloat::from(0.0),
            max: OrderedFloat::from(std::f64::NAN),
            min: OrderedFloat::from(std::f64::NAN),
        }
    }

    pub(crate) fn new_with_centroid(max_size: usize, centroid: Centroid) -> Self {
        TDigest {
            centroids: vec![centroid.clone()],
            max_size,
            sum: centroid.mean * centroid.weight,
            count: OrderedFloat::from(1.0),
            max: centroid.mean,
            min: centroid.mean,
        }
    }

    #[inline]
    pub(crate) fn count(&self) -> f64 {
        self.count.into_inner()
    }

    #[inline]
    pub(crate) fn max(&self) -> f64 {
        self.max.into_inner()
    }

    #[inline]
    pub(crate) fn min(&self) -> f64 {
        self.min.into_inner()
    }

    #[inline]
    pub(crate) fn max_size(&self) -> usize {
        self.max_size
    }
}

impl Default for TDigest {
    fn default() -> Self {
        TDigest {
            centroids: Vec::new(),
            max_size: 100,
            sum: OrderedFloat::from(0.0),
            count: OrderedFloat::from(0.0),
            max: OrderedFloat::from(std::f64::NAN),
            min: OrderedFloat::from(std::f64::NAN),
        }
    }
}

impl TDigest {
    fn k_to_q(k: f64, d: f64) -> OrderedFloat<f64> {
        let k_div_d = k / d;
        if k_div_d >= 0.5 {
            let base = 1.0 - k_div_d;
            1.0 - 2.0 * base * base
        } else {
            2.0 * k_div_d * k_div_d
        }
        .into()
    }

    fn clamp(
        v: OrderedFloat<f64>,
        lo: OrderedFloat<f64>,
        hi: OrderedFloat<f64>,
    ) -> OrderedFloat<f64> {
        if v > hi {
            hi
        } else if v < lo {
            lo
        } else {
            v
        }
    }

    pub(crate) fn merge_unsorted_f64(
        &self,
        unsorted_values: Vec<OrderedFloat<f64>>,
    ) -> TDigest {
        let mut values = unsorted_values;
        values.sort();
        self.merge_sorted_f64(&values)
    }

    fn merge_sorted_f64(&self, sorted_values: &[OrderedFloat<f64>]) -> TDigest {
        #[cfg(debug_assertions)]
        debug_assert!(is_sorted(sorted_values), "unsorted input to TDigest");

        if sorted_values.is_empty() {
            return self.clone();
        }

        let mut result = TDigest::new(self.max_size());
        result.count = OrderedFloat::from(self.count() + (sorted_values.len() as f64));

        let maybe_min = *sorted_values.first().unwrap();
        let maybe_max = *sorted_values.last().unwrap();

        if self.count() > 0.0 {
            result.min = std::cmp::min(self.min, maybe_min);
            result.max = std::cmp::max(self.max, maybe_max);
        } else {
            result.min = maybe_min;
            result.max = maybe_max;
        }

        let mut compressed: Vec<Centroid> = Vec::with_capacity(self.max_size);

        let mut k_limit: f64 = 1.0;
        let mut q_limit_times_count =
            Self::k_to_q(k_limit, self.max_size as f64) * result.count();
        k_limit += 1.0;

        let mut iter_centroids = self.centroids.iter().peekable();
        let mut iter_sorted_values = sorted_values.iter().peekable();

        let mut curr: Centroid = if let Some(c) = iter_centroids.peek() {
            let curr = **iter_sorted_values.peek().unwrap();
            if c.mean() < curr {
                iter_centroids.next().unwrap().clone()
            } else {
                Centroid::new(*iter_sorted_values.next().unwrap(), 1.0)
            }
        } else {
            Centroid::new(*iter_sorted_values.next().unwrap(), 1.0)
        };

        let mut weight_so_far = curr.weight();

        let mut sums_to_merge = OrderedFloat::from(0.0);
        let mut weights_to_merge = OrderedFloat::from(0.0);

        while iter_centroids.peek().is_some() || iter_sorted_values.peek().is_some() {
            let next: Centroid = if let Some(c) = iter_centroids.peek() {
                if iter_sorted_values.peek().is_none()
                    || c.mean() < **iter_sorted_values.peek().unwrap()
                {
                    iter_centroids.next().unwrap().clone()
                } else {
                    Centroid::new(*iter_sorted_values.next().unwrap(), 1.0)
                }
            } else {
                Centroid::new(*iter_sorted_values.next().unwrap(), 1.0)
            };

            let next_sum = next.mean() * next.weight();
            weight_so_far += next.weight();

            if weight_so_far <= q_limit_times_count {
                sums_to_merge += next_sum;
                weights_to_merge += next.weight();
            } else {
                result.sum = OrderedFloat::from(
                    result.sum.into_inner() + curr.add(sums_to_merge, weights_to_merge),
                );
                sums_to_merge = 0.0.into();
                weights_to_merge = 0.0.into();

                compressed.push(curr.clone());
                q_limit_times_count =
                    Self::k_to_q(k_limit, self.max_size as f64) * result.count();
                k_limit += 1.0;
                curr = next;
            }
        }

        result.sum = OrderedFloat::from(
            result.sum.into_inner() + curr.add(sums_to_merge, weights_to_merge),
        );
        compressed.push(curr);
        compressed.shrink_to_fit();
        compressed.sort();

        result.centroids = compressed;
        result
    }

    fn external_merge(
        centroids: &mut [Centroid],
        first: usize,
        middle: usize,
        last: usize,
    ) {
        let mut result: Vec<Centroid> = Vec::with_capacity(centroids.len());

        let mut i = first;
        let mut j = middle;

        while i < middle && j < last {
            match centroids[i].cmp(&centroids[j]) {
                Ordering::Less => {
                    result.push(centroids[i].clone());
                    i += 1;
                }
                Ordering::Greater => {
                    result.push(centroids[j].clone());
                    j += 1;
                }
                Ordering::Equal => {
                    result.push(centroids[i].clone());
                    i += 1;
                }
            }
        }

        while i < middle {
            result.push(centroids[i].clone());
            i += 1;
        }

        while j < last {
            result.push(centroids[j].clone());
            j += 1;
        }

        i = first;
        for centroid in result.into_iter() {
            centroids[i] = centroid;
            i += 1;
        }
    }

    // Merge multiple T-Digests
    pub(crate) fn merge_digests(digests: &[TDigest]) -> TDigest {
        let n_centroids: usize = digests.iter().map(|d| d.centroids.len()).sum();
        if n_centroids == 0 {
            return TDigest::default();
        }

        let max_size = digests.first().unwrap().max_size;
        let mut centroids: Vec<Centroid> = Vec::with_capacity(n_centroids);
        let mut starts: Vec<usize> = Vec::with_capacity(digests.len());

        let mut count: f64 = 0.0;
        let mut min = OrderedFloat::from(std::f64::INFINITY);
        let mut max = OrderedFloat::from(std::f64::NEG_INFINITY);

        let mut start: usize = 0;
        for digest in digests.iter() {
            starts.push(start);

            let curr_count: f64 = digest.count();
            if curr_count > 0.0 {
                min = std::cmp::min(min, digest.min);
                max = std::cmp::max(max, digest.max);
                count += curr_count;
                for centroid in &digest.centroids {
                    centroids.push(centroid.clone());
                    start += 1;
                }
            }
        }

        let mut digests_per_block: usize = 1;
        while digests_per_block < starts.len() {
            for i in (0..starts.len()).step_by(digests_per_block * 2) {
                if i + digests_per_block < starts.len() {
                    let first = starts[i];
                    let middle = starts[i + digests_per_block];
                    let last = if i + 2 * digests_per_block < starts.len() {
                        starts[i + 2 * digests_per_block]
                    } else {
                        centroids.len()
                    };

                    debug_assert!(first <= middle && middle <= last);
                    Self::external_merge(&mut centroids, first, middle, last);
                }
            }

            digests_per_block *= 2;
        }

        let mut result = TDigest::new(max_size);
        let mut compressed: Vec<Centroid> = Vec::with_capacity(max_size);

        let mut k_limit: f64 = 1.0;
        let mut q_limit_times_count =
            Self::k_to_q(k_limit, max_size as f64) * (count as f64);

        let mut iter_centroids = centroids.iter_mut();
        let mut curr = iter_centroids.next().unwrap();
        let mut weight_so_far = curr.weight();
        let mut sums_to_merge = OrderedFloat::from(0.0);
        let mut weights_to_merge = OrderedFloat::from(0.0);

        for centroid in iter_centroids {
            weight_so_far += centroid.weight();

            if weight_so_far <= q_limit_times_count {
                sums_to_merge += centroid.mean() * centroid.weight();
                weights_to_merge += centroid.weight();
            } else {
                result.sum = OrderedFloat::from(
                    result.sum.into_inner() + curr.add(sums_to_merge, weights_to_merge),
                );
                sums_to_merge = OrderedFloat::from(0.0);
                weights_to_merge = OrderedFloat::from(0.0);
                compressed.push(curr.clone());
                q_limit_times_count =
                    Self::k_to_q(k_limit, max_size as f64) * (count as f64);
                k_limit += 1.0;
                curr = centroid;
            }
        }

        result.sum = OrderedFloat::from(
            result.sum.into_inner() + curr.add(sums_to_merge, weights_to_merge),
        );
        compressed.push(curr.clone());
        compressed.shrink_to_fit();
        compressed.sort();

        result.count = OrderedFloat::from(count as f64);
        result.min = min;
        result.max = max;
        result.centroids = compressed;
        result
    }

    /// To estimate the value located at `q` quantile
    pub(crate) fn estimate_quantile(&self, q: f64) -> f64 {
        if self.centroids.is_empty() {
            return 0.0;
        }

        let count_ = self.count;
        let rank = OrderedFloat::from(q) * count_;

        let mut pos: usize;
        let mut t;
        if q > 0.5 {
            if q >= 1.0 {
                return self.max();
            }

            pos = 0;
            t = count_;

            for (k, centroid) in self.centroids.iter().enumerate().rev() {
                t -= centroid.weight();

                if rank >= t {
                    pos = k;
                    break;
                }
            }
        } else {
            if q <= 0.0 {
                return self.min();
            }

            pos = self.centroids.len() - 1;
            t = OrderedFloat::from(0.0);

            for (k, centroid) in self.centroids.iter().enumerate() {
                if rank < t + centroid.weight() {
                    pos = k;
                    break;
                }

                t += centroid.weight();
            }
        }

        let mut delta = OrderedFloat::from(0.0);
        let mut min = self.min;
        let mut max = self.max;

        if self.centroids.len() > 1 {
            if pos == 0 {
                delta = self.centroids[pos + 1].mean() - self.centroids[pos].mean();
                max = self.centroids[pos + 1].mean();
            } else if pos == (self.centroids.len() - 1) {
                delta = self.centroids[pos].mean() - self.centroids[pos - 1].mean();
                min = self.centroids[pos - 1].mean();
            } else {
                delta = (self.centroids[pos + 1].mean() - self.centroids[pos - 1].mean())
                    / 2.0;
                min = self.centroids[pos - 1].mean();
                max = self.centroids[pos + 1].mean();
            }
        }

        let value = self.centroids[pos].mean()
            + ((rank - t) / self.centroids[pos].weight() - 0.5) * delta;
        Self::clamp(value, min, max).into_inner()
    }

    /// This method decomposes the [`TDigest`] and its [`Centroid`] instances
    /// into a series of primitive scalar values.
    ///
    /// First the values of the TDigest are packed, followed by the variable
    /// number of centroids packed into a [`ScalarValue::List`] of
    /// [`ScalarValue::Float64`]:
    ///
    /// ```text
    ///
    ///    ┌────────┬────────┬────────┬───────┬────────┬────────┐
    ///    │max_size│  sum   │ count  │  max  │  min   │centroid│
    ///    └────────┴────────┴────────┴───────┴────────┴────────┘
    ///                                                     │
    ///                               ┌─────────────────────┘
    ///                               ▼
    ///                          ┌ List ───┐
    ///                          │┌ ─ ─ ─ ┐│
    ///                          │  mean   │
    ///                          │├ ─ ─ ─ ┼│─ ─ Centroid 1
    ///                          │ weight  │
    ///                          │└ ─ ─ ─ ┘│
    ///                          │         │
    ///                          │┌ ─ ─ ─ ┐│
    ///                          │  mean   │
    ///                          │├ ─ ─ ─ ┼│─ ─ Centroid 2
    ///                          │ weight  │
    ///                          │└ ─ ─ ─ ┘│
    ///                          │         │
    ///                              ...
    ///
    /// ```
    ///
    /// The [`TDigest::from_scalar_state()`] method reverses this processes,
    /// consuming the output of this method and returning an unpacked
    /// [`TDigest`].
    pub(crate) fn to_scalar_state(&self) -> Vec<ScalarValue> {
        // Gather up all the centroids
        let centroids: Vec<_> = self
            .centroids
            .iter()
            .flat_map(|c| [c.mean().into_inner(), c.weight().into_inner()])
            .map(|v| ScalarValue::Float64(Some(v)))
            .collect();

        vec![
            ScalarValue::UInt64(Some(self.max_size as u64)),
            ScalarValue::Float64(Some(self.sum.into_inner())),
            ScalarValue::Float64(Some(self.count.into_inner())),
            ScalarValue::Float64(Some(self.max.into_inner())),
            ScalarValue::Float64(Some(self.min.into_inner())),
            ScalarValue::new_list(Some(centroids), DataType::Float64),
        ]
    }

    /// Unpack the serialised state of a [`TDigest`] produced by
    /// [`Self::to_scalar_state()`].
    ///
    /// # Correctness
    ///
    /// Providing input to this method that was not obtained from
    /// [`Self::to_scalar_state()`] results in undefined behaviour and may
    /// panic.
    pub(crate) fn from_scalar_state(state: &[ScalarValue]) -> Self {
        assert_eq!(state.len(), 6, "invalid TDigest state");

        let max_size = match &state[0] {
            ScalarValue::UInt64(Some(v)) => *v as usize,
            v => panic!("invalid max_size type {:?}", v),
        };

        let centroids: Vec<_> = match &state[5] {
            ScalarValue::List(Some(c), f) if *f.data_type() == DataType::Float64 => c
                .chunks(2)
                .map(|v| Centroid::new(cast_scalar_f64!(v[0]), cast_scalar_f64!(v[1])))
                .collect(),
            v => panic!("invalid centroids type {:?}", v),
        };

        let max = cast_scalar_f64!(&state[3]);
        let min = cast_scalar_f64!(&state[4]);
        assert!(max >= min);

        Self {
            max_size,
            sum: cast_scalar_f64!(state[1]),
            count: cast_scalar_f64!(&state[2]),
            max,
            min,
            centroids,
        }
    }
}

#[cfg(debug_assertions)]
fn is_sorted(values: &[OrderedFloat<f64>]) -> bool {
    values.windows(2).all(|w| w[0] <= w[1])
}

#[cfg(test)]
mod tests {
    use super::*;

    // A macro to assert the specified `quantile` estimated by `t` is within the
    // allowable relative error bound.
    macro_rules! assert_error_bounds {
        ($t:ident, quantile = $quantile:literal, want = $want:literal) => {
            assert_error_bounds!(
                $t,
                quantile = $quantile,
                want = $want,
                allowable_error = 0.01
            )
        };
        ($t:ident, quantile = $quantile:literal, want = $want:literal, allowable_error = $re:literal) => {
            let ans = $t.estimate_quantile($quantile);
            let expected: f64 = $want;
            let percentage: f64 = (expected - ans).abs() / expected;
            assert!(
                percentage < $re,
                "relative error {} is more than {}% (got quantile {}, want {})",
                percentage,
                $re,
                ans,
                expected
            );
        };
    }

    macro_rules! assert_state_roundtrip {
        ($t:ident) => {
            let state = $t.to_scalar_state();
            let other = TDigest::from_scalar_state(&state);
            assert_eq!($t, other);
        };
    }

    #[test]
    fn test_int64_uniform() {
        let values = (1i64..=1000)
            .map(|v| OrderedFloat::from(v as f64))
            .collect();

        let t = TDigest::new(100);
        let t = t.merge_unsorted_f64(values);

        assert_error_bounds!(t, quantile = 0.1, want = 100.0);
        assert_error_bounds!(t, quantile = 0.5, want = 500.0);
        assert_error_bounds!(t, quantile = 0.9, want = 900.0);
        assert_state_roundtrip!(t);
    }

    #[test]
    fn test_centroid_addition_regression() {
        // https://github.com/MnO2/t-digest/pull/1

        let vals = vec![1.0, 1.0, 1.0, 2.0, 1.0, 1.0];
        let mut t = TDigest::new(10);

        for v in vals {
            t = t.merge_unsorted_f64(vec![OrderedFloat::from(v as f64)]);
        }

        assert_error_bounds!(t, quantile = 0.5, want = 1.0);
        assert_error_bounds!(t, quantile = 0.95, want = 2.0);
        assert_state_roundtrip!(t);
    }

    #[test]
    fn test_merge_unsorted_against_uniform_distro() {
        let t = TDigest::new(100);
        let values: Vec<_> = (1..=1_000_000)
            .map(f64::from)
            .map(|v| OrderedFloat::from(v as f64))
            .collect();

        let t = t.merge_unsorted_f64(values);

        assert_error_bounds!(t, quantile = 1.0, want = 1_000_000.0);
        assert_error_bounds!(t, quantile = 0.99, want = 990_000.0);
        assert_error_bounds!(t, quantile = 0.01, want = 10_000.0);
        assert_error_bounds!(t, quantile = 0.0, want = 1.0);
        assert_error_bounds!(t, quantile = 0.5, want = 500_000.0);
        assert_state_roundtrip!(t);
    }

    #[test]
    fn test_merge_unsorted_against_skewed_distro() {
        let t = TDigest::new(100);
        let mut values: Vec<_> = (1..=600_000)
            .map(f64::from)
            .map(|v| OrderedFloat::from(v as f64))
            .collect();
        for _ in 0..400_000 {
            values.push(OrderedFloat::from(1_000_000_f64));
        }

        let t = t.merge_unsorted_f64(values);

        assert_error_bounds!(t, quantile = 0.99, want = 1_000_000.0);
        assert_error_bounds!(t, quantile = 0.01, want = 10_000.0);
        assert_error_bounds!(t, quantile = 0.5, want = 500_000.0);
        assert_state_roundtrip!(t);
    }

    #[test]
    fn test_merge_digests() {
        let mut digests: Vec<TDigest> = Vec::new();

        for _ in 1..=100 {
            let t = TDigest::new(100);
            let values: Vec<_> = (1..=1_000)
                .map(f64::from)
                .map(|v| OrderedFloat::from(v as f64))
                .collect();
            let t = t.merge_unsorted_f64(values);
            digests.push(t)
        }

        let t = TDigest::merge_digests(&digests);

        assert_error_bounds!(t, quantile = 1.0, want = 1000.0);
        assert_error_bounds!(t, quantile = 0.99, want = 990.0);
        assert_error_bounds!(t, quantile = 0.01, want = 10.0, allowable_error = 0.2);
        assert_error_bounds!(t, quantile = 0.0, want = 1.0);
        assert_error_bounds!(t, quantile = 0.5, want = 500.0);
        assert_state_roundtrip!(t);
    }
}
