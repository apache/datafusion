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

//! This module provides data structures to represent statistics

use std::fmt::{self, Debug, Display};

use crate::{Result, ScalarValue};

use crate::error::_plan_err;
use crate::utils::aggregate::precision_add;
use arrow::datatypes::{DataType, Schema};

/// Represents a value with a degree of certainty. `Precision` is used to
/// propagate information the precision of statistical values.
#[derive(Clone, PartialEq, Eq, Default, Copy)]
pub enum Precision<T: Debug + Clone + PartialEq + Eq + PartialOrd> {
    /// The exact value is known. Used for guaranteeing correctness.
    ///
    /// Comes from definitive sources such as:
    /// - Parquet file metadata (row counts, byte sizes)
    /// - In-memory RecordBatch data (actual row counts, byte sizes, null counts)
    /// - and more...
    Exact(T),
    /// The value is not known exactly, but is likely close to this value.
    /// Used for cost-based optimizations.
    ///
    /// Some operations that would result in `Inexact(T)` would be:
    /// - Applying a filter (selectivity is unknown)
    /// - Mixing exact and inexact values in arithmetic
    /// - and more...
    Inexact(T),
    /// Nothing is known about the value. This is the default state.
    ///
    /// Acts as an absorbing element in arithmetic -> any operation
    /// involving `Absent` yields `Absent`. [`Precision::to_inexact`]
    /// on `Absent` returns `Absent`, not `Inexact` — it represents
    /// a fundamentally different state.
    ///
    /// Common sources include:
    /// - Data sources without statistics
    /// - Parquet columns missing from file metadata
    /// - Statistics that cannot be derived for an operation (e.g.,
    ///   `distinct_count` after a union, `total_byte_size` for joins)
    #[default]
    Absent,
}

impl<T: Debug + Clone + PartialEq + Eq + PartialOrd> Precision<T> {
    /// If we have some value (exact or inexact), it returns that value.
    /// Otherwise, it returns `None`.
    pub fn get_value(&self) -> Option<&T> {
        match self {
            Precision::Exact(value) | Precision::Inexact(value) => Some(value),
            Precision::Absent => None,
        }
    }

    /// Transform the value in this [`Precision`] object, if one exists, using
    /// the given function. Preserves the exactness state.
    pub fn map<U, F>(self, f: F) -> Precision<U>
    where
        F: Fn(T) -> U,
        U: Debug + Clone + PartialEq + Eq + PartialOrd,
    {
        match self {
            Precision::Exact(val) => Precision::Exact(f(val)),
            Precision::Inexact(val) => Precision::Inexact(f(val)),
            _ => Precision::<U>::Absent,
        }
    }

    /// Returns `Some(true)` if we have an exact value, `Some(false)` if we
    /// have an inexact value, and `None` if there is no value.
    pub fn is_exact(&self) -> Option<bool> {
        match self {
            Precision::Exact(_) => Some(true),
            Precision::Inexact(_) => Some(false),
            _ => None,
        }
    }

    /// Returns the maximum of two (possibly inexact) values, conservatively
    /// propagating exactness information. If one of the input values is
    /// [`Precision::Absent`], the result is `Absent` too.
    pub fn max(&self, other: &Precision<T>) -> Precision<T> {
        match (self, other) {
            (Precision::Exact(a), Precision::Exact(b)) => {
                Precision::Exact(if a >= b { a.clone() } else { b.clone() })
            }
            (Precision::Inexact(a), Precision::Exact(b))
            | (Precision::Exact(a), Precision::Inexact(b))
            | (Precision::Inexact(a), Precision::Inexact(b)) => {
                Precision::Inexact(if a >= b { a.clone() } else { b.clone() })
            }
            (_, _) => Precision::Absent,
        }
    }

    /// Returns the minimum of two (possibly inexact) values, conservatively
    /// propagating exactness information. If one of the input values is
    /// [`Precision::Absent`], the result is `Absent` too.
    pub fn min(&self, other: &Precision<T>) -> Precision<T> {
        match (self, other) {
            (Precision::Exact(a), Precision::Exact(b)) => {
                Precision::Exact(if a >= b { b.clone() } else { a.clone() })
            }
            (Precision::Inexact(a), Precision::Exact(b))
            | (Precision::Exact(a), Precision::Inexact(b))
            | (Precision::Inexact(a), Precision::Inexact(b)) => {
                Precision::Inexact(if a >= b { b.clone() } else { a.clone() })
            }
            (_, _) => Precision::Absent,
        }
    }

    /// Demotes the precision state from exact to inexact (if present).
    pub fn to_inexact(self) -> Self {
        match self {
            Precision::Exact(value) => Precision::Inexact(value),
            _ => self,
        }
    }
}

impl Precision<usize> {
    /// Calculates the sum of two (possibly inexact) [`usize`] values,
    /// conservatively propagating exactness information. If one of the input
    /// values is [`Precision::Absent`], the result is `Absent` too.
    pub fn add(&self, other: &Precision<usize>) -> Precision<usize> {
        match (self, other) {
            (Precision::Exact(a), Precision::Exact(b)) => a.checked_add(*b).map_or_else(
                || Precision::Inexact(a.saturating_add(*b)),
                Precision::Exact,
            ),
            (Precision::Inexact(a), Precision::Exact(b))
            | (Precision::Exact(a), Precision::Inexact(b))
            | (Precision::Inexact(a), Precision::Inexact(b)) => {
                Precision::Inexact(a.saturating_add(*b))
            }
            (_, _) => Precision::Absent,
        }
    }

    /// Calculates the difference of two (possibly inexact) [`usize`] values,
    /// conservatively propagating exactness information. If one of the input
    /// values is [`Precision::Absent`], the result is `Absent` too.
    pub fn sub(&self, other: &Precision<usize>) -> Precision<usize> {
        match (self, other) {
            (Precision::Exact(a), Precision::Exact(b)) => a.checked_sub(*b).map_or_else(
                || Precision::Inexact(a.saturating_sub(*b)),
                Precision::Exact,
            ),
            (Precision::Inexact(a), Precision::Exact(b))
            | (Precision::Exact(a), Precision::Inexact(b))
            | (Precision::Inexact(a), Precision::Inexact(b)) => {
                Precision::Inexact(a.saturating_sub(*b))
            }
            (_, _) => Precision::Absent,
        }
    }

    /// Calculates the multiplication of two (possibly inexact) [`usize`] values,
    /// conservatively propagating exactness information. If one of the input
    /// values is [`Precision::Absent`], the result is `Absent` too.
    pub fn multiply(&self, other: &Precision<usize>) -> Precision<usize> {
        match (self, other) {
            (Precision::Exact(a), Precision::Exact(b)) => a.checked_mul(*b).map_or_else(
                || Precision::Inexact(a.saturating_mul(*b)),
                Precision::Exact,
            ),
            (Precision::Inexact(a), Precision::Exact(b))
            | (Precision::Exact(a), Precision::Inexact(b))
            | (Precision::Inexact(a), Precision::Inexact(b)) => {
                Precision::Inexact(a.saturating_mul(*b))
            }
            (_, _) => Precision::Absent,
        }
    }

    /// Return the estimate of applying a filter with estimated selectivity
    /// `selectivity` to this Precision. A selectivity of `1.0` means that all
    /// rows are selected. A selectivity of `0.5` means half the rows are
    /// selected. Will always return inexact statistics.
    pub fn with_estimated_selectivity(self, selectivity: f64) -> Self {
        self.map(|v| ((v as f64 * selectivity).ceil()) as usize)
            .to_inexact()
    }
}

impl Precision<ScalarValue> {
    fn sum_data_type(data_type: &DataType) -> DataType {
        match data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 => DataType::Int64,
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => DataType::UInt64,
            _ => data_type.clone(),
        }
    }

    fn cast_scalar_to_sum_type(value: &ScalarValue) -> Result<ScalarValue> {
        let source_type = value.data_type();
        let target_type = Self::sum_data_type(&source_type);
        if source_type == target_type {
            Ok(value.clone())
        } else {
            value.cast_to(&target_type)
        }
    }

    /// Calculates the sum of two (possibly inexact) [`ScalarValue`] values,
    /// conservatively propagating exactness information. If one of the input
    /// values is [`Precision::Absent`], the result is `Absent` too.
    ///
    /// Uses [`ScalarValue::add_checked`] so that integer overflow returns
    /// an error (mapped to `Absent`) instead of silently wrapping.
    ///
    /// For performance-sensitive paths prefer `precision_add` which
    /// avoids the Arrow array round-trip.
    pub fn add(&self, other: &Precision<ScalarValue>) -> Precision<ScalarValue> {
        match (self, other) {
            (Precision::Exact(a), Precision::Exact(b)) => a
                .add_checked(b)
                .map(Precision::Exact)
                .unwrap_or(Precision::Absent),
            (Precision::Inexact(a), Precision::Exact(b))
            | (Precision::Exact(a), Precision::Inexact(b))
            | (Precision::Inexact(a), Precision::Inexact(b)) => a
                .add_checked(b)
                .map(Precision::Inexact)
                .unwrap_or(Precision::Absent),
            (_, _) => Precision::Absent,
        }
    }

    /// Casts integer values to the wider SQL `SUM` return type.
    ///
    /// This narrows overflow risk when `sum_value` statistics are merged:
    /// `Int8/Int16/Int32 -> Int64` and `UInt8/UInt16/UInt32 -> UInt64`.
    pub fn cast_to_sum_type(&self) -> Precision<ScalarValue> {
        match (self.is_exact(), self.get_value()) {
            (Some(true), Some(value)) => Self::cast_scalar_to_sum_type(value)
                .map(Precision::Exact)
                .unwrap_or(Precision::Absent),
            (Some(false), Some(value)) => Self::cast_scalar_to_sum_type(value)
                .map(Precision::Inexact)
                .unwrap_or(Precision::Absent),
            (_, _) => Precision::Absent,
        }
    }

    /// SUM-style addition with integer widening to match SQL `SUM` return
    /// types for smaller integral inputs.
    pub fn add_for_sum(&self, other: &Precision<ScalarValue>) -> Precision<ScalarValue> {
        let mut lhs = self.cast_to_sum_type();
        let rhs = other.cast_to_sum_type();
        precision_add(&mut lhs, &rhs);
        lhs
    }

    /// Calculates the difference of two (possibly inexact) [`ScalarValue`] values,
    /// conservatively propagating exactness information. If one of the input
    /// values is [`Precision::Absent`], the result is `Absent` too.
    pub fn sub(&self, other: &Precision<ScalarValue>) -> Precision<ScalarValue> {
        match (self, other) {
            (Precision::Exact(a), Precision::Exact(b)) => {
                a.sub(b).map(Precision::Exact).unwrap_or(Precision::Absent)
            }
            (Precision::Inexact(a), Precision::Exact(b))
            | (Precision::Exact(a), Precision::Inexact(b))
            | (Precision::Inexact(a), Precision::Inexact(b)) => a
                .sub(b)
                .map(Precision::Inexact)
                .unwrap_or(Precision::Absent),
            (_, _) => Precision::Absent,
        }
    }

    /// Calculates the multiplication of two (possibly inexact) [`ScalarValue`] values,
    /// conservatively propagating exactness information. If one of the input
    /// values is [`Precision::Absent`], the result is `Absent` too.
    pub fn multiply(&self, other: &Precision<ScalarValue>) -> Precision<ScalarValue> {
        match (self, other) {
            (Precision::Exact(a), Precision::Exact(b)) => a
                .mul_checked(b)
                .map(Precision::Exact)
                .unwrap_or(Precision::Absent),
            (Precision::Inexact(a), Precision::Exact(b))
            | (Precision::Exact(a), Precision::Inexact(b))
            | (Precision::Inexact(a), Precision::Inexact(b)) => a
                .mul_checked(b)
                .map(Precision::Inexact)
                .unwrap_or(Precision::Absent),
            (_, _) => Precision::Absent,
        }
    }

    /// Casts the value to the given data type, propagating exactness information.
    pub fn cast_to(&self, data_type: &DataType) -> Result<Precision<ScalarValue>> {
        match self {
            Precision::Exact(value) => value.cast_to(data_type).map(Precision::Exact),
            Precision::Inexact(value) => value.cast_to(data_type).map(Precision::Inexact),
            Precision::Absent => Ok(Precision::Absent),
        }
    }
}

impl<T: Debug + Clone + PartialEq + Eq + PartialOrd> Debug for Precision<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Precision::Exact(inner) => write!(f, "Exact({inner:?})"),
            Precision::Inexact(inner) => write!(f, "Inexact({inner:?})"),
            Precision::Absent => write!(f, "Absent"),
        }
    }
}

impl<T: Debug + Clone + PartialEq + Eq + PartialOrd> Display for Precision<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Precision::Exact(inner) => write!(f, "Exact({inner:?})"),
            Precision::Inexact(inner) => write!(f, "Inexact({inner:?})"),
            Precision::Absent => write!(f, "Absent"),
        }
    }
}

impl From<Precision<usize>> for Precision<ScalarValue> {
    fn from(value: Precision<usize>) -> Self {
        match value {
            Precision::Exact(v) => Precision::Exact(ScalarValue::UInt64(Some(v as u64))),
            Precision::Inexact(v) => {
                Precision::Inexact(ScalarValue::UInt64(Some(v as u64)))
            }
            Precision::Absent => Precision::Absent,
        }
    }
}

/// Statistics for a relation
/// Fields are optional and can be inexact because the sources
/// sometimes provide approximate estimates for performance reasons
/// and the transformations output are not always predictable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statistics {
    /// The number of rows estimated to be scanned.
    pub num_rows: Precision<usize>,
    /// The total bytes of the output data.
    ///
    /// Note that this is not the same as the total bytes that may be scanned,
    /// processed, etc.
    /// E.g. we may read 1GB of data from a Parquet file but the Arrow data
    /// the node produces may be 2GB; it's this 2GB that is tracked here.
    pub total_byte_size: Precision<usize>,
    /// Statistics on a column level.
    ///
    /// It must contains a [`ColumnStatistics`] for each field in the schema of
    /// the table to which the [`Statistics`] refer.
    pub column_statistics: Vec<ColumnStatistics>,
}

impl Default for Statistics {
    /// Returns a new [`Statistics`] instance with all fields set to unknown
    /// and no columns.
    fn default() -> Self {
        Self {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        }
    }
}

impl Statistics {
    /// Returns a [`Statistics`] instance for the given schema by assigning
    /// unknown statistics to each column in the schema.
    pub fn new_unknown(schema: &Schema) -> Self {
        Self {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: Statistics::unknown_column(schema),
        }
    }

    /// Calculates `total_byte_size` based on the schema and `num_rows`.
    /// If any of the columns has non-primitive width, `total_byte_size` is set to inexact.
    pub fn calculate_total_byte_size(&mut self, schema: &Schema) {
        let mut row_size = Some(0);
        for field in schema.fields() {
            match field.data_type().primitive_width() {
                Some(width) => {
                    row_size = row_size.map(|s| s + width);
                }
                None => {
                    row_size = None;
                    break;
                }
            }
        }
        match row_size {
            None => {
                self.total_byte_size = self.total_byte_size.to_inexact();
            }
            Some(size) => {
                self.total_byte_size = self.num_rows.multiply(&Precision::Exact(size));
            }
        }
    }

    /// Returns an unbounded `ColumnStatistics` for each field in the schema.
    pub fn unknown_column(schema: &Schema) -> Vec<ColumnStatistics> {
        schema
            .fields()
            .iter()
            .map(|_| ColumnStatistics::new_unknown())
            .collect()
    }

    /// Set the number of rows
    pub fn with_num_rows(mut self, num_rows: Precision<usize>) -> Self {
        self.num_rows = num_rows;
        self
    }

    /// Set the total size, in bytes
    pub fn with_total_byte_size(mut self, total_byte_size: Precision<usize>) -> Self {
        self.total_byte_size = total_byte_size;
        self
    }

    /// Add a column to the column statistics
    pub fn add_column_statistics(mut self, column_stats: ColumnStatistics) -> Self {
        self.column_statistics.push(column_stats);
        self
    }

    /// If the exactness of a [`Statistics`] instance is lost, this function relaxes
    /// the exactness of all information by converting them [`Precision::Inexact`].
    pub fn to_inexact(mut self) -> Self {
        self.num_rows = self.num_rows.to_inexact();
        self.total_byte_size = self.total_byte_size.to_inexact();
        self.column_statistics = self
            .column_statistics
            .into_iter()
            .map(|s| s.to_inexact())
            .collect();
        self
    }

    /// Project the statistics to the given column indices.
    ///
    /// For example, if we had statistics for columns `{"a", "b", "c"}`,
    /// projecting to `vec![2, 1]` would return statistics for columns `{"c",
    /// "b"}`.
    pub fn project(self, projection: Option<&impl AsRef<[usize]>>) -> Self {
        let projection = projection.map(AsRef::as_ref);
        self.project_impl(projection)
    }

    fn project_impl(mut self, projection: Option<&[usize]>) -> Self {
        let Some(projection) = projection.map(AsRef::as_ref) else {
            return self;
        };

        #[expect(clippy::large_enum_variant)]
        enum Slot {
            /// The column is taken and put into the specified statistics location
            Taken(usize),
            /// The original columns is present
            Present(ColumnStatistics),
        }

        // Convert to Vec<Slot> so we can avoid copying the statistics
        let mut columns: Vec<_> = std::mem::take(&mut self.column_statistics)
            .into_iter()
            .map(Slot::Present)
            .collect();

        for idx in projection.iter() {
            let next_idx = self.column_statistics.len();
            let slot = std::mem::replace(
                columns.get_mut(*idx).expect("projection out of bounds"),
                Slot::Taken(next_idx),
            );
            match slot {
                // The column was there, so just move it
                Slot::Present(col) => self.column_statistics.push(col),
                // The column was taken, so copy from the previous location
                Slot::Taken(prev_idx) => self
                    .column_statistics
                    .push(self.column_statistics[prev_idx].clone()),
            }
        }

        self
    }

    /// Calculates the statistics after applying `fetch` and `skip` operations.
    ///
    /// Here, `self` denotes per-partition statistics. Use the `n_partitions`
    /// parameter to compute global statistics in a multi-partition setting.
    pub fn with_fetch(
        mut self,
        fetch: Option<usize>,
        skip: usize,
        n_partitions: usize,
    ) -> Result<Self> {
        let fetch_val = fetch.unwrap_or(usize::MAX);

        // Get the ratio of rows after / rows before on a per-partition basis
        let num_rows_before = self.num_rows;

        self.num_rows = match self {
            Statistics {
                num_rows: Precision::Exact(nr),
                ..
            }
            | Statistics {
                num_rows: Precision::Inexact(nr),
                ..
            } => {
                // Here, the inexact case gives us an upper bound on the number of rows.
                if nr <= skip {
                    // All input data will be skipped:
                    Precision::Exact(0)
                } else if nr <= fetch_val && skip == 0 {
                    // If the input does not reach the `fetch` globally, and `skip`
                    // is zero (meaning the input and output are identical), return
                    // input stats as is.
                    // TODO: Can input stats still be used, but adjusted, when `skip`
                    //       is non-zero?
                    return Ok(self);
                } else if nr - skip <= fetch_val {
                    // After `skip` input rows are skipped, the remaining rows are
                    // less than or equal to the `fetch` values, so `num_rows` must
                    // equal the remaining rows.
                    check_num_rows(
                        (nr - skip).checked_mul(n_partitions),
                        // We know that we have an estimate for the number of rows:
                        self.num_rows.is_exact().unwrap(),
                    )
                } else {
                    // At this point we know that we were given a `fetch` value
                    // as the `None` case would go into the branch above. Since
                    // the input has more rows than `fetch + skip`, the number
                    // of rows will be the `fetch`, other statistics will have to be downgraded to inexact.
                    check_num_rows(
                        fetch_val.checked_mul(n_partitions),
                        // We know that we have an estimate for the number of rows:
                        self.num_rows.is_exact().unwrap(),
                    )
                }
            }
            Statistics {
                num_rows: Precision::Absent,
                ..
            } => check_num_rows(fetch.and_then(|v| v.checked_mul(n_partitions)), false),
        };
        let ratio: f64 = match (num_rows_before, self.num_rows) {
            (
                Precision::Exact(nr_before) | Precision::Inexact(nr_before),
                Precision::Exact(nr_after) | Precision::Inexact(nr_after),
            ) => {
                if nr_before == 0 {
                    0.0
                } else {
                    nr_after as f64 / nr_before as f64
                }
            }
            _ => 0.0,
        };
        self.column_statistics = self
            .column_statistics
            .into_iter()
            .map(|cs| {
                let mut cs = cs.to_inexact();
                // Scale byte_size by the row ratio
                cs.byte_size = match cs.byte_size {
                    Precision::Exact(n) | Precision::Inexact(n) => {
                        Precision::Inexact((n as f64 * ratio) as usize)
                    }
                    Precision::Absent => Precision::Absent,
                };
                cs
            })
            .collect();

        // Compute total_byte_size as sum of column byte_size values if all are present,
        // otherwise fall back to scaling the original total_byte_size
        let sum_scan_bytes: Option<usize> = self
            .column_statistics
            .iter()
            .map(|cs| cs.byte_size.get_value().copied())
            .try_fold(0usize, |acc, val| val.map(|v| acc + v));

        self.total_byte_size = match sum_scan_bytes {
            Some(sum) => Precision::Inexact(sum),
            None => {
                // Fall back to scaling original total_byte_size if not all columns have byte_size
                match &self.total_byte_size {
                    Precision::Exact(n) | Precision::Inexact(n) => {
                        Precision::Inexact((*n as f64 * ratio) as usize)
                    }
                    Precision::Absent => Precision::Absent,
                }
            }
        };
        Ok(self)
    }

    /// Summarize zero or more statistics into a single `Statistics` instance.
    ///
    /// The method assumes that all statistics are for the same schema.
    /// If not, maybe you can call `SchemaMapper::map_column_statistics` to make them consistent.
    ///
    /// Returns an error if the statistics do not match the specified schemas.
    ///
    /// # Example
    /// ```
    /// # use datafusion_common::{ColumnStatistics, ScalarValue, Statistics};
    /// # use arrow::datatypes::{Field, Schema, DataType};
    /// # use datafusion_common::stats::Precision;
    /// let stats1 = Statistics::default()
    ///     .with_num_rows(Precision::Exact(10))
    ///     .add_column_statistics(
    ///         ColumnStatistics::new_unknown()
    ///             .with_min_value(Precision::Exact(ScalarValue::from(1)))
    ///             .with_max_value(Precision::Exact(ScalarValue::from(100)))
    ///             .with_sum_value(Precision::Exact(ScalarValue::from(500))),
    ///     );
    ///
    /// let stats2 = Statistics::default()
    ///     .with_num_rows(Precision::Exact(20))
    ///     .add_column_statistics(
    ///         ColumnStatistics::new_unknown()
    ///             .with_min_value(Precision::Exact(ScalarValue::from(5)))
    ///             .with_max_value(Precision::Exact(ScalarValue::from(200)))
    ///             .with_sum_value(Precision::Exact(ScalarValue::from(1000))),
    ///     );
    ///
    /// let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    /// let merged = Statistics::try_merge_iter(
    ///     &[stats1, stats2],
    ///     &schema,
    /// ).unwrap();
    ///
    /// assert_eq!(merged.num_rows, Precision::Exact(30));
    /// assert_eq!(merged.column_statistics[0].min_value,
    ///     Precision::Exact(ScalarValue::from(1)));
    /// assert_eq!(merged.column_statistics[0].max_value,
    ///     Precision::Exact(ScalarValue::from(200)));
    /// assert_eq!(merged.column_statistics[0].sum_value,
    ///     Precision::Exact(ScalarValue::Int64(Some(1500))));
    /// ```
    pub fn try_merge_iter<'a, I>(items: I, schema: &Schema) -> Result<Statistics>
    where
        I: IntoIterator<Item = &'a Statistics>,
    {
        let mut items = items.into_iter();
        let Some(first) = items.next() else {
            return Ok(Statistics::new_unknown(schema));
        };
        let Some(second) = items.next() else {
            return Ok(first.clone());
        };

        let num_cols = first.column_statistics.len();
        let mut num_rows = first.num_rows;
        let mut total_byte_size = first.total_byte_size;
        let mut column_statistics = first.column_statistics.clone();
        for col_stats in &mut column_statistics {
            cast_sum_value_to_sum_type_in_place(&mut col_stats.sum_value);
        }

        // Merge the remaining items in a single pass.
        for (i, stat) in std::iter::once(second).chain(items).enumerate() {
            if stat.column_statistics.len() != num_cols {
                return _plan_err!(
                    "Cannot merge statistics with different number of columns: {} vs {} (item {})",
                    num_cols,
                    stat.column_statistics.len(),
                    i + 1
                );
            }
            num_rows = num_rows.add(&stat.num_rows);
            total_byte_size = total_byte_size.add(&stat.total_byte_size);

            // Uses precision_add for sum (reuses the lhs accumulator for
            // direct numeric addition), while preserving the NDV update
            // ordering required by estimate_ndv_with_overlap.
            for (col_stats, item_cs) in
                column_statistics.iter_mut().zip(&stat.column_statistics)
            {
                col_stats.null_count = col_stats.null_count.add(&item_cs.null_count);

                // NDV must be computed before min/max update (needs pre-merge ranges)
                col_stats.distinct_count = match (
                    col_stats.distinct_count.get_value(),
                    item_cs.distinct_count.get_value(),
                ) {
                    (Some(&l), Some(&r)) => Precision::Inexact(
                        estimate_ndv_with_overlap(col_stats, item_cs, l, r)
                            .unwrap_or_else(|| usize::max(l, r)),
                    ),
                    _ => Precision::Absent,
                };
                precision_min(&mut col_stats.min_value, &item_cs.min_value);
                precision_max(&mut col_stats.max_value, &item_cs.max_value);
                precision_add_for_sum_in_place(
                    &mut col_stats.sum_value,
                    &item_cs.sum_value,
                );
                col_stats.byte_size = col_stats.byte_size.add(&item_cs.byte_size);
            }
        }

        Ok(Statistics {
            num_rows,
            total_byte_size,
            column_statistics,
        })
    }
}

/// Estimates the combined number of distinct values (NDV) when merging two
/// column statistics, using range overlap to avoid double-counting shared values.
///
/// Assumes values are distributed uniformly within each input's
/// `[min, max]` range (the standard assumption when only summary
/// statistics are available). Under uniformity the fraction of an input's
/// distinct values that land in a sub-range equals the fraction of
/// the range that sub-range covers.
///
/// The combined value space is split into three disjoint regions:
///
/// ```text
///   |-- only A --|-- overlap --|-- only B --|
/// ```
///
/// * **Only in A/B** - values outside the other input's range
///   contribute `(1 - overlap_a) * NDV_a` and `(1 - overlap_b) * NDV_b`.
/// * **Overlap** - both inputs may produce values here. We take
///   `max(overlap_a * NDV_a, overlap_b * NDV_b)` rather than the
///   sum because values in the same sub-range are likely shared
///   (the smaller set is assumed to be a subset of the larger).
///
/// The formula ranges between `[max(NDV_a, NDV_b), NDV_a + NDV_b]`,
/// from full overlap to no overlap.
///
/// ```text
/// NDV = max(overlap_a * NDV_a, overlap_b * NDV_b)   [intersection]
///     + (1 - overlap_a) * NDV_a                      [only in A]
///     + (1 - overlap_b) * NDV_b                      [only in B]
/// ```
///
/// Returns `None` when min/max are absent or distance is unsupported
/// (e.g. strings), in which case the caller should fall back to a simpler
/// estimate.
pub fn estimate_ndv_with_overlap(
    left: &ColumnStatistics,
    right: &ColumnStatistics,
    ndv_left: usize,
    ndv_right: usize,
) -> Option<usize> {
    let left_min = left.min_value.get_value()?;
    let left_max = left.max_value.get_value()?;
    let right_min = right.min_value.get_value()?;
    let right_max = right.max_value.get_value()?;

    let range_left = left_max.distance(left_min)?;
    let range_right = right_max.distance(right_min)?;

    // Constant columns (range == 0) can't use the proportional overlap
    // formula below, so check interval overlap directly instead.
    if range_left == 0 || range_right == 0 {
        let overlaps = left_min <= right_max && right_min <= left_max;
        return Some(if overlaps {
            usize::max(ndv_left, ndv_right)
        } else {
            ndv_left + ndv_right
        });
    }

    let overlap_min = if left_min >= right_min {
        left_min
    } else {
        right_min
    };
    let overlap_max = if left_max <= right_max {
        left_max
    } else {
        right_max
    };

    // Disjoint ranges: no overlap, NDVs are additive
    if overlap_min > overlap_max {
        return Some(ndv_left + ndv_right);
    }

    let overlap_range = overlap_max.distance(overlap_min)? as f64;

    let overlap_left = overlap_range / range_left as f64;
    let overlap_right = overlap_range / range_right as f64;

    let intersection = f64::max(
        overlap_left * ndv_left as f64,
        overlap_right * ndv_right as f64,
    );
    let only_left = (1.0 - overlap_left) * ndv_left as f64;
    let only_right = (1.0 - overlap_right) * ndv_right as f64;

    Some((intersection + only_left + only_right).round() as usize)
}

/// Returns the minimum precision while not allocating a new value,
/// mirrors the semantics of `PartialOrd`.
#[inline]
fn precision_min<T>(lhs: &mut Precision<T>, rhs: &Precision<T>)
where
    T: Debug + Clone + PartialEq + Eq + PartialOrd,
{
    *lhs = match (std::mem::take(lhs), rhs) {
        (Precision::Exact(left), Precision::Exact(right)) => {
            if left <= *right {
                Precision::Exact(left)
            } else {
                Precision::Exact(right.clone())
            }
        }
        (Precision::Exact(left), Precision::Inexact(right))
        | (Precision::Inexact(left), Precision::Exact(right))
        | (Precision::Inexact(left), Precision::Inexact(right)) => {
            if left <= *right {
                Precision::Inexact(left)
            } else {
                Precision::Inexact(right.clone())
            }
        }
        (_, _) => Precision::Absent,
    };
}

/// Returns the maximum precision while not allocating a new value,
/// mirrors the semantics of `PartialOrd`.
#[inline]
fn precision_max<T>(lhs: &mut Precision<T>, rhs: &Precision<T>)
where
    T: Debug + Clone + PartialEq + Eq + PartialOrd,
{
    *lhs = match (std::mem::take(lhs), rhs) {
        (Precision::Exact(left), Precision::Exact(right)) => {
            if left >= *right {
                Precision::Exact(left)
            } else {
                Precision::Exact(right.clone())
            }
        }
        (Precision::Exact(left), Precision::Inexact(right))
        | (Precision::Inexact(left), Precision::Exact(right))
        | (Precision::Inexact(left), Precision::Inexact(right)) => {
            if left >= *right {
                Precision::Inexact(left)
            } else {
                Precision::Inexact(right.clone())
            }
        }
        (_, _) => Precision::Absent,
    };
}

#[inline]
fn cast_sum_value_to_sum_type_in_place(value: &mut Precision<ScalarValue>) {
    let (is_exact, inner) = match std::mem::take(value) {
        Precision::Exact(v) => (true, v),
        Precision::Inexact(v) => (false, v),
        Precision::Absent => return,
    };
    let source_type = inner.data_type();
    let target_type = Precision::<ScalarValue>::sum_data_type(&source_type);

    let wrap_precision_fn: fn(ScalarValue) -> Precision<ScalarValue> = if is_exact {
        Precision::Exact
    } else {
        Precision::Inexact
    };

    *value = if source_type == target_type {
        wrap_precision_fn(inner)
    } else {
        inner
            .cast_to(&target_type)
            .map(wrap_precision_fn)
            .unwrap_or(Precision::Absent)
    };
}

#[inline]
fn precision_add_for_sum_in_place(
    lhs: &mut Precision<ScalarValue>,
    rhs: &Precision<ScalarValue>,
) {
    let (value, wrap_fn): (&ScalarValue, fn(ScalarValue) -> Precision<ScalarValue>) =
        match rhs {
            Precision::Exact(v) => (v, Precision::Exact),
            Precision::Inexact(v) => (v, Precision::Inexact),
            Precision::Absent => {
                *lhs = Precision::Absent;
                return;
            }
        };
    let source_type = value.data_type();
    let target_type = Precision::<ScalarValue>::sum_data_type(&source_type);
    if source_type == target_type {
        precision_add(lhs, rhs);
    } else {
        let rhs = value
            .cast_to(&target_type)
            .map(wrap_fn)
            .unwrap_or(Precision::Absent);
        precision_add(lhs, &rhs);
    }
}

/// Creates an estimate of the number of rows in the output using the given
/// optional value and exactness flag.
fn check_num_rows(value: Option<usize>, is_exact: bool) -> Precision<usize> {
    if let Some(value) = value {
        if is_exact {
            Precision::Exact(value)
        } else {
            // If the input stats are inexact, so are the output stats.
            Precision::Inexact(value)
        }
    } else {
        // If the estimate is not available (e.g. due to an overflow), we can
        // not produce a reliable estimate.
        Precision::Absent
    }
}

impl Display for Statistics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // string of column statistics
        let column_stats = self
            .column_statistics
            .iter()
            .enumerate()
            .map(|(i, cs)| {
                let s = format!("(Col[{i}]:");
                let s = if cs.min_value != Precision::Absent {
                    format!("{} Min={}", s, cs.min_value)
                } else {
                    s
                };
                let s = if cs.max_value != Precision::Absent {
                    format!("{} Max={}", s, cs.max_value)
                } else {
                    s
                };
                let s = if cs.sum_value != Precision::Absent {
                    format!("{} Sum={}", s, cs.sum_value)
                } else {
                    s
                };
                let s = if cs.null_count != Precision::Absent {
                    format!("{} Null={}", s, cs.null_count)
                } else {
                    s
                };
                let s = if cs.distinct_count != Precision::Absent {
                    format!("{} Distinct={}", s, cs.distinct_count)
                } else {
                    s
                };
                let s = if cs.byte_size != Precision::Absent {
                    format!("{} ScanBytes={}", s, cs.byte_size)
                } else {
                    s
                };

                s + ")"
            })
            .collect::<Vec<_>>()
            .join(",");

        write!(
            f,
            "Rows={}, Bytes={}, [{}]",
            self.num_rows, self.total_byte_size, column_stats
        )?;

        Ok(())
    }
}

/// Statistics for a column within a relation
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ColumnStatistics {
    /// Number of null values on column
    pub null_count: Precision<usize>,
    /// Maximum value of column
    pub max_value: Precision<ScalarValue>,
    /// Minimum value of column
    pub min_value: Precision<ScalarValue>,
    /// Sum value of a column.
    ///
    /// For integral columns, values should be kept in SUM-compatible widened
    /// types (`Int8/Int16/Int32 -> Int64`, `UInt8/UInt16/UInt32 -> UInt64`) to
    /// reduce overflow risk during statistics propagation.
    ///
    /// Callers should prefer [`ColumnStatistics::with_sum_value`] for setting
    /// this field and [`Precision<ScalarValue>::add_for_sum`] /
    /// [`Precision<ScalarValue>::cast_to_sum_type`] for sum arithmetic.
    pub sum_value: Precision<ScalarValue>,
    /// Number of distinct values
    pub distinct_count: Precision<usize>,
    /// Estimated size of this column's data in bytes for the output.
    ///
    /// Note that this is not the same as the total bytes that may be scanned,
    /// processed, etc.
    ///
    /// E.g. we may read 1GB of data from a Parquet file but the Arrow data
    /// the node produces may be 2GB; it's this 2GB that is tracked here.
    ///
    /// Currently this is accurately calculated for primitive types only.
    /// For complex types (like Utf8, List, Struct, etc), this value may be
    /// absent or inexact (e.g. estimated from the size of the data in the source Parquet files).
    ///
    /// This value is automatically scaled when operations like limits or
    /// filters reduce the number of rows (see [`Statistics::with_fetch`]).
    pub byte_size: Precision<usize>,
}

impl ColumnStatistics {
    /// Column contains a single non null value (e.g constant).
    pub fn is_singleton(&self) -> bool {
        match (&self.min_value, &self.max_value) {
            // Min and max values are the same and not infinity.
            (Precision::Exact(min), Precision::Exact(max)) => {
                !min.is_null() && !max.is_null() && (min == max)
            }
            (_, _) => false,
        }
    }

    /// Returns a [`ColumnStatistics`] instance having all [`Precision::Absent`] parameters.
    pub fn new_unknown() -> Self {
        Self {
            null_count: Precision::Absent,
            max_value: Precision::Absent,
            min_value: Precision::Absent,
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Absent,
        }
    }

    /// Set the null count
    pub fn with_null_count(mut self, null_count: Precision<usize>) -> Self {
        self.null_count = null_count;
        self
    }

    /// Set the max value
    pub fn with_max_value(mut self, max_value: Precision<ScalarValue>) -> Self {
        self.max_value = max_value;
        self
    }

    /// Set the min value
    pub fn with_min_value(mut self, min_value: Precision<ScalarValue>) -> Self {
        self.min_value = min_value;
        self
    }

    /// Set the sum value
    pub fn with_sum_value(mut self, sum_value: Precision<ScalarValue>) -> Self {
        self.sum_value = match sum_value {
            Precision::Exact(value) => {
                Precision::<ScalarValue>::cast_scalar_to_sum_type(&value)
                    .map(Precision::Exact)
                    .unwrap_or(Precision::Absent)
            }
            Precision::Inexact(value) => {
                Precision::<ScalarValue>::cast_scalar_to_sum_type(&value)
                    .map(Precision::Inexact)
                    .unwrap_or(Precision::Absent)
            }
            Precision::Absent => Precision::Absent,
        };
        self
    }

    /// Set the distinct count
    pub fn with_distinct_count(mut self, distinct_count: Precision<usize>) -> Self {
        self.distinct_count = distinct_count;
        self
    }

    /// Set the scan byte size
    /// This should initially be set to the total size of the column.
    pub fn with_byte_size(mut self, byte_size: Precision<usize>) -> Self {
        self.byte_size = byte_size;
        self
    }

    /// If the exactness of a [`ColumnStatistics`] instance is lost, this
    /// function relaxes the exactness of all information by converting them
    /// [`Precision::Inexact`].
    pub fn to_inexact(mut self) -> Self {
        self.null_count = self.null_count.to_inexact();
        self.max_value = self.max_value.to_inexact();
        self.min_value = self.min_value.to_inexact();
        self.sum_value = self.sum_value.to_inexact();
        self.distinct_count = self.distinct_count.to_inexact();
        self.byte_size = self.byte_size.to_inexact();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_contains;
    use arrow::datatypes::Field;
    use std::sync::Arc;

    #[test]
    fn test_get_value() {
        let exact_precision = Precision::Exact(42);
        let inexact_precision = Precision::Inexact(23);
        let absent_precision = Precision::<i32>::Absent;

        assert_eq!(*exact_precision.get_value().unwrap(), 42);
        assert_eq!(*inexact_precision.get_value().unwrap(), 23);
        assert_eq!(absent_precision.get_value(), None);
    }

    #[test]
    fn test_map() {
        let exact_precision = Precision::Exact(42);
        let inexact_precision = Precision::Inexact(23);
        let absent_precision = Precision::Absent;

        let squared = |x| x * x;

        assert_eq!(exact_precision.map(squared), Precision::Exact(1764));
        assert_eq!(inexact_precision.map(squared), Precision::Inexact(529));
        assert_eq!(absent_precision.map(squared), Precision::Absent);
    }

    #[test]
    fn test_is_exact() {
        let exact_precision = Precision::Exact(42);
        let inexact_precision = Precision::Inexact(23);
        let absent_precision = Precision::<i32>::Absent;

        assert_eq!(exact_precision.is_exact(), Some(true));
        assert_eq!(inexact_precision.is_exact(), Some(false));
        assert_eq!(absent_precision.is_exact(), None);
    }

    #[test]
    fn test_max() {
        let precision1 = Precision::Exact(42);
        let precision2 = Precision::Inexact(23);
        let precision3 = Precision::Exact(30);
        let absent_precision = Precision::Absent;

        assert_eq!(precision1.max(&precision2), Precision::Inexact(42));
        assert_eq!(precision1.max(&precision3), Precision::Exact(42));
        assert_eq!(precision2.max(&precision3), Precision::Inexact(30));
        assert_eq!(precision1.max(&absent_precision), Precision::Absent);
    }

    #[test]
    fn test_min() {
        let precision1 = Precision::Exact(42);
        let precision2 = Precision::Inexact(23);
        let precision3 = Precision::Exact(30);
        let absent_precision = Precision::Absent;

        assert_eq!(precision1.min(&precision2), Precision::Inexact(23));
        assert_eq!(precision1.min(&precision3), Precision::Exact(30));
        assert_eq!(precision2.min(&precision3), Precision::Inexact(23));
        assert_eq!(precision1.min(&absent_precision), Precision::Absent);
    }

    #[test]
    fn test_to_inexact() {
        let exact_precision = Precision::Exact(42);
        let inexact_precision = Precision::Inexact(42);
        let absent_precision = Precision::<i32>::Absent;

        assert_eq!(exact_precision.to_inexact(), inexact_precision);
        assert_eq!(inexact_precision.to_inexact(), inexact_precision);
        assert_eq!(absent_precision.to_inexact(), absent_precision);
    }

    #[test]
    fn test_add() {
        let precision1 = Precision::Exact(42);
        let precision2 = Precision::Inexact(23);
        let precision3 = Precision::Exact(30);
        let absent_precision = Precision::Absent;
        let precision_max_exact = Precision::Exact(usize::MAX);
        let precision_max_inexact = Precision::Exact(usize::MAX);

        assert_eq!(precision1.add(&precision2), Precision::Inexact(65));
        assert_eq!(precision1.add(&precision3), Precision::Exact(72));
        assert_eq!(precision2.add(&precision3), Precision::Inexact(53));
        assert_eq!(precision1.add(&absent_precision), Precision::Absent);
        assert_eq!(
            precision_max_exact.add(&precision1),
            Precision::Inexact(usize::MAX)
        );
        assert_eq!(
            precision_max_inexact.add(&precision1),
            Precision::Inexact(usize::MAX)
        );
    }

    #[test]
    fn test_add_scalar() {
        let precision = Precision::Exact(ScalarValue::Int32(Some(42)));

        assert_eq!(
            precision.add(&Precision::Exact(ScalarValue::Int32(Some(23)))),
            Precision::Exact(ScalarValue::Int32(Some(65))),
        );
        assert_eq!(
            precision.add(&Precision::Inexact(ScalarValue::Int32(Some(23)))),
            Precision::Inexact(ScalarValue::Int32(Some(65))),
        );
        assert_eq!(
            precision.add(&Precision::Exact(ScalarValue::Int32(None))),
            // As per behavior of ScalarValue::add
            Precision::Exact(ScalarValue::Int32(None)),
        );
        assert_eq!(precision.add(&Precision::Absent), Precision::Absent);
    }

    #[test]
    fn test_add_for_sum_scalar_integer_widening() {
        let precision = Precision::Exact(ScalarValue::Int32(Some(42)));

        assert_eq!(
            precision.add_for_sum(&Precision::Exact(ScalarValue::Int32(Some(23)))),
            Precision::Exact(ScalarValue::Int64(Some(65))),
        );
        assert_eq!(
            precision.add_for_sum(&Precision::Inexact(ScalarValue::Int32(Some(23)))),
            Precision::Inexact(ScalarValue::Int64(Some(65))),
        );
    }

    #[test]
    fn test_add_for_sum_prevents_int32_overflow() {
        let lhs = Precision::Exact(ScalarValue::Int32(Some(i32::MAX)));
        let rhs = Precision::Exact(ScalarValue::Int32(Some(1)));

        assert_eq!(
            lhs.add_for_sum(&rhs),
            Precision::Exact(ScalarValue::Int64(Some(i64::from(i32::MAX) + 1))),
        );
    }

    #[test]
    fn test_add_for_sum_scalar_unsigned_integer_widening() {
        let precision = Precision::Exact(ScalarValue::UInt32(Some(42)));

        assert_eq!(
            precision.add_for_sum(&Precision::Exact(ScalarValue::UInt32(Some(23)))),
            Precision::Exact(ScalarValue::UInt64(Some(65))),
        );
        assert_eq!(
            precision.add_for_sum(&Precision::Inexact(ScalarValue::UInt32(Some(23)))),
            Precision::Inexact(ScalarValue::UInt64(Some(65))),
        );
    }

    #[test]
    fn test_sub() {
        let precision1 = Precision::Exact(42);
        let precision2 = Precision::Inexact(23);
        let precision3 = Precision::Exact(30);
        let absent_precision = Precision::Absent;

        assert_eq!(precision1.sub(&precision2), Precision::Inexact(19));
        assert_eq!(precision1.sub(&precision3), Precision::Exact(12));
        assert_eq!(precision2.sub(&precision1), Precision::Inexact(0));
        assert_eq!(precision3.sub(&precision1), Precision::Inexact(0));
        assert_eq!(precision1.sub(&absent_precision), Precision::Absent);
    }

    #[test]
    fn test_sub_scalar() {
        let precision = Precision::Exact(ScalarValue::Int32(Some(42)));

        assert_eq!(
            precision.sub(&Precision::Exact(ScalarValue::Int32(Some(23)))),
            Precision::Exact(ScalarValue::Int32(Some(19))),
        );
        assert_eq!(
            precision.sub(&Precision::Inexact(ScalarValue::Int32(Some(23)))),
            Precision::Inexact(ScalarValue::Int32(Some(19))),
        );
        assert_eq!(
            precision.sub(&Precision::Exact(ScalarValue::Int32(None))),
            // As per behavior of ScalarValue::sub
            Precision::Exact(ScalarValue::Int32(None)),
        );
        assert_eq!(precision.sub(&Precision::Absent), Precision::Absent);
    }

    #[test]
    fn test_multiply() {
        let precision1 = Precision::Exact(6);
        let precision2 = Precision::Inexact(3);
        let precision3 = Precision::Exact(5);
        let precision_max_exact = Precision::Exact(usize::MAX);
        let precision_max_inexact = Precision::Exact(usize::MAX);
        let absent_precision = Precision::Absent;

        assert_eq!(precision1.multiply(&precision2), Precision::Inexact(18));
        assert_eq!(precision1.multiply(&precision3), Precision::Exact(30));
        assert_eq!(precision2.multiply(&precision3), Precision::Inexact(15));
        assert_eq!(precision1.multiply(&absent_precision), Precision::Absent);
        assert_eq!(
            precision_max_exact.multiply(&precision1),
            Precision::Inexact(usize::MAX)
        );
        assert_eq!(
            precision_max_inexact.multiply(&precision1),
            Precision::Inexact(usize::MAX)
        );
    }

    #[test]
    fn test_multiply_scalar() {
        let precision = Precision::Exact(ScalarValue::Int32(Some(6)));

        assert_eq!(
            precision.multiply(&Precision::Exact(ScalarValue::Int32(Some(5)))),
            Precision::Exact(ScalarValue::Int32(Some(30))),
        );
        assert_eq!(
            precision.multiply(&Precision::Inexact(ScalarValue::Int32(Some(5)))),
            Precision::Inexact(ScalarValue::Int32(Some(30))),
        );
        assert_eq!(
            precision.multiply(&Precision::Exact(ScalarValue::Int32(None))),
            // As per behavior of ScalarValue::mul_checked
            Precision::Exact(ScalarValue::Int32(None)),
        );
        assert_eq!(precision.multiply(&Precision::Absent), Precision::Absent);
    }

    #[test]
    fn test_cast_to() {
        // Valid
        assert_eq!(
            Precision::Exact(ScalarValue::Int32(Some(42)))
                .cast_to(&DataType::Int64)
                .unwrap(),
            Precision::Exact(ScalarValue::Int64(Some(42))),
        );
        assert_eq!(
            Precision::Inexact(ScalarValue::Int32(Some(42)))
                .cast_to(&DataType::Int64)
                .unwrap(),
            Precision::Inexact(ScalarValue::Int64(Some(42))),
        );
        // Null
        assert_eq!(
            Precision::Exact(ScalarValue::Int32(None))
                .cast_to(&DataType::Int64)
                .unwrap(),
            Precision::Exact(ScalarValue::Int64(None)),
        );
        // Overflow returns error
        assert!(
            Precision::Exact(ScalarValue::Int32(Some(256)))
                .cast_to(&DataType::Int8)
                .is_err()
        );
    }

    #[test]
    fn test_precision_cloning() {
        // Precision<usize> is copy
        let precision: Precision<usize> = Precision::Exact(42);
        let p2 = precision;
        assert_eq!(precision, p2);

        // Precision<ScalarValue> is not copy (requires .clone())
        let precision: Precision<ScalarValue> =
            Precision::Exact(ScalarValue::Int64(Some(42)));
        let p2 = precision.clone();
        assert_eq!(precision, p2);
    }

    #[test]
    fn test_project_none() {
        let projection: Option<Vec<usize>> = None;
        let stats = make_stats(vec![10, 20, 30]).project(projection.as_ref());
        assert_eq!(stats, make_stats(vec![10, 20, 30]));
    }

    #[test]
    fn test_project_empty() {
        let projection = Some(vec![]);
        let stats = make_stats(vec![10, 20, 30]).project(projection.as_ref());
        assert_eq!(stats, make_stats(vec![]));
    }

    #[test]
    fn test_project_swap() {
        let projection = Some(vec![2, 1]);
        let stats = make_stats(vec![10, 20, 30]).project(projection.as_ref());
        assert_eq!(stats, make_stats(vec![30, 20]));
    }

    #[test]
    fn test_project_repeated() {
        let projection = Some(vec![1, 2, 1, 1, 0, 2]);
        let stats = make_stats(vec![10, 20, 30]).project(projection.as_ref());
        assert_eq!(stats, make_stats(vec![20, 30, 20, 20, 10, 30]));
    }

    // Make a Statistics structure with the specified null counts for each column
    fn make_stats(counts: impl IntoIterator<Item = usize>) -> Statistics {
        Statistics {
            num_rows: Precision::Exact(42),
            total_byte_size: Precision::Exact(500),
            column_statistics: counts.into_iter().map(col_stats_i64).collect(),
        }
    }

    fn col_stats_i64(null_count: usize) -> ColumnStatistics {
        ColumnStatistics {
            null_count: Precision::Exact(null_count),
            max_value: Precision::Exact(ScalarValue::Int64(Some(42))),
            min_value: Precision::Exact(ScalarValue::Int64(Some(64))),
            sum_value: Precision::Exact(ScalarValue::Int64(Some(4600))),
            distinct_count: Precision::Exact(100),
            byte_size: Precision::Exact(800),
        }
    }

    #[test]
    fn test_try_merge() {
        // Create a schema with two columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int32, false),
            Field::new("col2", DataType::Int32, false),
        ]));

        // Create items with statistics
        let stats1 = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Exact(100),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(100))),
                    min_value: Precision::Exact(ScalarValue::Int32(Some(1))),
                    sum_value: Precision::Exact(ScalarValue::Int32(Some(500))),
                    distinct_count: Precision::Absent,
                    byte_size: Precision::Exact(40),
                },
                ColumnStatistics {
                    null_count: Precision::Exact(2),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(200))),
                    min_value: Precision::Exact(ScalarValue::Int32(Some(10))),
                    sum_value: Precision::Exact(ScalarValue::Int32(Some(1000))),
                    distinct_count: Precision::Absent,
                    byte_size: Precision::Exact(40),
                },
            ],
        };

        let stats2 = Statistics {
            num_rows: Precision::Exact(15),
            total_byte_size: Precision::Exact(150),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(2),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(120))),
                    min_value: Precision::Exact(ScalarValue::Int32(Some(-10))),
                    sum_value: Precision::Exact(ScalarValue::Int32(Some(600))),
                    distinct_count: Precision::Absent,
                    byte_size: Precision::Exact(60),
                },
                ColumnStatistics {
                    null_count: Precision::Exact(3),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(180))),
                    min_value: Precision::Exact(ScalarValue::Int32(Some(5))),
                    sum_value: Precision::Exact(ScalarValue::Int32(Some(1200))),
                    distinct_count: Precision::Absent,
                    byte_size: Precision::Exact(60),
                },
            ],
        };

        let items = vec![stats1, stats2];

        let summary_stats = Statistics::try_merge_iter(&items, &schema).unwrap();

        // Verify the results
        assert_eq!(summary_stats.num_rows, Precision::Exact(25)); // 10 + 15
        assert_eq!(summary_stats.total_byte_size, Precision::Exact(250)); // 100 + 150

        // Verify column statistics
        let col1_stats = &summary_stats.column_statistics[0];
        assert_eq!(col1_stats.null_count, Precision::Exact(3)); // 1 + 2
        assert_eq!(
            col1_stats.max_value,
            Precision::Exact(ScalarValue::Int32(Some(120)))
        );
        assert_eq!(
            col1_stats.min_value,
            Precision::Exact(ScalarValue::Int32(Some(-10)))
        );
        assert_eq!(
            col1_stats.sum_value,
            Precision::Exact(ScalarValue::Int64(Some(1100)))
        ); // 500 + 600

        let col2_stats = &summary_stats.column_statistics[1];
        assert_eq!(col2_stats.null_count, Precision::Exact(5)); // 2 + 3
        assert_eq!(
            col2_stats.max_value,
            Precision::Exact(ScalarValue::Int32(Some(200)))
        );
        assert_eq!(
            col2_stats.min_value,
            Precision::Exact(ScalarValue::Int32(Some(5)))
        );
        assert_eq!(
            col2_stats.sum_value,
            Precision::Exact(ScalarValue::Int64(Some(2200)))
        ); // 1000 + 1200
    }

    #[test]
    fn test_try_merge_mixed_precision() {
        // Create a schema with one column
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        // Create items with different precision levels
        let stats1 = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Inexact(100),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(1),
                max_value: Precision::Exact(ScalarValue::Int32(Some(100))),
                min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                sum_value: Precision::Exact(ScalarValue::Int32(Some(500))),
                distinct_count: Precision::Absent,
                byte_size: Precision::Exact(40),
            }],
        };

        let stats2 = Statistics {
            num_rows: Precision::Inexact(15),
            total_byte_size: Precision::Exact(150),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Inexact(2),
                max_value: Precision::Inexact(ScalarValue::Int32(Some(120))),
                min_value: Precision::Exact(ScalarValue::Int32(Some(-10))),
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
                byte_size: Precision::Inexact(60),
            }],
        };

        let items = vec![stats1, stats2];

        let summary_stats = Statistics::try_merge_iter(&items, &schema).unwrap();

        assert_eq!(summary_stats.num_rows, Precision::Inexact(25));
        assert_eq!(summary_stats.total_byte_size, Precision::Inexact(250));

        let col_stats = &summary_stats.column_statistics[0];
        assert_eq!(col_stats.null_count, Precision::Inexact(3));
        assert_eq!(
            col_stats.max_value,
            Precision::Inexact(ScalarValue::Int32(Some(120)))
        );
        assert_eq!(
            col_stats.min_value,
            Precision::Inexact(ScalarValue::Int32(Some(-10)))
        );
        assert_eq!(col_stats.sum_value, Precision::Absent);
    }

    #[test]
    fn test_try_merge_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        // Empty collection
        let items: Vec<Statistics> = vec![];

        let summary_stats = Statistics::try_merge_iter(&items, &schema).unwrap();

        // Verify default values for empty collection
        assert_eq!(summary_stats.num_rows, Precision::Absent);
        assert_eq!(summary_stats.total_byte_size, Precision::Absent);
        assert_eq!(summary_stats.column_statistics.len(), 1);
        assert_eq!(
            summary_stats.column_statistics[0].null_count,
            Precision::Absent
        );
    }

    #[test]
    fn test_try_merge_mismatched_size() {
        // Create a schema with one column
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        // No column statistics
        let stats1 = Statistics::default();

        let stats2 =
            Statistics::default().add_column_statistics(ColumnStatistics::new_unknown());

        let items = vec![stats1, stats2];

        let e = Statistics::try_merge_iter(&items, &schema).unwrap_err();
        assert_contains!(
            e.to_string(),
            "Error during planning: Cannot merge statistics with different number of columns: 0 vs 1"
        );
    }

    #[test]
    fn test_try_merge_distinct_count_absent() {
        // Create statistics with known distinct counts
        let stats1 = Statistics::default()
            .with_num_rows(Precision::Exact(10))
            .with_total_byte_size(Precision::Exact(100))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_null_count(Precision::Exact(0))
                    .with_min_value(Precision::Exact(ScalarValue::Int32(Some(1))))
                    .with_max_value(Precision::Exact(ScalarValue::Int32(Some(10))))
                    .with_distinct_count(Precision::Exact(5)),
            );

        let stats2 = Statistics::default()
            .with_num_rows(Precision::Exact(15))
            .with_total_byte_size(Precision::Exact(150))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_null_count(Precision::Exact(0))
                    .with_min_value(Precision::Exact(ScalarValue::Int32(Some(5))))
                    .with_max_value(Precision::Exact(ScalarValue::Int32(Some(20))))
                    .with_distinct_count(Precision::Exact(7)),
            );

        // Merge statistics
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let merged_stats =
            Statistics::try_merge_iter([&stats1, &stats2], &schema).unwrap();

        // Verify the results
        assert_eq!(merged_stats.num_rows, Precision::Exact(25));
        assert_eq!(merged_stats.total_byte_size, Precision::Exact(250));

        let col_stats = &merged_stats.column_statistics[0];
        assert_eq!(col_stats.null_count, Precision::Exact(0));
        assert_eq!(
            col_stats.min_value,
            Precision::Exact(ScalarValue::Int32(Some(1)))
        );
        assert_eq!(
            col_stats.max_value,
            Precision::Exact(ScalarValue::Int32(Some(20)))
        );
        // Overlap-based NDV: ranges [1,10] and [5,20], overlap [5,10]
        // range_left=9, range_right=15, overlap=5
        // overlap_left=5*(5/9)=2.78, overlap_right=7*(5/15)=2.33
        // result = max(2.78, 2.33) + (5-2.78) + (7-2.33) = 9.67 -> 10
        assert_eq!(col_stats.distinct_count, Precision::Inexact(10));
    }

    #[test]
    fn test_try_merge_ndv_disjoint_ranges() {
        let stats1 = Statistics::default()
            .with_num_rows(Precision::Exact(10))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_min_value(Precision::Exact(ScalarValue::Int32(Some(0))))
                    .with_max_value(Precision::Exact(ScalarValue::Int32(Some(10))))
                    .with_distinct_count(Precision::Exact(5)),
            );
        let stats2 = Statistics::default()
            .with_num_rows(Precision::Exact(10))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_min_value(Precision::Exact(ScalarValue::Int32(Some(20))))
                    .with_max_value(Precision::Exact(ScalarValue::Int32(Some(30))))
                    .with_distinct_count(Precision::Exact(8)),
            );

        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let merged = Statistics::try_merge_iter([&stats1, &stats2], &schema).unwrap();
        // No overlap -> sum of NDVs
        assert_eq!(
            merged.column_statistics[0].distinct_count,
            Precision::Inexact(13)
        );
    }

    #[test]
    fn test_try_merge_ndv_identical_ranges() {
        let stats1 = Statistics::default()
            .with_num_rows(Precision::Exact(100))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_min_value(Precision::Exact(ScalarValue::Int32(Some(0))))
                    .with_max_value(Precision::Exact(ScalarValue::Int32(Some(100))))
                    .with_distinct_count(Precision::Exact(50)),
            );
        let stats2 = Statistics::default()
            .with_num_rows(Precision::Exact(100))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_min_value(Precision::Exact(ScalarValue::Int32(Some(0))))
                    .with_max_value(Precision::Exact(ScalarValue::Int32(Some(100))))
                    .with_distinct_count(Precision::Exact(30)),
            );

        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let merged = Statistics::try_merge_iter([&stats1, &stats2], &schema).unwrap();
        // Full overlap -> max(50, 30) = 50
        assert_eq!(
            merged.column_statistics[0].distinct_count,
            Precision::Inexact(50)
        );
    }

    #[test]
    fn test_try_merge_ndv_partial_overlap() {
        let stats1 = Statistics::default()
            .with_num_rows(Precision::Exact(100))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_min_value(Precision::Exact(ScalarValue::Int32(Some(0))))
                    .with_max_value(Precision::Exact(ScalarValue::Int32(Some(100))))
                    .with_distinct_count(Precision::Exact(80)),
            );
        let stats2 = Statistics::default()
            .with_num_rows(Precision::Exact(100))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_min_value(Precision::Exact(ScalarValue::Int32(Some(50))))
                    .with_max_value(Precision::Exact(ScalarValue::Int32(Some(150))))
                    .with_distinct_count(Precision::Exact(60)),
            );

        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let merged = Statistics::try_merge_iter([&stats1, &stats2], &schema).unwrap();
        // overlap=[50,100], range_left=100, range_right=100, overlap_range=50
        // overlap_left=80*(50/100)=40, overlap_right=60*(50/100)=30
        // result = max(40,30) + (80-40) + (60-30) = 40 + 40 + 30 = 110
        assert_eq!(
            merged.column_statistics[0].distinct_count,
            Precision::Inexact(110)
        );
    }

    #[test]
    fn test_try_merge_ndv_missing_min_max() {
        let stats1 = Statistics::default()
            .with_num_rows(Precision::Exact(10))
            .add_column_statistics(
                ColumnStatistics::new_unknown().with_distinct_count(Precision::Exact(5)),
            );
        let stats2 = Statistics::default()
            .with_num_rows(Precision::Exact(10))
            .add_column_statistics(
                ColumnStatistics::new_unknown().with_distinct_count(Precision::Exact(8)),
            );

        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let merged = Statistics::try_merge_iter([&stats1, &stats2], &schema).unwrap();
        // No min/max -> fallback to max(5, 8)
        assert_eq!(
            merged.column_statistics[0].distinct_count,
            Precision::Inexact(8)
        );
    }

    #[test]
    fn test_try_merge_ndv_non_numeric_types() {
        let stats1 = Statistics::default()
            .with_num_rows(Precision::Exact(10))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_min_value(Precision::Exact(ScalarValue::Utf8(Some(
                        "aaa".to_string(),
                    ))))
                    .with_max_value(Precision::Exact(ScalarValue::Utf8(Some(
                        "zzz".to_string(),
                    ))))
                    .with_distinct_count(Precision::Exact(5)),
            );
        let stats2 = Statistics::default()
            .with_num_rows(Precision::Exact(10))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_min_value(Precision::Exact(ScalarValue::Utf8(Some(
                        "bbb".to_string(),
                    ))))
                    .with_max_value(Precision::Exact(ScalarValue::Utf8(Some(
                        "yyy".to_string(),
                    ))))
                    .with_distinct_count(Precision::Exact(8)),
            );

        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let merged = Statistics::try_merge_iter([&stats1, &stats2], &schema).unwrap();
        // distance() unsupported for strings -> fallback to max
        assert_eq!(
            merged.column_statistics[0].distinct_count,
            Precision::Inexact(8)
        );
    }

    #[test]
    fn test_try_merge_ndv_constant_columns() {
        // Same constant: [5,5]+[5,5] -> max
        let stats1 = Statistics::default()
            .with_num_rows(Precision::Exact(10))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_min_value(Precision::Exact(ScalarValue::Int32(Some(5))))
                    .with_max_value(Precision::Exact(ScalarValue::Int32(Some(5))))
                    .with_distinct_count(Precision::Exact(1)),
            );
        let stats2 = Statistics::default()
            .with_num_rows(Precision::Exact(10))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_min_value(Precision::Exact(ScalarValue::Int32(Some(5))))
                    .with_max_value(Precision::Exact(ScalarValue::Int32(Some(5))))
                    .with_distinct_count(Precision::Exact(1)),
            );

        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let merged = Statistics::try_merge_iter([&stats1, &stats2], &schema).unwrap();
        assert_eq!(
            merged.column_statistics[0].distinct_count,
            Precision::Inexact(1)
        );

        // Different constants: [5,5]+[10,10] -> sum
        let stats3 = Statistics::default()
            .with_num_rows(Precision::Exact(10))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_min_value(Precision::Exact(ScalarValue::Int32(Some(5))))
                    .with_max_value(Precision::Exact(ScalarValue::Int32(Some(5))))
                    .with_distinct_count(Precision::Exact(1)),
            );
        let stats4 = Statistics::default()
            .with_num_rows(Precision::Exact(10))
            .add_column_statistics(
                ColumnStatistics::new_unknown()
                    .with_min_value(Precision::Exact(ScalarValue::Int32(Some(10))))
                    .with_max_value(Precision::Exact(ScalarValue::Int32(Some(10))))
                    .with_distinct_count(Precision::Exact(1)),
            );

        let merged = Statistics::try_merge_iter([&stats3, &stats4], &schema).unwrap();
        assert_eq!(
            merged.column_statistics[0].distinct_count,
            Precision::Inexact(2)
        );
    }

    #[test]
    fn test_with_fetch_basic_preservation() {
        // Test that column statistics and byte size are preserved (as inexact) when applying fetch
        let original_stats = Statistics {
            num_rows: Precision::Exact(1000),
            total_byte_size: Precision::Exact(8000),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(10),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(100))),
                    min_value: Precision::Exact(ScalarValue::Int32(Some(0))),
                    sum_value: Precision::Exact(ScalarValue::Int32(Some(5050))),
                    distinct_count: Precision::Exact(50),
                    byte_size: Precision::Exact(4000),
                },
                ColumnStatistics {
                    null_count: Precision::Exact(20),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(200))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(10))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(10100))),
                    distinct_count: Precision::Exact(75),
                    byte_size: Precision::Exact(8000),
                },
            ],
        };

        // Apply fetch of 100 rows (10% of original)
        let result = original_stats.clone().with_fetch(Some(100), 0, 1).unwrap();

        // Check num_rows
        assert_eq!(result.num_rows, Precision::Exact(100));

        // Check total_byte_size is computed as sum of scaled column byte_size values
        // Column 1: 4000 * 0.1 = 400, Column 2: 8000 * 0.1 = 800, Sum = 1200
        assert_eq!(result.total_byte_size, Precision::Inexact(1200));

        // Check column statistics are preserved but marked as inexact
        assert_eq!(result.column_statistics.len(), 2);

        // First column
        assert_eq!(
            result.column_statistics[0].null_count,
            Precision::Inexact(10)
        );
        assert_eq!(
            result.column_statistics[0].max_value,
            Precision::Inexact(ScalarValue::Int32(Some(100)))
        );
        assert_eq!(
            result.column_statistics[0].min_value,
            Precision::Inexact(ScalarValue::Int32(Some(0)))
        );
        assert_eq!(
            result.column_statistics[0].sum_value,
            Precision::Inexact(ScalarValue::Int32(Some(5050)))
        );
        assert_eq!(
            result.column_statistics[0].distinct_count,
            Precision::Inexact(50)
        );

        // Second column
        assert_eq!(
            result.column_statistics[1].null_count,
            Precision::Inexact(20)
        );
        assert_eq!(
            result.column_statistics[1].max_value,
            Precision::Inexact(ScalarValue::Int64(Some(200)))
        );
        assert_eq!(
            result.column_statistics[1].min_value,
            Precision::Inexact(ScalarValue::Int64(Some(10)))
        );
        assert_eq!(
            result.column_statistics[1].sum_value,
            Precision::Inexact(ScalarValue::Int64(Some(10100)))
        );
        assert_eq!(
            result.column_statistics[1].distinct_count,
            Precision::Inexact(75)
        );
    }

    #[test]
    fn test_with_fetch_inexact_input() {
        // Test that inexact input statistics remain inexact
        let original_stats = Statistics {
            num_rows: Precision::Inexact(1000),
            total_byte_size: Precision::Inexact(8000),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Inexact(10),
                max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                min_value: Precision::Inexact(ScalarValue::Int32(Some(0))),
                sum_value: Precision::Inexact(ScalarValue::Int32(Some(5050))),
                distinct_count: Precision::Inexact(50),
                byte_size: Precision::Inexact(4000),
            }],
        };

        let result = original_stats.clone().with_fetch(Some(500), 0, 1).unwrap();

        // Check num_rows is inexact
        assert_eq!(result.num_rows, Precision::Inexact(500));

        // Check total_byte_size is computed as sum of scaled column byte_size values
        // Column 1: 4000 * 0.5 = 2000, Sum = 2000
        assert_eq!(result.total_byte_size, Precision::Inexact(2000));

        // Column stats remain inexact
        assert_eq!(
            result.column_statistics[0].null_count,
            Precision::Inexact(10)
        );
    }

    #[test]
    fn test_with_fetch_skip_all_rows() {
        // Test when skip >= num_rows (all rows are skipped)
        let original_stats = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Exact(800),
            column_statistics: vec![col_stats_i64(10)],
        };

        let result = original_stats.clone().with_fetch(Some(50), 100, 1).unwrap();

        assert_eq!(result.num_rows, Precision::Exact(0));
        // When ratio is 0/100 = 0, byte size should be 0
        assert_eq!(result.total_byte_size, Precision::Inexact(0));
    }

    #[test]
    fn test_with_fetch_no_limit() {
        // Test when fetch is None and skip is 0 (no limit applied)
        let original_stats = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Exact(800),
            column_statistics: vec![col_stats_i64(10)],
        };

        let result = original_stats.clone().with_fetch(None, 0, 1).unwrap();

        // Stats should be unchanged when no fetch and no skip
        assert_eq!(result.num_rows, Precision::Exact(100));
        assert_eq!(result.total_byte_size, Precision::Exact(800));
    }

    #[test]
    fn test_with_fetch_with_skip() {
        // Test with both skip and fetch
        let original_stats = Statistics {
            num_rows: Precision::Exact(1000),
            total_byte_size: Precision::Exact(8000),
            column_statistics: vec![col_stats_i64(10)],
        };

        // Skip 200, fetch 300, so we get rows 200-500
        let result = original_stats
            .clone()
            .with_fetch(Some(300), 200, 1)
            .unwrap();

        assert_eq!(result.num_rows, Precision::Exact(300));
        // Column 1: byte_size 800 * (300/500) = 240, Sum = 240
        assert_eq!(result.total_byte_size, Precision::Inexact(240));
    }

    #[test]
    fn test_with_fetch_multi_partition() {
        // Test with multiple partitions
        let original_stats = Statistics {
            num_rows: Precision::Exact(1000), // per partition
            total_byte_size: Precision::Exact(8000),
            column_statistics: vec![col_stats_i64(10)],
        };

        // Fetch 100 per partition, 4 partitions = 400 total
        let result = original_stats.clone().with_fetch(Some(100), 0, 4).unwrap();

        assert_eq!(result.num_rows, Precision::Exact(400));
        // Column 1: byte_size 800 * 0.4 = 320, Sum = 320
        assert_eq!(result.total_byte_size, Precision::Inexact(320));
    }

    #[test]
    fn test_with_fetch_absent_stats() {
        // Test with absent statistics
        let original_stats = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Absent,
                min_value: Precision::Absent,
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
                byte_size: Precision::Absent,
            }],
        };

        let result = original_stats.clone().with_fetch(Some(100), 0, 1).unwrap();

        // With absent input stats, output should be inexact estimate
        assert_eq!(result.num_rows, Precision::Inexact(100));
        assert_eq!(result.total_byte_size, Precision::Absent);
        // Column stats should remain absent
        assert_eq!(result.column_statistics[0].null_count, Precision::Absent);
    }

    #[test]
    fn test_with_fetch_fetch_exceeds_rows() {
        // Test when fetch is larger than available rows after skip
        let original_stats = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Exact(800),
            column_statistics: vec![col_stats_i64(10)],
        };

        // Skip 50, fetch 100, but only 50 rows remain
        let result = original_stats.clone().with_fetch(Some(100), 50, 1).unwrap();

        assert_eq!(result.num_rows, Precision::Exact(50));
        // 50/100 = 0.5, so 800 * 0.5 = 400
        assert_eq!(result.total_byte_size, Precision::Inexact(400));
    }

    #[test]
    fn test_with_fetch_preserves_all_column_stats() {
        // Comprehensive test that all column statistic fields are preserved
        let original_col_stats = ColumnStatistics {
            null_count: Precision::Exact(42),
            max_value: Precision::Exact(ScalarValue::Int32(Some(999))),
            min_value: Precision::Exact(ScalarValue::Int32(Some(-100))),
            sum_value: Precision::Exact(ScalarValue::Int32(Some(123456))),
            distinct_count: Precision::Exact(789),
            byte_size: Precision::Exact(4000),
        };

        let original_stats = Statistics {
            num_rows: Precision::Exact(1000),
            total_byte_size: Precision::Exact(8000),
            column_statistics: vec![original_col_stats.clone()],
        };

        let result = original_stats.with_fetch(Some(250), 0, 1).unwrap();

        let result_col_stats = &result.column_statistics[0];

        // All values should be preserved but marked as inexact
        assert_eq!(result_col_stats.null_count, Precision::Inexact(42));
        assert_eq!(
            result_col_stats.max_value,
            Precision::Inexact(ScalarValue::Int32(Some(999)))
        );
        assert_eq!(
            result_col_stats.min_value,
            Precision::Inexact(ScalarValue::Int32(Some(-100)))
        );
        assert_eq!(
            result_col_stats.sum_value,
            Precision::Inexact(ScalarValue::Int32(Some(123456)))
        );
        assert_eq!(result_col_stats.distinct_count, Precision::Inexact(789));
    }

    #[test]
    fn test_byte_size_to_inexact() {
        let col_stats = ColumnStatistics {
            null_count: Precision::Exact(10),
            max_value: Precision::Absent,
            min_value: Precision::Absent,
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Exact(5000),
        };

        let inexact = col_stats.to_inexact();
        assert_eq!(inexact.byte_size, Precision::Inexact(5000));
    }

    #[test]
    fn test_with_byte_size_builder() {
        let col_stats =
            ColumnStatistics::new_unknown().with_byte_size(Precision::Exact(8192));
        assert_eq!(col_stats.byte_size, Precision::Exact(8192));
    }

    #[test]
    fn test_with_sum_value_builder_widens_small_integers() {
        let col_stats = ColumnStatistics::new_unknown()
            .with_sum_value(Precision::Exact(ScalarValue::UInt32(Some(123))));
        assert_eq!(
            col_stats.sum_value,
            Precision::Exact(ScalarValue::UInt64(Some(123)))
        );
    }

    #[test]
    fn test_with_fetch_scales_byte_size() {
        // Test that byte_size is scaled by the row ratio in with_fetch
        let original_stats = Statistics {
            num_rows: Precision::Exact(1000),
            total_byte_size: Precision::Exact(8000),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(10),
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                    byte_size: Precision::Exact(4000),
                },
                ColumnStatistics {
                    null_count: Precision::Exact(20),
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                    byte_size: Precision::Exact(8000),
                },
            ],
        };

        // Apply fetch of 100 rows (10% of original)
        let result = original_stats.with_fetch(Some(100), 0, 1).unwrap();

        // byte_size should be scaled: 4000 * 0.1 = 400, 8000 * 0.1 = 800
        assert_eq!(
            result.column_statistics[0].byte_size,
            Precision::Inexact(400)
        );
        assert_eq!(
            result.column_statistics[1].byte_size,
            Precision::Inexact(800)
        );

        // total_byte_size should be computed as sum of byte_size values: 400 + 800 = 1200
        assert_eq!(result.total_byte_size, Precision::Inexact(1200));
    }

    #[test]
    fn test_with_fetch_total_byte_size_fallback() {
        // Test that total_byte_size falls back to scaling when not all columns have byte_size
        let original_stats = Statistics {
            num_rows: Precision::Exact(1000),
            total_byte_size: Precision::Exact(8000),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(10),
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                    byte_size: Precision::Exact(4000),
                },
                ColumnStatistics {
                    null_count: Precision::Exact(20),
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                    byte_size: Precision::Absent, // One column has no byte_size
                },
            ],
        };

        // Apply fetch of 100 rows (10% of original)
        let result = original_stats.with_fetch(Some(100), 0, 1).unwrap();

        // total_byte_size should fall back to scaling: 8000 * 0.1 = 800
        assert_eq!(result.total_byte_size, Precision::Inexact(800));
    }

    #[test]
    fn test_try_merge_iter_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int32, false),
            Field::new("col2", DataType::Int32, false),
        ]));

        let stats1 = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Exact(100),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(100))),
                    min_value: Precision::Exact(ScalarValue::Int32(Some(1))),
                    sum_value: Precision::Exact(ScalarValue::Int32(Some(500))),
                    distinct_count: Precision::Absent,
                    byte_size: Precision::Exact(40),
                },
                ColumnStatistics {
                    null_count: Precision::Exact(2),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(200))),
                    min_value: Precision::Exact(ScalarValue::Int32(Some(10))),
                    sum_value: Precision::Exact(ScalarValue::Int32(Some(1000))),
                    distinct_count: Precision::Absent,
                    byte_size: Precision::Exact(40),
                },
            ],
        };

        let stats2 = Statistics {
            num_rows: Precision::Exact(15),
            total_byte_size: Precision::Exact(150),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(2),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(120))),
                    min_value: Precision::Exact(ScalarValue::Int32(Some(-10))),
                    sum_value: Precision::Exact(ScalarValue::Int32(Some(600))),
                    distinct_count: Precision::Absent,
                    byte_size: Precision::Exact(60),
                },
                ColumnStatistics {
                    null_count: Precision::Exact(3),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(180))),
                    min_value: Precision::Exact(ScalarValue::Int32(Some(5))),
                    sum_value: Precision::Exact(ScalarValue::Int32(Some(1200))),
                    distinct_count: Precision::Absent,
                    byte_size: Precision::Exact(60),
                },
            ],
        };

        let items = vec![&stats1, &stats2];
        let summary_stats = Statistics::try_merge_iter(items, &schema).unwrap();

        assert_eq!(summary_stats.num_rows, Precision::Exact(25));
        assert_eq!(summary_stats.total_byte_size, Precision::Exact(250));

        let col1_stats = &summary_stats.column_statistics[0];
        assert_eq!(col1_stats.null_count, Precision::Exact(3));
        assert_eq!(
            col1_stats.max_value,
            Precision::Exact(ScalarValue::Int32(Some(120)))
        );
        assert_eq!(
            col1_stats.min_value,
            Precision::Exact(ScalarValue::Int32(Some(-10)))
        );
        assert_eq!(
            col1_stats.sum_value,
            Precision::Exact(ScalarValue::Int64(Some(1100)))
        );

        let col2_stats = &summary_stats.column_statistics[1];
        assert_eq!(col2_stats.null_count, Precision::Exact(5));
        assert_eq!(
            col2_stats.max_value,
            Precision::Exact(ScalarValue::Int32(Some(200)))
        );
        assert_eq!(
            col2_stats.min_value,
            Precision::Exact(ScalarValue::Int32(Some(5)))
        );
        assert_eq!(
            col2_stats.sum_value,
            Precision::Exact(ScalarValue::Int64(Some(2200)))
        );
    }

    #[test]
    fn test_try_merge_iter_mixed_precision() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        let stats1 = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Inexact(100),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(1),
                max_value: Precision::Exact(ScalarValue::Int32(Some(100))),
                min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                sum_value: Precision::Exact(ScalarValue::Int32(Some(500))),
                distinct_count: Precision::Absent,
                byte_size: Precision::Exact(40),
            }],
        };

        let stats2 = Statistics {
            num_rows: Precision::Inexact(15),
            total_byte_size: Precision::Exact(150),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Inexact(2),
                max_value: Precision::Inexact(ScalarValue::Int32(Some(120))),
                min_value: Precision::Exact(ScalarValue::Int32(Some(-10))),
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
                byte_size: Precision::Inexact(60),
            }],
        };

        let items = vec![&stats1, &stats2];
        let summary_stats = Statistics::try_merge_iter(items, &schema).unwrap();

        assert_eq!(summary_stats.num_rows, Precision::Inexact(25));
        assert_eq!(summary_stats.total_byte_size, Precision::Inexact(250));

        let col_stats = &summary_stats.column_statistics[0];
        assert_eq!(col_stats.null_count, Precision::Inexact(3));
        assert_eq!(
            col_stats.max_value,
            Precision::Inexact(ScalarValue::Int32(Some(120)))
        );
        assert_eq!(
            col_stats.min_value,
            Precision::Inexact(ScalarValue::Int32(Some(-10)))
        );
        // sum_value becomes Absent because stats2 has Absent sum
        assert_eq!(col_stats.sum_value, Precision::Absent);
    }

    #[test]
    fn test_try_merge_iter_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        let items: Vec<&Statistics> = vec![];
        let summary_stats = Statistics::try_merge_iter(items, &schema).unwrap();

        assert_eq!(summary_stats.num_rows, Precision::Absent);
        assert_eq!(summary_stats.total_byte_size, Precision::Absent);
        assert_eq!(summary_stats.column_statistics.len(), 1);
        assert_eq!(
            summary_stats.column_statistics[0].null_count,
            Precision::Absent
        );
    }

    #[test]
    fn test_try_merge_iter_single_item() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        let stats = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Exact(100),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(1),
                max_value: Precision::Exact(ScalarValue::Int32(Some(100))),
                min_value: Precision::Exact(ScalarValue::Int32(Some(1))),
                sum_value: Precision::Exact(ScalarValue::Int32(Some(500))),
                distinct_count: Precision::Exact(10),
                byte_size: Precision::Exact(40),
            }],
        };

        let items = vec![&stats];
        let summary_stats = Statistics::try_merge_iter(items, &schema).unwrap();

        assert_eq!(summary_stats, stats);
    }

    #[test]
    fn test_try_merge_iter_mismatched_columns() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        let stats1 = Statistics::default();
        let stats2 =
            Statistics::default().add_column_statistics(ColumnStatistics::new_unknown());

        let items = vec![&stats1, &stats2];
        let e = Statistics::try_merge_iter(items, &schema).unwrap_err();
        assert_contains!(
            e.to_string(),
            "Cannot merge statistics with different number of columns: 0 vs 1"
        );
    }

    #[test]
    fn test_try_merge_iter_three_items() {
        // Verify that merging three items works correctly
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int64,
            false,
        )]));

        let stats1 = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Exact(100),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(1),
                max_value: Precision::Exact(ScalarValue::Int64(Some(100))),
                min_value: Precision::Exact(ScalarValue::Int64(Some(10))),
                sum_value: Precision::Exact(ScalarValue::Int64(Some(500))),
                distinct_count: Precision::Exact(8),
                byte_size: Precision::Exact(80),
            }],
        };

        let stats2 = Statistics {
            num_rows: Precision::Exact(20),
            total_byte_size: Precision::Exact(200),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(2),
                max_value: Precision::Exact(ScalarValue::Int64(Some(200))),
                min_value: Precision::Exact(ScalarValue::Int64(Some(5))),
                sum_value: Precision::Exact(ScalarValue::Int64(Some(1000))),
                distinct_count: Precision::Exact(15),
                byte_size: Precision::Exact(160),
            }],
        };

        let stats3 = Statistics {
            num_rows: Precision::Exact(30),
            total_byte_size: Precision::Exact(300),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(3),
                max_value: Precision::Exact(ScalarValue::Int64(Some(150))),
                min_value: Precision::Exact(ScalarValue::Int64(Some(1))),
                sum_value: Precision::Exact(ScalarValue::Int64(Some(2000))),
                distinct_count: Precision::Exact(25),
                byte_size: Precision::Exact(240),
            }],
        };

        let items = vec![&stats1, &stats2, &stats3];
        let summary_stats = Statistics::try_merge_iter(items, &schema).unwrap();

        assert_eq!(summary_stats.num_rows, Precision::Exact(60));
        assert_eq!(summary_stats.total_byte_size, Precision::Exact(600));

        let col_stats = &summary_stats.column_statistics[0];
        assert_eq!(col_stats.null_count, Precision::Exact(6));
        assert_eq!(
            col_stats.max_value,
            Precision::Exact(ScalarValue::Int64(Some(200)))
        );
        assert_eq!(
            col_stats.min_value,
            Precision::Exact(ScalarValue::Int64(Some(1)))
        );
        assert_eq!(
            col_stats.sum_value,
            Precision::Exact(ScalarValue::Int64(Some(3500)))
        );
        assert_eq!(col_stats.byte_size, Precision::Exact(480));
        // Overlap-based NDV merge (pairwise left-to-right):
        // stats1+stats2: [10,100]+[5,200] -> NDV=16, then +stats3: [5,200]+[1,150] -> NDV=29
        assert_eq!(col_stats.distinct_count, Precision::Inexact(29));
    }

    #[test]
    fn test_try_merge_iter_float_types() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Float64,
            false,
        )]));

        let stats1 = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Exact(80),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(0),
                max_value: Precision::Exact(ScalarValue::Float64(Some(99.9))),
                min_value: Precision::Exact(ScalarValue::Float64(Some(1.1))),
                sum_value: Precision::Exact(ScalarValue::Float64(Some(500.5))),
                distinct_count: Precision::Absent,
                byte_size: Precision::Exact(80),
            }],
        };

        let stats2 = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Exact(80),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(0),
                max_value: Precision::Exact(ScalarValue::Float64(Some(200.0))),
                min_value: Precision::Exact(ScalarValue::Float64(Some(0.5))),
                sum_value: Precision::Exact(ScalarValue::Float64(Some(1000.0))),
                distinct_count: Precision::Absent,
                byte_size: Precision::Exact(80),
            }],
        };

        let items = vec![&stats1, &stats2];
        let summary_stats = Statistics::try_merge_iter(items, &schema).unwrap();

        let col_stats = &summary_stats.column_statistics[0];
        assert_eq!(
            col_stats.max_value,
            Precision::Exact(ScalarValue::Float64(Some(200.0)))
        );
        assert_eq!(
            col_stats.min_value,
            Precision::Exact(ScalarValue::Float64(Some(0.5)))
        );
        assert_eq!(
            col_stats.sum_value,
            Precision::Exact(ScalarValue::Float64(Some(1500.5)))
        );
    }

    #[test]
    fn test_try_merge_iter_string_types() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("col1", DataType::Utf8, false)]));

        let stats1 = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Exact(100),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(0),
                max_value: Precision::Exact(ScalarValue::Utf8(Some("dog".to_string()))),
                min_value: Precision::Exact(ScalarValue::Utf8(Some("ant".to_string()))),
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
                byte_size: Precision::Exact(100),
            }],
        };

        let stats2 = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Exact(100),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(0),
                max_value: Precision::Exact(ScalarValue::Utf8(Some("zebra".to_string()))),
                min_value: Precision::Exact(ScalarValue::Utf8(Some("bat".to_string()))),
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
                byte_size: Precision::Exact(100),
            }],
        };

        let items = vec![&stats1, &stats2];
        let summary_stats = Statistics::try_merge_iter(items, &schema).unwrap();

        let col_stats = &summary_stats.column_statistics[0];
        assert_eq!(
            col_stats.max_value,
            Precision::Exact(ScalarValue::Utf8(Some("zebra".to_string())))
        );
        assert_eq!(
            col_stats.min_value,
            Precision::Exact(ScalarValue::Utf8(Some("ant".to_string())))
        );
        assert_eq!(col_stats.sum_value, Precision::Absent);
    }

    #[test]
    fn test_try_merge_iter_all_inexact() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        let stats1 = Statistics {
            num_rows: Precision::Inexact(10),
            total_byte_size: Precision::Inexact(100),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Inexact(1),
                max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                sum_value: Precision::Inexact(ScalarValue::Int32(Some(500))),
                distinct_count: Precision::Absent,
                byte_size: Precision::Inexact(40),
            }],
        };

        let stats2 = Statistics {
            num_rows: Precision::Inexact(20),
            total_byte_size: Precision::Inexact(200),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Inexact(2),
                max_value: Precision::Inexact(ScalarValue::Int32(Some(200))),
                min_value: Precision::Inexact(ScalarValue::Int32(Some(-5))),
                sum_value: Precision::Inexact(ScalarValue::Int32(Some(1000))),
                distinct_count: Precision::Absent,
                byte_size: Precision::Inexact(60),
            }],
        };

        let items = vec![&stats1, &stats2];
        let summary_stats = Statistics::try_merge_iter(items, &schema).unwrap();

        assert_eq!(summary_stats.num_rows, Precision::Inexact(30));
        assert_eq!(summary_stats.total_byte_size, Precision::Inexact(300));

        let col_stats = &summary_stats.column_statistics[0];
        assert_eq!(col_stats.null_count, Precision::Inexact(3));
        assert_eq!(
            col_stats.max_value,
            Precision::Inexact(ScalarValue::Int32(Some(200)))
        );
        assert_eq!(
            col_stats.min_value,
            Precision::Inexact(ScalarValue::Int32(Some(-5)))
        );
        assert_eq!(
            col_stats.sum_value,
            Precision::Inexact(ScalarValue::Int64(Some(1500)))
        );
    }

    #[test]
    fn test_precision_min_in_place() {
        // Exact vs Exact: keeps the smaller
        let mut lhs = Precision::Exact(10);
        precision_min(&mut lhs, &Precision::Exact(20));
        assert_eq!(lhs, Precision::Exact(10));

        let mut lhs = Precision::Exact(20);
        precision_min(&mut lhs, &Precision::Exact(10));
        assert_eq!(lhs, Precision::Exact(10));

        // Equal exact values
        let mut lhs = Precision::Exact(5);
        precision_min(&mut lhs, &Precision::Exact(5));
        assert_eq!(lhs, Precision::Exact(5));

        // Mixed exact/inexact: result is Inexact with smaller value
        let mut lhs = Precision::Exact(10);
        precision_min(&mut lhs, &Precision::Inexact(20));
        assert_eq!(lhs, Precision::Inexact(10));

        let mut lhs = Precision::Inexact(10);
        precision_min(&mut lhs, &Precision::Exact(5));
        assert_eq!(lhs, Precision::Inexact(5));

        // Inexact vs Inexact
        let mut lhs = Precision::Inexact(30);
        precision_min(&mut lhs, &Precision::Inexact(20));
        assert_eq!(lhs, Precision::Inexact(20));

        // Absent makes result Absent
        let mut lhs = Precision::Exact(10);
        precision_min(&mut lhs, &Precision::Absent);
        assert_eq!(lhs, Precision::Absent);

        let mut lhs = Precision::<i32>::Absent;
        precision_min(&mut lhs, &Precision::Exact(10));
        assert_eq!(lhs, Precision::Absent);
    }

    #[test]
    fn test_precision_max_in_place() {
        // Exact vs Exact: keeps the larger
        let mut lhs = Precision::Exact(10);
        precision_max(&mut lhs, &Precision::Exact(20));
        assert_eq!(lhs, Precision::Exact(20));

        let mut lhs = Precision::Exact(20);
        precision_max(&mut lhs, &Precision::Exact(10));
        assert_eq!(lhs, Precision::Exact(20));

        // Equal exact values
        let mut lhs = Precision::Exact(5);
        precision_max(&mut lhs, &Precision::Exact(5));
        assert_eq!(lhs, Precision::Exact(5));

        // Mixed exact/inexact: result is Inexact with larger value
        let mut lhs = Precision::Exact(10);
        precision_max(&mut lhs, &Precision::Inexact(20));
        assert_eq!(lhs, Precision::Inexact(20));

        let mut lhs = Precision::Inexact(10);
        precision_max(&mut lhs, &Precision::Exact(5));
        assert_eq!(lhs, Precision::Inexact(10));

        // Inexact vs Inexact
        let mut lhs = Precision::Inexact(20);
        precision_max(&mut lhs, &Precision::Inexact(30));
        assert_eq!(lhs, Precision::Inexact(30));

        // Absent makes result Absent
        let mut lhs = Precision::Exact(10);
        precision_max(&mut lhs, &Precision::Absent);
        assert_eq!(lhs, Precision::Absent);

        let mut lhs = Precision::<i32>::Absent;
        precision_max(&mut lhs, &Precision::Exact(10));
        assert_eq!(lhs, Precision::Absent);
    }

    #[test]
    fn test_cast_sum_value_to_sum_type_in_place_widens_int32() {
        let mut value = Precision::Exact(ScalarValue::Int32(Some(42)));
        cast_sum_value_to_sum_type_in_place(&mut value);
        assert_eq!(value, Precision::Exact(ScalarValue::Int64(Some(42))));
    }

    #[test]
    fn test_cast_sum_value_to_sum_type_in_place_preserves_int64() {
        // Int64 is already the sum type for Int64, no widening needed
        let mut value = Precision::Exact(ScalarValue::Int64(Some(100)));
        cast_sum_value_to_sum_type_in_place(&mut value);
        assert_eq!(value, Precision::Exact(ScalarValue::Int64(Some(100))));
    }

    #[test]
    fn test_cast_sum_value_to_sum_type_in_place_inexact() {
        let mut value = Precision::Inexact(ScalarValue::Int32(Some(42)));
        cast_sum_value_to_sum_type_in_place(&mut value);
        assert_eq!(value, Precision::Inexact(ScalarValue::Int64(Some(42))));
    }

    #[test]
    fn test_cast_sum_value_to_sum_type_in_place_absent() {
        let mut value = Precision::<ScalarValue>::Absent;
        cast_sum_value_to_sum_type_in_place(&mut value);
        assert_eq!(value, Precision::Absent);
    }

    #[test]
    fn test_precision_add_for_sum_in_place_same_type() {
        // Int64 + Int64: no widening needed, straight add
        let mut lhs = Precision::Exact(ScalarValue::Int64(Some(10)));
        let rhs = Precision::Exact(ScalarValue::Int64(Some(20)));
        precision_add_for_sum_in_place(&mut lhs, &rhs);
        assert_eq!(lhs, Precision::Exact(ScalarValue::Int64(Some(30))));
    }

    #[test]
    fn test_precision_add_for_sum_in_place_widens_rhs() {
        // lhs is already Int64 (widened), rhs is Int32 -> gets cast to Int64
        let mut lhs = Precision::Exact(ScalarValue::Int64(Some(10)));
        let rhs = Precision::Exact(ScalarValue::Int32(Some(5)));
        precision_add_for_sum_in_place(&mut lhs, &rhs);
        assert_eq!(lhs, Precision::Exact(ScalarValue::Int64(Some(15))));
    }

    #[test]
    fn test_precision_add_for_sum_in_place_inexact() {
        let mut lhs = Precision::Inexact(ScalarValue::Int64(Some(10)));
        let rhs = Precision::Inexact(ScalarValue::Int32(Some(5)));
        precision_add_for_sum_in_place(&mut lhs, &rhs);
        assert_eq!(lhs, Precision::Inexact(ScalarValue::Int64(Some(15))));
    }

    #[test]
    fn test_precision_add_for_sum_in_place_absent_rhs() {
        let mut lhs = Precision::Exact(ScalarValue::Int64(Some(10)));
        precision_add_for_sum_in_place(&mut lhs, &Precision::Absent);
        assert_eq!(lhs, Precision::Absent);
    }
}
