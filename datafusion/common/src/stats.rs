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

use arrow::datatypes::{DataType, Schema, SchemaRef};

/// Represents a value with a degree of certainty. `Precision` is used to
/// propagate information the precision of statistical values.
#[derive(Clone, PartialEq, Eq, Default, Copy)]
pub enum Precision<T: Debug + Clone + PartialEq + Eq + PartialOrd> {
    /// The exact value is known
    Exact(T),
    /// The value is not known exactly, but is likely close to this value
    Inexact(T),
    /// Nothing is known about the value
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
            (Precision::Exact(a), Precision::Exact(b)) => Precision::Exact(a + b),
            (Precision::Inexact(a), Precision::Exact(b))
            | (Precision::Exact(a), Precision::Inexact(b))
            | (Precision::Inexact(a), Precision::Inexact(b)) => Precision::Inexact(a + b),
            (_, _) => Precision::Absent,
        }
    }

    /// Calculates the difference of two (possibly inexact) [`usize`] values,
    /// conservatively propagating exactness information. If one of the input
    /// values is [`Precision::Absent`], the result is `Absent` too.
    pub fn sub(&self, other: &Precision<usize>) -> Precision<usize> {
        match (self, other) {
            (Precision::Exact(a), Precision::Exact(b)) => Precision::Exact(a - b),
            (Precision::Inexact(a), Precision::Exact(b))
            | (Precision::Exact(a), Precision::Inexact(b))
            | (Precision::Inexact(a), Precision::Inexact(b)) => Precision::Inexact(a - b),
            (_, _) => Precision::Absent,
        }
    }

    /// Calculates the multiplication of two (possibly inexact) [`usize`] values,
    /// conservatively propagating exactness information. If one of the input
    /// values is [`Precision::Absent`], the result is `Absent` too.
    pub fn multiply(&self, other: &Precision<usize>) -> Precision<usize> {
        match (self, other) {
            (Precision::Exact(a), Precision::Exact(b)) => Precision::Exact(a * b),
            (Precision::Inexact(a), Precision::Exact(b))
            | (Precision::Exact(a), Precision::Inexact(b))
            | (Precision::Inexact(a), Precision::Inexact(b)) => Precision::Inexact(a * b),
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
    /// Calculates the sum of two (possibly inexact) [`ScalarValue`] values,
    /// conservatively propagating exactness information. If one of the input
    /// values is [`Precision::Absent`], the result is `Absent` too.
    pub fn add(&self, other: &Precision<ScalarValue>) -> Precision<ScalarValue> {
        match (self, other) {
            (Precision::Exact(a), Precision::Exact(b)) => {
                a.add(b).map(Precision::Exact).unwrap_or(Precision::Absent)
            }
            (Precision::Inexact(a), Precision::Exact(b))
            | (Precision::Exact(a), Precision::Inexact(b))
            | (Precision::Inexact(a), Precision::Inexact(b)) => a
                .add(b)
                .map(Precision::Inexact)
                .unwrap_or(Precision::Absent),
            (_, _) => Precision::Absent,
        }
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
            Precision::Exact(inner) => write!(f, "Exact({:?})", inner),
            Precision::Inexact(inner) => write!(f, "Inexact({:?})", inner),
            Precision::Absent => write!(f, "Absent"),
        }
    }
}

impl<T: Debug + Clone + PartialEq + Eq + PartialOrd> Display for Precision<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Precision::Exact(inner) => write!(f, "Exact({:?})", inner),
            Precision::Inexact(inner) => write!(f, "Inexact({:?})", inner),
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
    /// The number of table rows.
    pub num_rows: Precision<usize>,
    /// Total bytes of the table rows.
    pub total_byte_size: Precision<usize>,
    /// Statistics on a column level. It contains a [`ColumnStatistics`] for
    /// each field in the schema of the table to which the [`Statistics`] refer.
    pub column_statistics: Vec<ColumnStatistics>,
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

    /// Returns an unbounded `ColumnStatistics` for each field in the schema.
    pub fn unknown_column(schema: &Schema) -> Vec<ColumnStatistics> {
        schema
            .fields()
            .iter()
            .map(|_| ColumnStatistics::new_unknown())
            .collect()
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
    pub fn project(mut self, projection: Option<&Vec<usize>>) -> Self {
        let Some(projection) = projection else {
            return self;
        };

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

        for idx in projection {
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

    /// Calculates the statistics after `fetch` and `skip` operations apply.
    /// Here, `self` denotes per-partition statistics. Use the `n_partitions`
    /// parameter to compute global statistics in a multi-partition setting.
    pub fn with_fetch(
        mut self,
        schema: SchemaRef,
        fetch: Option<usize>,
        skip: usize,
        n_partitions: usize,
    ) -> Result<Self> {
        let fetch_val = fetch.unwrap_or(usize::MAX);

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
                    // of rows will be the `fetch`, but we won't be able to
                    // predict the other statistics.
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
        self.column_statistics = Statistics::unknown_column(&schema);
        self.total_byte_size = Precision::Absent;
        Ok(self)
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
                let s = format!("(Col[{}]:", i);
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
    /// Sum value of a column
    pub sum_value: Precision<ScalarValue>,
    /// Number of distinct values
    pub distinct_count: Precision<usize>,
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
        }
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
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        assert_eq!(precision1.add(&precision2), Precision::Inexact(65));
        assert_eq!(precision1.add(&precision3), Precision::Exact(72));
        assert_eq!(precision2.add(&precision3), Precision::Inexact(53));
        assert_eq!(precision1.add(&absent_precision), Precision::Absent);
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
    fn test_sub() {
        let precision1 = Precision::Exact(42);
        let precision2 = Precision::Inexact(23);
        let precision3 = Precision::Exact(30);
        let absent_precision = Precision::Absent;

        assert_eq!(precision1.sub(&precision2), Precision::Inexact(19));
        assert_eq!(precision1.sub(&precision3), Precision::Exact(12));
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
        let absent_precision = Precision::Absent;

        assert_eq!(precision1.multiply(&precision2), Precision::Inexact(18));
        assert_eq!(precision1.multiply(&precision3), Precision::Exact(30));
        assert_eq!(precision2.multiply(&precision3), Precision::Inexact(15));
        assert_eq!(precision1.multiply(&absent_precision), Precision::Absent);
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
        assert!(Precision::Exact(ScalarValue::Int32(Some(256)))
            .cast_to(&DataType::Int8)
            .is_err());
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
        // Clippy would complain about this if it were Copy
        #[allow(clippy::redundant_clone)]
        let p2 = precision.clone();
        assert_eq!(precision, p2);
    }

    #[test]
    fn test_project_none() {
        let projection = None;
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
        }
    }
}
