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

use crate::ScalarValue;
use arrow::datatypes::DataType;

use std::fmt::{self, Debug, Display};

/// Represents a value with a degree of certainty. `Precision` is used to
/// propagate information the precision of statistical values.
#[derive(Clone, PartialEq, Eq, Default)]
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
    pub fn map<F>(self, f: F) -> Precision<T>
    where
        F: Fn(T) -> T,
    {
        match self {
            Precision::Exact(val) => Precision::Exact(f(val)),
            Precision::Inexact(val) => Precision::Inexact(f(val)),
            _ => self,
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
}

impl Precision<ScalarValue> {
    /// Calculates the sum of two (possibly inexact) [`ScalarValue`] values,
    /// conservatively propagating exactness information. If one of the input
    /// values is [`Precision::Absent`], the result is `Absent` too.
    pub fn add(&self, other: &Precision<ScalarValue>) -> Precision<ScalarValue> {
        match (self, other) {
            (Precision::Exact(a), Precision::Exact(b)) => {
                if let Ok(result) = a.add(b) {
                    Precision::Exact(result)
                } else {
                    Precision::Absent
                }
            }
            (Precision::Inexact(a), Precision::Exact(b))
            | (Precision::Exact(a), Precision::Inexact(b))
            | (Precision::Inexact(a), Precision::Inexact(b)) => {
                if let Ok(result) = a.add(b) {
                    Precision::Inexact(result)
                } else {
                    Precision::Absent
                }
            }
            (_, _) => Precision::Absent,
        }
    }
}

impl<T: fmt::Debug + Clone + PartialEq + Eq + PartialOrd> Debug for Precision<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Precision::Exact(inner) => write!(f, "Exact({:?})", inner),
            Precision::Inexact(inner) => write!(f, "Inexact({:?})", inner),
            Precision::Absent => write!(f, "Absent"),
        }
    }
}

impl<T: fmt::Debug + Clone + PartialEq + Eq + PartialOrd> Display for Precision<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Precision::Exact(inner) => write!(f, "Exact({:?})", inner),
            Precision::Inexact(inner) => write!(f, "Inexact({:?})", inner),
            Precision::Absent => write!(f, "Absent"),
        }
    }
}

/// Statistics for a relation
/// Fields are optional and can be inexact because the sources
/// sometimes provide approximate estimates for performance reasons
/// and the transformations output are not always predictable.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Statistics {
    /// The number of table rows
    pub num_rows: Option<usize>,
    /// total bytes of the table rows
    pub total_byte_size: Option<usize>,
    /// Statistics on a column level
    pub column_statistics: Option<Vec<ColumnStatistics>>,
    /// If true, any field that is `Some(..)` is the actual value in the data provided by the operator (it is not
    /// an estimate). Any or all other fields might still be None, in which case no information is known.
    /// if false, any field that is `Some(..)` may contain an inexact estimate and may not be the actual value.
    pub is_exact: bool,
}

impl Display for Statistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.num_rows.is_none() && self.total_byte_size.is_none() && !self.is_exact {
            return Ok(());
        }

        let rows = self
            .num_rows
            .map_or_else(|| "None".to_string(), |v| v.to_string());
        let bytes = self
            .total_byte_size
            .map_or_else(|| "None".to_string(), |v| v.to_string());

        write!(f, "rows={}, bytes={}, exact={}", rows, bytes, self.is_exact)?;

        Ok(())
    }
}

/// Statistics for a column within a relation
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ColumnStatistics {
    /// Number of null values on column
    pub null_count: Option<usize>,
    /// Maximum value of column
    pub max_value: Option<ScalarValue>,
    /// Minimum value of column
    pub min_value: Option<ScalarValue>,
    /// Number of distinct values
    pub distinct_count: Option<usize>,
}

impl ColumnStatistics {
    /// Column contains a single non null value (e.g constant).
    pub fn is_singleton(&self) -> bool {
        match (&self.min_value, &self.max_value) {
            // Min and max values are the same and not infinity.
            (Some(min), Some(max)) => !min.is_null() && !max.is_null() && (min == max),
            (_, _) => false,
        }
    }

    /// Returns the [`ColumnStatistics`] corresponding to the given datatype by assigning infinite bounds.
    pub fn new_with_unbounded_column(dt: &DataType) -> ColumnStatistics {
        let null = ScalarValue::try_from(dt.clone()).ok();
        ColumnStatistics {
            null_count: None,
            max_value: null.clone(),
            min_value: null,
            distinct_count: None,
        }
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

        assert_eq!(exact_precision.clone().to_inexact(), inexact_precision);
        assert_eq!(inexact_precision.clone().to_inexact(), inexact_precision);
        assert_eq!(absent_precision.clone().to_inexact(), absent_precision);
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
}
