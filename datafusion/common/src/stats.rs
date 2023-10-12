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

/// To deal with information without exactness guarantees, we wrap it inside a
/// [`Sharpness`] object to express its reliability.
#[derive(Clone, PartialEq, Eq, Default)]
pub enum Sharpness<T: Debug + Clone + PartialEq + Eq + PartialOrd> {
    Exact(T),
    Inexact(T),
    #[default]
    Absent,
}

impl<T: Debug + Clone + PartialEq + Eq + PartialOrd> Sharpness<T> {
    /// If we have some value (exact or inexact), it returns that value.
    /// Otherwise, it returns `None`.
    pub fn get_value(&self) -> Option<&T> {
        match self {
            Sharpness::Exact(value) | Sharpness::Inexact(value) => Some(value),
            Sharpness::Absent => None,
        }
    }

    /// Transform the value in this [`Sharpness`] object, if one exists, using
    /// the given function. Preserves the exactness state.
    pub fn map<F>(self, f: F) -> Sharpness<T>
    where
        F: Fn(T) -> T,
    {
        match self {
            Sharpness::Exact(val) => Sharpness::Exact(f(val)),
            Sharpness::Inexact(val) => Sharpness::Inexact(f(val)),
            _ => self,
        }
    }

    /// Returns `Some(true)` if we have an exact value, `Some(false)` if we
    /// have an inexact value, and `None` if there is no value.
    pub fn is_exact(&self) -> Option<bool> {
        match self {
            Sharpness::Exact(_) => Some(true),
            Sharpness::Inexact(_) => Some(false),
            _ => None,
        }
    }

    /// Returns the maximum of two (possibly inexact) values, conservatively
    /// propagating exactness information. If one of the input values is
    /// [`Sharpness::Absent`], the result is `Absent` too.
    pub fn max(&self, other: &Sharpness<T>) -> Sharpness<T> {
        match (self, other) {
            (Sharpness::Exact(a), Sharpness::Exact(b)) => {
                Sharpness::Exact(if a >= b { a.clone() } else { b.clone() })
            }
            (Sharpness::Inexact(a), Sharpness::Exact(b))
            | (Sharpness::Exact(a), Sharpness::Inexact(b))
            | (Sharpness::Inexact(a), Sharpness::Inexact(b)) => {
                Sharpness::Inexact(if a >= b { a.clone() } else { b.clone() })
            }
            (_, _) => Sharpness::Absent,
        }
    }

    /// Returns the minimum of two (possibly inexact) values, conservatively
    /// propagating exactness information. If one of the input values is
    /// [`Sharpness::Absent`], the result is `Absent` too.
    pub fn min(&self, other: &Sharpness<T>) -> Sharpness<T> {
        match (self, other) {
            (Sharpness::Exact(a), Sharpness::Exact(b)) => {
                Sharpness::Exact(if a >= b { b.clone() } else { a.clone() })
            }
            (Sharpness::Inexact(a), Sharpness::Exact(b))
            | (Sharpness::Exact(a), Sharpness::Inexact(b))
            | (Sharpness::Inexact(a), Sharpness::Inexact(b)) => {
                Sharpness::Inexact(if a >= b { b.clone() } else { a.clone() })
            }
            (_, _) => Sharpness::Absent,
        }
    }

    /// Demotes the sharpness state to inexact (if present).
    pub fn to_inexact(self) -> Self {
        match self {
            Sharpness::Exact(value) => Sharpness::Inexact(value),
            _ => self,
        }
    }
}

impl Sharpness<usize> {
    /// Calculates the sum of two (possibly inexact) [`usize`] values,
    /// conservatively propagating exactness information. If one of the input
    /// values is [`Sharpness::Absent`], the result is `Absent` too.
    pub fn add(&self, other: &Sharpness<usize>) -> Sharpness<usize> {
        match (self, other) {
            (Sharpness::Exact(a), Sharpness::Exact(b)) => Sharpness::Exact(a + b),
            (Sharpness::Inexact(a), Sharpness::Exact(b))
            | (Sharpness::Exact(a), Sharpness::Inexact(b))
            | (Sharpness::Inexact(a), Sharpness::Inexact(b)) => Sharpness::Inexact(a + b),
            (_, _) => Sharpness::Absent,
        }
    }

    /// Calculates the difference of two (possibly inexact) [`usize`] values,
    /// conservatively propagating exactness information. If one of the input
    /// values is [`Sharpness::Absent`], the result is `Absent` too.
    pub fn sub(&self, other: &Sharpness<usize>) -> Sharpness<usize> {
        match (self, other) {
            (Sharpness::Exact(a), Sharpness::Exact(b)) => Sharpness::Exact(a - b),
            (Sharpness::Inexact(a), Sharpness::Exact(b))
            | (Sharpness::Exact(a), Sharpness::Inexact(b))
            | (Sharpness::Inexact(a), Sharpness::Inexact(b)) => Sharpness::Inexact(a - b),
            (_, _) => Sharpness::Absent,
        }
    }

    /// Calculates the multiplication of two (possibly inexact) [`usize`] values,
    /// conservatively propagating exactness information. If one of the input
    /// values is [`Sharpness::Absent`], the result is `Absent` too.
    pub fn multiply(&self, other: &Sharpness<usize>) -> Sharpness<usize> {
        match (self, other) {
            (Sharpness::Exact(a), Sharpness::Exact(b)) => Sharpness::Exact(a * b),
            (Sharpness::Inexact(a), Sharpness::Exact(b))
            | (Sharpness::Exact(a), Sharpness::Inexact(b))
            | (Sharpness::Inexact(a), Sharpness::Inexact(b)) => Sharpness::Inexact(a * b),
            (_, _) => Sharpness::Absent,
        }
    }
}

impl Sharpness<ScalarValue> {
    /// Calculates the sum of two (possibly inexact) [`ScalarValue`] values,
    /// conservatively propagating exactness information. If one of the input
    /// values is [`Sharpness::Absent`], the result is `Absent` too.
    pub fn add(&self, other: &Sharpness<ScalarValue>) -> Sharpness<ScalarValue> {
        match (self, other) {
            (Sharpness::Exact(a), Sharpness::Exact(b)) => {
                if let Ok(result) = a.add(b) {
                    Sharpness::Exact(result)
                } else {
                    Sharpness::Absent
                }
            }
            (Sharpness::Inexact(a), Sharpness::Exact(b))
            | (Sharpness::Exact(a), Sharpness::Inexact(b))
            | (Sharpness::Inexact(a), Sharpness::Inexact(b)) => {
                if let Ok(result) = a.add(b) {
                    Sharpness::Inexact(result)
                } else {
                    Sharpness::Absent
                }
            }
            (_, _) => Sharpness::Absent,
        }
    }
}

impl<T: fmt::Debug + Clone + PartialEq + Eq + PartialOrd> Debug for Sharpness<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Sharpness::Exact(inner) => write!(f, "Exact({:?})", inner),
            Sharpness::Inexact(inner) => write!(f, "Inexact({:?})", inner),
            Sharpness::Absent => write!(f, "Absent"),
        }
    }
}

impl<T: fmt::Debug + Clone + PartialEq + Eq + PartialOrd> Display for Sharpness<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Sharpness::Exact(inner) => write!(f, "Exact({:?})", inner),
            Sharpness::Inexact(inner) => write!(f, "Inexact({:?})", inner),
            Sharpness::Absent => write!(f, "Absent"),
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
        let exact_sharpness = Sharpness::Exact(42);
        let inexact_sharpness = Sharpness::Inexact(23);
        let absent_sharpness = Sharpness::<i32>::Absent;

        assert_eq!(*exact_sharpness.get_value().unwrap(), 42);
        assert_eq!(*inexact_sharpness.get_value().unwrap(), 23);
        assert_eq!(absent_sharpness.get_value(), None);
    }

    #[test]
    fn test_map() {
        let exact_sharpness = Sharpness::Exact(42);
        let inexact_sharpness = Sharpness::Inexact(23);
        let absent_sharpness = Sharpness::Absent;

        let squared = |x| x * x;

        assert_eq!(exact_sharpness.map(squared), Sharpness::Exact(1764));
        assert_eq!(inexact_sharpness.map(squared), Sharpness::Inexact(529));
        assert_eq!(absent_sharpness.map(squared), Sharpness::Absent);
    }

    #[test]
    fn test_is_exact() {
        let exact_sharpness = Sharpness::Exact(42);
        let inexact_sharpness = Sharpness::Inexact(23);
        let absent_sharpness = Sharpness::<i32>::Absent;

        assert_eq!(exact_sharpness.is_exact(), Some(true));
        assert_eq!(inexact_sharpness.is_exact(), Some(false));
        assert_eq!(absent_sharpness.is_exact(), None);
    }

    #[test]
    fn test_max() {
        let sharpness1 = Sharpness::Exact(42);
        let sharpness2 = Sharpness::Inexact(23);
        let sharpness3 = Sharpness::Exact(30);
        let absent_sharpness = Sharpness::Absent;

        assert_eq!(sharpness1.max(&sharpness2), Sharpness::Inexact(42));
        assert_eq!(sharpness1.max(&sharpness3), Sharpness::Exact(42));
        assert_eq!(sharpness2.max(&sharpness3), Sharpness::Inexact(30));
        assert_eq!(sharpness1.max(&absent_sharpness), Sharpness::Absent);
    }

    #[test]
    fn test_min() {
        let sharpness1 = Sharpness::Exact(42);
        let sharpness2 = Sharpness::Inexact(23);
        let sharpness3 = Sharpness::Exact(30);
        let absent_sharpness = Sharpness::Absent;

        assert_eq!(sharpness1.min(&sharpness2), Sharpness::Inexact(23));
        assert_eq!(sharpness1.min(&sharpness3), Sharpness::Exact(30));
        assert_eq!(sharpness2.min(&sharpness3), Sharpness::Inexact(23));
        assert_eq!(sharpness1.min(&absent_sharpness), Sharpness::Absent);
    }

    #[test]
    fn test_to_inexact() {
        let exact_sharpness = Sharpness::Exact(42);
        let inexact_sharpness = Sharpness::Inexact(23);
        let absent_sharpness = Sharpness::<i32>::Absent;

        assert_eq!(exact_sharpness.clone().to_inexact(), inexact_sharpness);
        assert_eq!(inexact_sharpness.clone().to_inexact(), inexact_sharpness);
        assert_eq!(absent_sharpness.clone().to_inexact(), absent_sharpness);
    }

    #[test]
    fn test_add() {
        let sharpness1 = Sharpness::Exact(42);
        let sharpness2 = Sharpness::Inexact(23);
        let sharpness3 = Sharpness::Exact(30);
        let absent_sharpness = Sharpness::Absent;

        assert_eq!(sharpness1.add(&sharpness2), Sharpness::Inexact(65));
        assert_eq!(sharpness1.add(&sharpness3), Sharpness::Exact(72));
        assert_eq!(sharpness2.add(&sharpness3), Sharpness::Inexact(53));
        assert_eq!(sharpness1.add(&absent_sharpness), Sharpness::Absent);
    }

    #[test]
    fn test_sub() {
        let sharpness1 = Sharpness::Exact(42);
        let sharpness2 = Sharpness::Inexact(23);
        let sharpness3 = Sharpness::Exact(30);
        let absent_sharpness = Sharpness::Absent;

        assert_eq!(sharpness1.sub(&sharpness2), Sharpness::Inexact(19));
        assert_eq!(sharpness1.sub(&sharpness3), Sharpness::Exact(12));
        assert_eq!(sharpness1.sub(&absent_sharpness), Sharpness::Absent);
    }

    #[test]
    fn test_multiply() {
        let sharpness1 = Sharpness::Exact(6);
        let sharpness2 = Sharpness::Inexact(3);
        let sharpness3 = Sharpness::Exact(5);
        let absent_sharpness = Sharpness::Absent;

        assert_eq!(sharpness1.multiply(&sharpness2), Sharpness::Inexact(18));
        assert_eq!(sharpness1.multiply(&sharpness3), Sharpness::Exact(30));
        assert_eq!(sharpness2.multiply(&sharpness3), Sharpness::Inexact(15));
        assert_eq!(sharpness1.multiply(&absent_sharpness), Sharpness::Absent);
    }
}
