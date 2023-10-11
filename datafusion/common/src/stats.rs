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

use crate::ScalarValue;

use arrow::datatypes::Schema;

/// To deal with information without exactness guarantees, we wrap it inside a
/// [`Sharpness`] object to express its reliability. See [`Statistics`] for a usage.
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statistics {
    /// The number of table rows
    pub num_rows: Sharpness<usize>,
    /// total bytes of the table rows
    pub total_byte_size: Sharpness<usize>,
    /// Statistics on a column level
    pub column_statistics: Vec<ColumnStatistics>,
}

impl Statistics {
    /// Returns a [`Statistics`] instance for the given schema by assigning
    /// unknown statistics to each column in the schema.
    pub fn new_unknown(schema: &Schema) -> Self {
        Self {
            num_rows: Sharpness::Absent,
            total_byte_size: Sharpness::Absent,
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
    /// the exactness of all information by converting them [`Sharpness::Inexact`].
    pub fn make_inexact(self) -> Self {
        Statistics {
            num_rows: self.num_rows.to_inexact(),
            total_byte_size: self.total_byte_size.to_inexact(),
            column_statistics: self
                .column_statistics
                .into_iter()
                .map(|cs| ColumnStatistics {
                    null_count: cs.null_count.to_inexact(),
                    max_value: cs.max_value.to_inexact(),
                    min_value: cs.min_value.to_inexact(),
                    distinct_count: cs.distinct_count.to_inexact(),
                })
                .collect::<Vec<_>>(),
        }
    }
}

impl Display for Statistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Rows={}, Bytes={}", self.num_rows, self.total_byte_size)?;

        Ok(())
    }
}

/// Statistics for a column within a relation
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ColumnStatistics {
    /// Number of null values on column
    pub null_count: Sharpness<usize>,
    /// Maximum value of column
    pub max_value: Sharpness<ScalarValue>,
    /// Minimum value of column
    pub min_value: Sharpness<ScalarValue>,
    /// Number of distinct values
    pub distinct_count: Sharpness<usize>,
}

impl ColumnStatistics {
    /// Column contains a single non null value (e.g constant).
    pub fn is_singleton(&self) -> bool {
        match (self.min_value.get_value(), self.max_value.get_value()) {
            // Min and max values are the same and not infinity.
            (Some(min), Some(max)) => !min.is_null() && !max.is_null() && (min == max),
            (_, _) => false,
        }
    }

    /// Returns a [`ColumnStatistics`] instance having all [`Sharpness::Absent`] parameters.
    pub fn new_unknown() -> ColumnStatistics {
        ColumnStatistics {
            null_count: Sharpness::Absent,
            max_value: Sharpness::Absent,
            min_value: Sharpness::Absent,
            distinct_count: Sharpness::Absent,
        }
    }
}
