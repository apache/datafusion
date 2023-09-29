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
use arrow::datatypes::{DataType, Schema};
use core::fmt::Debug;
use std::fmt::{self, Display};

/// To deal with information whose exactness is not guaranteed, it can be wrapped with [`Sharpness`]
/// to express its reliability, such as in Statistics.
#[derive(Clone, PartialEq, Eq, Default)]
pub enum Sharpness<T: Debug + Clone + PartialEq + Eq + Display + PartialOrd> {
    Exact(T),
    Inexact(T),
    #[default]
    Absent,
}

impl<T: Debug + Clone + PartialEq + Eq + Display + PartialOrd> Sharpness<T> {
    /// If the information is known somehow, it returns the value. Otherwise, it returns None.
    pub fn get_value(&self) -> Option<T> {
        match self {
            Sharpness::Exact(val) | Sharpness::Inexact(val) => Some(val.clone()),
            Sharpness::Absent => None,
        }
    }

    /// Value in the [`Sharpness`] is mapped to the function result wrapped with same exactness state.
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

    /// Returns Some(true) if the information is exact, or Some(false) if not exact.
    /// If the information does not even exist, it returns None.
    pub fn is_exact(&self) -> Option<bool> {
        match self {
            Sharpness::Exact(_) => Some(true),
            Sharpness::Inexact(_) => Some(false),
            _ => None,
        }
    }

    /// Returns the greater one between two exact or inexact values.
    /// If one of them is a [`Sharpness::Absent`], it returns [`Sharpness::Absent`].
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

    /// Returns the smaller one between two exact or inexact values.
    /// If one of them is a [`Sharpness::Absent`], it returns [`Sharpness::Absent`].
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
}

impl Sharpness<usize> {
    /// Calculates the sum of two exact or inexact values in the type of [`usize`].
    /// If one of them is a [`Sharpness::Absent`], it returns [`Sharpness::Absent`].
    pub fn add(&self, other: &Sharpness<usize>) -> Sharpness<usize> {
        match (self, other) {
            (Sharpness::Exact(a), Sharpness::Exact(b)) => Sharpness::Exact(a + b),
            (Sharpness::Inexact(a), Sharpness::Exact(b))
            | (Sharpness::Exact(a), Sharpness::Inexact(b))
            | (Sharpness::Inexact(a), Sharpness::Inexact(b)) => Sharpness::Inexact(a + b),
            (_, _) => Sharpness::Absent,
        }
    }

    /// Calculates the difference of two exact or inexact values in the type of [`usize`].
    /// If one of them is a [`Sharpness::Absent`], it returns [`Sharpness::Absent`].
    pub fn sub(&self, other: &Sharpness<usize>) -> Sharpness<usize> {
        match (self, other) {
            (Sharpness::Exact(a), Sharpness::Exact(b)) => Sharpness::Exact(a - b),
            (Sharpness::Inexact(a), Sharpness::Exact(b))
            | (Sharpness::Exact(a), Sharpness::Inexact(b))
            | (Sharpness::Inexact(a), Sharpness::Inexact(b)) => Sharpness::Inexact(a - b),
            (_, _) => Sharpness::Absent,
        }
    }

    /// Calculates the multiplication of two exact or inexact values in the type of [`usize`].
    /// If one of them is a [`Sharpness::Absent`], it returns [`Sharpness::Absent`].
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

impl<T: fmt::Debug + Clone + PartialEq + Eq + Display + PartialOrd> Debug
    for Sharpness<T>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Sharpness::Exact(inner) => write!(f, "Exact:({:?})", inner),
            Sharpness::Inexact(inner) => write!(f, "Approximate:({:?})", inner),
            Sharpness::Absent => write!(f, "Absent Info"),
        }
    }
}

impl<T: fmt::Debug + Clone + PartialEq + Eq + Display + PartialOrd> Display
    for Sharpness<T>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Sharpness::Exact(inner) => write!(f, "Exact:({})", inner),
            Sharpness::Inexact(inner) => write!(f, "Approximate:({})", inner),
            Sharpness::Absent => write!(f, "Absent Info"),
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
    /// Returns a [`Statistics`] instance corresponding to the given schema by assigning infinite
    /// bounds to each column in the schema. This is useful when the input statistics are not
    /// known to give an opportunity to the current executor to shrink the bounds of some columns.
    pub fn new_with_unbounded_columns(schema: &Schema) -> Self {
        Self {
            num_rows: Sharpness::Absent,
            total_byte_size: Sharpness::Absent,
            column_statistics: Statistics::unbounded_column_statistics(schema),
        }
    }

    /// Returns an unbounded ColumnStatistics for each field in the schema.
    pub fn unbounded_column_statistics(schema: &Schema) -> Vec<ColumnStatistics> {
        schema
            .fields()
            .iter()
            .map(|field| {
                let inf = ScalarValue::try_from(field.data_type()).ok();
                ColumnStatistics {
                    null_count: Sharpness::Absent,
                    max_value: inf
                        .clone()
                        .map(Sharpness::Inexact)
                        .unwrap_or(Sharpness::Absent),
                    min_value: inf.map(Sharpness::Inexact).unwrap_or(Sharpness::Absent),
                    distinct_count: Sharpness::Absent,
                }
            })
            .collect()
    }

    /// Returns true if all the statistical parameters contain exact information.
    pub fn all_exact(&self) -> bool {
        self.num_rows.is_exact().unwrap_or(false)
            && self.total_byte_size.is_exact().unwrap_or(false)
            && self.column_statistics.iter().all(|col_stat| {
                col_stat.distinct_count.is_exact().unwrap_or(false)
                    && col_stat.max_value.is_exact().unwrap_or(false)
                    && col_stat.min_value.is_exact().unwrap_or(false)
                    && col_stat.null_count.is_exact().unwrap_or(false)
            })
    }

    /// If the exactness of a [`Statistics`] instance is lost, this function relaxes
    /// the exactness of all information by converting them [`Sharpness::Inexact`].
    pub fn make_inexact(self) -> Self {
        Statistics {
            num_rows: if let Sharpness::Exact(val) = &self.num_rows {
                Sharpness::Inexact(*val)
            } else {
                self.num_rows
            },
            total_byte_size: if let Sharpness::Exact(val) = &self.total_byte_size {
                Sharpness::Inexact(*val)
            } else {
                self.total_byte_size
            },
            column_statistics: self
                .column_statistics
                .iter()
                .map(|cs| ColumnStatistics {
                    null_count: if let Sharpness::Exact(val) = &cs.null_count {
                        Sharpness::Inexact(*val)
                    } else {
                        cs.null_count.clone()
                    },
                    max_value: if let Sharpness::Exact(val) = &cs.max_value {
                        Sharpness::Inexact(val.clone())
                    } else {
                        cs.max_value.clone()
                    },
                    min_value: if let Sharpness::Exact(val) = &cs.min_value {
                        Sharpness::Inexact(val.clone())
                    } else {
                        cs.min_value.clone()
                    },
                    distinct_count: if let Sharpness::Exact(val) = &cs.distinct_count {
                        Sharpness::Inexact(*val)
                    } else {
                        cs.distinct_count.clone()
                    },
                })
                .collect::<Vec<_>>(),
        }
    }
}

impl Display for Statistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Number of Rows={}, Number of Bytes={}, Columns Statistics={:?}",
            self.num_rows, self.total_byte_size, self.column_statistics
        )?;

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
        match (&self.min_value.get_value(), &self.max_value.get_value()) {
            // Min and max values are the same and not infinity.
            (Some(min), Some(max)) => !min.is_null() && !max.is_null() && (min == max),
            (_, _) => false,
        }
    }

    /// Returns the [`ColumnStatistics`] corresponding to the given datatype by assigning infinite bounds.
    pub fn new_with_unbounded_column(dt: &DataType) -> ColumnStatistics {
        let null = ScalarValue::try_from(dt.clone()).ok();
        ColumnStatistics {
            null_count: Sharpness::Absent,
            max_value: null
                .clone()
                .map(Sharpness::Inexact)
                .unwrap_or(Sharpness::Absent),
            min_value: null.map(Sharpness::Inexact).unwrap_or(Sharpness::Absent),
            distinct_count: Sharpness::Absent,
        }
    }
}
