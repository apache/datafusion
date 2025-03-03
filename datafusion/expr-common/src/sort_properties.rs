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

use std::ops::Neg;

use crate::interval_arithmetic::Interval;

use arrow::datatypes::DataType;
use datafusion_common::sort::AdvSortOptions;

/// To propagate [`AdvSortOptions`] across the `PhysicalExpr`, it is insufficient
/// to simply use `Option<SortOptions>`: There must be a differentiation between
/// unordered columns and literal values, since literals may not break the ordering
/// when they are used as a child of some binary expression when the other child has
/// some ordering. On the other hand, unordered columns cannot maintain ordering when
/// they take part in such operations.
///
/// Example: ((a_ordered + b_unordered) + c_ordered) expression cannot end up with
/// sorted data; however the ((a_ordered + 999) + c_ordered) expression can. Therefore,
/// we need two different variants for literals and unordered columns as literals are
/// often more ordering-friendly under most mathematical operations.
#[derive(PartialEq, Debug, Clone, Default)]
pub enum SortProperties {
    /// Use the ordinary [`AdvSortOptions`] struct to represent ordered data
    Ordered(AdvSortOptions),
    /// This alternative represents unordered data:
    #[default]
    Unordered,
    /// Singleton is used for single-valued literal numbers:
    Singleton,
}

impl SortProperties {
    pub fn add(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Self::Singleton, _) => rhs.clone(),
            (_, Self::Singleton) => self.clone(),
            (Self::Ordered(lhs), Self::Ordered(rhs))
                if lhs.ordering == rhs.ordering && lhs.descending == rhs.descending =>
            {
                Self::Ordered(
                    lhs.clone()
                        .with_nulls_first(lhs.nulls_first || rhs.nulls_first),
                )
            }
            _ => Self::Unordered,
        }
    }

    pub fn sub(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Self::Singleton, Self::Singleton) => Self::Singleton,
            (Self::Singleton, Self::Ordered(rhs)) => {
                Self::Ordered(rhs.clone().with_descending(!rhs.descending))
            }
            (_, Self::Singleton) => self.clone(),
            (Self::Ordered(lhs), Self::Ordered(rhs))
                if lhs.ordering == rhs.ordering && lhs.descending != rhs.descending =>
            {
                Self::Ordered(
                    lhs.clone()
                        .with_nulls_first(lhs.nulls_first || rhs.nulls_first),
                )
            }
            _ => Self::Unordered,
        }
    }

    pub fn gt_or_gteq(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Self::Singleton, Self::Ordered(rhs)) => {
                Self::Ordered(rhs.clone().with_descending(!rhs.descending))
            }
            (_, Self::Singleton) => self.clone(),
            (Self::Ordered(lhs), Self::Ordered(rhs))
                if lhs.ordering == rhs.ordering && lhs.descending != rhs.descending =>
            {
                self.clone()
            }
            _ => Self::Unordered,
        }
    }

    pub fn and_or(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Self::Ordered(lhs), Self::Ordered(rhs))
                if lhs.ordering == rhs.ordering && lhs.descending == rhs.descending =>
            {
                Self::Ordered(
                    lhs.clone()
                        .with_nulls_first(lhs.nulls_first || rhs.nulls_first),
                )
            }
            (Self::Ordered(opt), Self::Singleton)
            | (Self::Singleton, Self::Ordered(opt)) => Self::Ordered(opt.clone()),
            (Self::Singleton, Self::Singleton) => Self::Singleton,
            _ => Self::Unordered,
        }
    }
}

impl Neg for SortProperties {
    type Output = Self;

    fn neg(self) -> Self::Output {
        match self {
            SortProperties::Ordered(sort_definition) => {
                SortProperties::Ordered(sort_definition.with_reversed_order())
            }
            SortProperties::Unordered => self,
            SortProperties::Singleton => self,
        }
    }
}

/// Represents the properties of a `PhysicalExpr`, including its sorting,
/// range, and whether it preserves lexicographical ordering.
#[derive(Debug, Clone)]
pub struct ExprProperties {
    /// Properties that describe the sorting behavior of the expression,
    /// such as whether it is ordered, unordered, or a singleton value.
    pub sort_properties: SortProperties,
    /// A closed interval representing the range of possible values for
    /// the expression. Used to compute reliable bounds.
    pub range: Interval,
    /// Indicates whether the expression preserves lexicographical ordering
    /// of its inputs. For example, string concatenation preserves ordering,
    /// while addition does not.
    pub preserves_lex_ordering: bool,
}

impl ExprProperties {
    /// Creates a new `ExprProperties` instance with unknown sort properties,
    /// unknown range, and unknown lexicographical ordering preservation.
    pub fn new_unknown() -> Self {
        Self {
            sort_properties: SortProperties::default(),
            range: Interval::make_unbounded(&DataType::Null).unwrap(),
            preserves_lex_ordering: false,
        }
    }

    /// Sets the sorting properties of the expression and returns the modified instance.
    pub fn with_order(mut self, order: SortProperties) -> Self {
        self.sort_properties = order;
        self
    }

    /// Sets the range of the expression and returns the modified instance.
    pub fn with_range(mut self, range: Interval) -> Self {
        self.range = range;
        self
    }

    /// Sets whether the expression maintains lexicographical ordering and returns the modified instance.
    pub fn with_preserves_lex_ordering(mut self, preserves_lex_ordering: bool) -> Self {
        self.preserves_lex_ordering = preserves_lex_ordering;
        self
    }
}
