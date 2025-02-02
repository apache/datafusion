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

use std::cmp::{self, Ordering};
use std::fmt;
use std::hash::{Hash, Hasher};

/// Represents a location, determined by a line and a column number, in the
/// original SQL query.
#[derive(Eq, PartialEq, Hash, Clone, Copy, Ord, PartialOrd)]
pub struct Location {
    /// Line number, starting from 1.
    ///
    /// Note: Line 0 is used for empty spans
    pub line: u64,
    /// Line column, starting from 1.
    ///
    /// Note: Column 0 is used for empty spans
    pub column: u64,
}

impl fmt::Debug for Location {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Location({},{})", self.line, self.column)
    }
}

impl From<sqlparser::tokenizer::Location> for Location {
    fn from(value: sqlparser::tokenizer::Location) -> Self {
        Self {
            line: value.line,
            column: value.column,
        }
    }
}

/// Represents an interval of characters in the original SQL query.
#[derive(Eq, PartialEq, Hash, Clone, PartialOrd, Ord, Copy)]
pub struct Span {
    pub start: Location,
    pub end: Location,
}

impl fmt::Debug for Span {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Span({:?}..{:?})", self.start, self.end)
    }
}

impl Span {
    /// Creates a new [`Span`] from a start and an end [`Location`].
    pub fn new(start: Location, end: Location) -> Self {
        Self { start, end }
    }

    /// Convert a [`Span`](sqlparser::tokenizer::Span) from the parser, into a
    /// DataFusion [`Span`]. If the input span is empty (line 0 column 0, to
    /// line 0 column 0), then [`None`] is returned.
    pub fn try_from_sqlparser_span(span: sqlparser::tokenizer::Span) -> Option<Span> {
        if span == sqlparser::tokenizer::Span::empty() {
            None
        } else {
            Some(Span {
                start: span.start.into(),
                end: span.end.into(),
            })
        }
    }

    /// Returns the smallest Span that contains both `self` and `other`
    ///
    /// # Examples
    /// ```
    /// # use sqlparser::tokenizer::{Span, Location};
    /// // line 1, column1 -> line 2, column 5
    /// let span1 = Span::new(Location::new(1, 1), Location::new(2, 5));
    /// // line 2, column 3 -> line 3, column 7
    /// let span2 = Span::new(Location::new(2, 3), Location::new(3, 7));
    /// // Union of the two is the min/max of the two spans
    /// // line 1, column 1 -> line 3, column 7
    /// let union = span1.union(&span2);
    /// assert_eq!(union, Span::new(Location::new(1, 1), Location::new(3, 7)));
    /// ```
    pub fn union(&self, other: &Span) -> Span {
        Span {
            start: cmp::min(self.start, other.start),
            end: cmp::max(self.end, other.end),
        }
    }

    /// Same as [Span::union] for `Option<Span>`.
    ///
    /// If `other` is `None`, `self` is returned.
    pub fn union_opt(&self, other: &Option<Span>) -> Span {
        match other {
            Some(other) => self.union(other),
            None => *self,
        }
    }

    /// Return the [Span::union] of all spans in the iterator.
    ///
    /// If the iterator is empty, [`None`] is returned.
    ///
    /// # Example
    /// ```
    /// # use sqlparser::tokenizer::{Span, Location};
    /// let spans = vec![
    ///     Span::new(Location::new(1, 1), Location::new(2, 5)),
    ///     Span::new(Location::new(2, 3), Location::new(3, 7)),
    ///     Span::new(Location::new(3, 1), Location::new(4, 2)),
    /// ];
    /// // line 1, column 1 -> line 4, column 2
    /// assert_eq!(
    ///   Span::union_iter(spans),
    ///   Span::new(Location::new(1, 1), Location::new(4, 2))
    /// );
    pub fn union_iter<I: IntoIterator<Item = Span>>(iter: I) -> Option<Span> {
        iter.into_iter().reduce(|acc, item| acc.union(&item))
    }
}

/// A collection of [`Span`], meant to be used as a field of entities whose
/// location in the original SQL query is desired to be tracked. Sometimes an
/// entity can have multiple spans. e.g. if you want to track the position of
/// the column a that comes from SELECT 1 AS a UNION ALL SELECT 2 AS a you'll
/// need two spans.
#[derive(Debug, Clone)]
// Store teh first [`Span`] on the stack because that is by far the most common
// case. More will spill onto the heap.
pub struct Spans(pub Vec<Span>);

impl Spans {
    /// Creates a new empty [`Spans`] with no [`Span`].
    pub fn new() -> Self {
        Spans(Vec::new())
    }

    /// Returns the first [`Span`], if any. This is useful when you know that
    /// there's gonna be only one [`Span`] at most.
    pub fn first(&self) -> Option<Span> {
        self.0.first().copied()
    }

    /// Returns a slice of the [`Span`]s.
    pub fn get_spans(&self) -> &[Span] {
        &self.0
    }

    /// Adds a [`Span`] to the collection.
    pub fn add_span(&mut self, span: Span) {
        self.0.push(span);
    }

    /// Iterates over the [`Span`]s.
    pub fn iter(&self) -> impl Iterator<Item = &Span> {
        self.0.iter()
    }
}

impl Default for Spans {
    fn default() -> Self {
        Self::new()
    }
}

// Since [`Spans`] will be used as a field in other structs, we don't want it to
// interfere with the equality and ordering of the entities themselves, since
// this is just diagnostics information for the end user.
impl PartialEq for Spans {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

// Since [`Spans`] will be used as a field in other structs, we don't want it to
// interfere with the equality and ordering of the entities themselves, since
// this is just diagnostics information for the end user.
impl Eq for Spans {}

// Since [`Spans`] will be used as a field in other structs, we don't want it to
// interfere with the equality and ordering of the entities themselves, since
// this is just diagnostics information for the end user.
impl PartialOrd for Spans {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Since [`Spans`] will be used as a field in other structs, we don't want it to
// interfere with the equality and ordering of the entities themselves, since
// this is just diagnostics information for the end user.
impl Ord for Spans {
    fn cmp(&self, _other: &Self) -> Ordering {
        Ordering::Equal
    }
}

// Since [`Spans`] will be used as a field in other structs, we don't want it to
// interfere with the equality and ordering of the entities themselves, since
// this is just diagnostics information for the end user.
impl Hash for Spans {
    fn hash<H: Hasher>(&self, _state: &mut H) {}
}
