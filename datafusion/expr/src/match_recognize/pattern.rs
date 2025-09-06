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

use std::fmt::{self, Display, Formatter};

/// Specification for how many rows to return per match
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
pub enum RowsPerMatch {
    /// Return one row per match
    OneRow,
    /// Return all rows per match with specified empty matches mode
    AllRows(EmptyMatchesMode),
}

/// How to handle empty matches
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
pub enum EmptyMatchesMode {
    /// Show empty matches
    Show,
    /// Omit empty matches
    Omit,
    /// Include unmatched rows
    WithUnmatched,
}

/// Specification for where to continue after a match
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
pub enum AfterMatchSkip {
    /// Skip past the last row of the match
    PastLastRow,
    /// Skip to the next row after the match start
    ToNextRow,
    /// Skip to the first occurrence of the specified symbol
    ToFirst(String),
    /// Skip to the last occurrence of the specified symbol
    ToLast(String),
}

/// A pattern for matching in MATCH_RECOGNIZE
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
pub enum Pattern {
    /// A single symbol
    Symbol(Symbol),
    /// An excluded symbol
    Exclude(Symbol),
    /// A permutation of symbols
    Permute(Vec<Symbol>),
    /// A concatenation of patterns
    Concat(Vec<Pattern>),
    /// A grouped pattern
    Group(Box<Pattern>),
    /// An alternation of patterns
    Alternation(Vec<Pattern>),
    /// A pattern with repetition
    Repetition(Box<Pattern>, RepetitionQuantifier),
}

/// A symbol in a MATCH_RECOGNIZE pattern
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
pub enum Symbol {
    /// A named symbol
    Named(String),
    /// Start of partition symbol
    Start,
    /// End of partition symbol
    End,
}

/// Quantifier for pattern repetition
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
pub enum RepetitionQuantifier {
    /// Zero or more (*)
    ZeroOrMore,
    /// One or more (+)
    OneOrMore,
    /// At most one (?)
    AtMostOne,
    /// Exactly n occurrences
    Exactly(u32),
    /// At least n occurrences
    AtLeast(u32),
    /// At most n occurrences
    AtMost(u32),
    /// Between n and m occurrences
    Range(u32, u32),
}

// Display implementations
impl Display for RowsPerMatch {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            RowsPerMatch::OneRow => write!(f, "ONE ROW PER MATCH"),
            RowsPerMatch::AllRows(mode) => write!(f, "ALL ROWS PER MATCH {mode}"),
        }
    }
}

impl Display for EmptyMatchesMode {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            EmptyMatchesMode::Show => write!(f, "SHOW EMPTY MATCHES"),
            EmptyMatchesMode::Omit => write!(f, "OMIT EMPTY MATCHES"),
            EmptyMatchesMode::WithUnmatched => write!(f, "WITH UNMATCHED ROWS"),
        }
    }
}

impl Display for AfterMatchSkip {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            AfterMatchSkip::PastLastRow => write!(f, "PAST LAST ROW"),
            AfterMatchSkip::ToNextRow => write!(f, "TO NEXT ROW"),
            AfterMatchSkip::ToFirst(symbol) => write!(f, "TO FIRST {symbol}"),
            AfterMatchSkip::ToLast(symbol) => write!(f, "TO LAST {symbol}"),
        }
    }
}

impl Display for Symbol {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Symbol::Named(name) => write!(f, "{name}"),
            Symbol::Start => write!(f, "^"),
            Symbol::End => write!(f, "$"),
        }
    }
}

impl Display for RepetitionQuantifier {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            RepetitionQuantifier::ZeroOrMore => write!(f, "*"),
            RepetitionQuantifier::OneOrMore => write!(f, "+"),
            RepetitionQuantifier::AtMostOne => write!(f, "?"),
            RepetitionQuantifier::Exactly(n) => write!(f, "{{{n}}}"),
            RepetitionQuantifier::AtLeast(n) => write!(f, "{{{n},}}"),
            RepetitionQuantifier::AtMost(n) => write!(f, "{{,{n}}}"),
            RepetitionQuantifier::Range(min, max) => write!(f, "{{{min},{max}}}"),
        }
    }
}

impl Display for Pattern {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Pattern::Symbol(symbol) => write!(f, "{symbol}"),
            Pattern::Exclude(symbol) => write!(f, "{{- {symbol} -}}"),
            Pattern::Permute(symbols) => {
                let symbols_str = symbols
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                write!(f, "PERMUTE({symbols_str})")
            }
            Pattern::Concat(patterns) => {
                let patterns_str = patterns
                    .iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                write!(f, "{patterns_str}")
            }
            Pattern::Group(pattern) => write!(f, "({pattern})"),
            Pattern::Alternation(patterns) => {
                let patterns_str = patterns
                    .iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
                    .join("|");
                write!(f, "{patterns_str}")
            }
            Pattern::Repetition(pattern, quantifier) => {
                write!(f, "{pattern}{quantifier}")
            }
        }
    }
}
