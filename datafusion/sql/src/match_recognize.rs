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

//! Internal utility: convert sqlparser-rs AST patterns to DataFusion’s `Pattern`.
//! **Note**: This is meant for internal & test usage. Public-docs hidden.
#![doc(hidden)]

use datafusion_expr::match_recognize::{Pattern, RepetitionQuantifier, Symbol};
use sqlparser::ast::{
    MatchRecognizePattern, MatchRecognizeSymbol,
    RepetitionQuantifier as SqlRepetitionQuantifier,
};
use sqlparser::ast::{
    MatchRecognizePattern as SqlPattern, MatchRecognizeSymbol as SqlSymbol,
    RepetitionQuantifier as SqlQuantifier,
};

/// Convert an sqlparser-rs `MatchRecognizePattern` AST node into DataFusion's
/// internal [`Pattern`] representation.
pub fn sql_pattern_to_df(p: &SqlPattern) -> Pattern {
    match p {
        SqlPattern::Symbol(s) => Pattern::Symbol(conv_sym(s)),
        SqlPattern::Exclude(s) => Pattern::Exclude(conv_sym(s)),
        SqlPattern::Permute(list) => {
            Pattern::Permute(list.iter().map(conv_sym).collect())
        }
        SqlPattern::Concat(parts) => {
            Pattern::Concat(parts.iter().map(sql_pattern_to_df).collect())
        }
        SqlPattern::Group(inner) => Pattern::Group(Box::new(sql_pattern_to_df(inner))),
        SqlPattern::Alternation(parts) => {
            Pattern::Alternation(parts.iter().map(sql_pattern_to_df).collect())
        }
        SqlPattern::Repetition(inner, q) => {
            Pattern::Repetition(Box::new(sql_pattern_to_df(inner)), conv_quant(q))
        }
    }
}

fn conv_sym(s: &SqlSymbol) -> Symbol {
    match s {
        SqlSymbol::Named(ident) => Symbol::Named(ident.value.clone()),
        SqlSymbol::Start => Symbol::Start,
        SqlSymbol::End => Symbol::End,
    }
}

fn conv_quant(q: &SqlQuantifier) -> RepetitionQuantifier {
    match q {
        SqlQuantifier::ZeroOrMore => RepetitionQuantifier::ZeroOrMore,
        SqlQuantifier::OneOrMore => RepetitionQuantifier::OneOrMore,
        SqlQuantifier::AtMostOne => RepetitionQuantifier::AtMostOne,
        SqlQuantifier::Exactly(n) => RepetitionQuantifier::Exactly(*n),
        SqlQuantifier::AtLeast(n) => RepetitionQuantifier::AtLeast(*n),
        SqlQuantifier::AtMost(n) => RepetitionQuantifier::AtMost(*n),
        SqlQuantifier::Range(n, m) => RepetitionQuantifier::Range(*n, *m),
    }
}

/// Convert a DataFusion `Pattern` back to a `sqlparser` `MatchRecognizePattern`.
pub fn df_pattern_to_sql(p: &Pattern) -> MatchRecognizePattern {
    match p {
        Pattern::Symbol(sym) => MatchRecognizePattern::Symbol(df_sym_to_sql(sym)),
        Pattern::Exclude(sym) => MatchRecognizePattern::Exclude(df_sym_to_sql(sym)),
        Pattern::Permute(list) => {
            MatchRecognizePattern::Permute(list.iter().map(df_sym_to_sql).collect())
        }
        Pattern::Concat(parts) => {
            MatchRecognizePattern::Concat(parts.iter().map(df_pattern_to_sql).collect())
        }
        Pattern::Group(inner) => {
            MatchRecognizePattern::Group(Box::new(df_pattern_to_sql(inner)))
        }
        Pattern::Alternation(parts) => MatchRecognizePattern::Alternation(
            parts.iter().map(df_pattern_to_sql).collect(),
        ),
        Pattern::Repetition(inner, q) => MatchRecognizePattern::Repetition(
            Box::new(df_pattern_to_sql(inner)),
            df_quant_to_sql(q),
        ),
    }
}

fn df_sym_to_sql(s: &Symbol) -> MatchRecognizeSymbol {
    match s {
        Symbol::Named(name) => {
            MatchRecognizeSymbol::Named(sqlparser::ast::Ident::new(name))
        }
        Symbol::Start => MatchRecognizeSymbol::Start,
        Symbol::End => MatchRecognizeSymbol::End,
    }
}

fn df_quant_to_sql(q: &RepetitionQuantifier) -> SqlRepetitionQuantifier {
    match q {
        RepetitionQuantifier::ZeroOrMore => SqlRepetitionQuantifier::ZeroOrMore,
        RepetitionQuantifier::OneOrMore => SqlRepetitionQuantifier::OneOrMore,
        RepetitionQuantifier::AtMostOne => SqlRepetitionQuantifier::AtMostOne,
        RepetitionQuantifier::Exactly(n) => SqlRepetitionQuantifier::Exactly(*n),
        RepetitionQuantifier::AtLeast(n) => SqlRepetitionQuantifier::AtLeast(*n),
        RepetitionQuantifier::AtMost(n) => SqlRepetitionQuantifier::AtMost(*n),
        RepetitionQuantifier::Range(n, m) => SqlRepetitionQuantifier::Range(*n, *m),
    }
}
