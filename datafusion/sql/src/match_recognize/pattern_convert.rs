//! Internal utility: convert sqlparser-rs AST patterns to DataFusion’s `Pattern`.
//! **Note**: This is meant for internal & test usage. Public-docs hidden.
#![doc(hidden)]

use datafusion_expr::match_recognize::{Pattern, RepetitionQuantifier, Symbol};
use sqlparser::ast::{
    MatchRecognizePattern as SqlPattern, MatchRecognizeSymbol as SqlSymbol,
    RepetitionQuantifier as SqlQuant,
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

fn conv_quant(q: &SqlQuant) -> RepetitionQuantifier {
    match q {
        SqlQuant::ZeroOrMore => RepetitionQuantifier::ZeroOrMore,
        SqlQuant::OneOrMore => RepetitionQuantifier::OneOrMore,
        SqlQuant::AtMostOne => RepetitionQuantifier::AtMostOne,
        SqlQuant::Exactly(n) => RepetitionQuantifier::Exactly(*n),
        SqlQuant::AtLeast(n) => RepetitionQuantifier::AtLeast(*n),
        SqlQuant::AtMost(n) => RepetitionQuantifier::AtMost(*n),
        SqlQuant::Range(n, m) => RepetitionQuantifier::Range(*n, *m),
    }
}
