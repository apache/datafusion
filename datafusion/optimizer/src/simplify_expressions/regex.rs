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

use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{lit, BinaryExpr, Expr, Like, Operator};
use regex_syntax::hir::{Capture, Hir, HirKind, Literal, Look};

/// Maximum number of regex alternations (`foo|bar|...`) that will be expanded into multiple `LIKE` expressions.
const MAX_REGEX_ALTERNATIONS_EXPANSION: usize = 4;

const ANY_CHAR_REGEX_PATTERN: &str = ".*";

/// Tries to convert a regexp expression to a `LIKE` or `Eq`/`NotEq` expression.
///
/// This function also validates the regex pattern. And will return error if the
/// pattern is invalid.
///
/// Typical cases this function can simplify:
/// - empty regex pattern to `LIKE '%'`
/// - literal regex patterns to `LIKE '%foo%'`
/// - full anchored regex patterns (e.g. `^foo$`) to `= 'foo'`
/// - partial anchored regex patterns (e.g. `^foo`) to `LIKE 'foo%'`
/// - combinations (alternatives) of the above, will be concatenated with `OR` or `AND`
/// - `EQ .*` to NotNull
/// - `NE .*` means IS EMPTY
///
/// Dev note: unit tests of this function are in `expr_simplifier.rs`, case `test_simplify_regex`.
pub fn simplify_regex_expr(
    left: Box<Expr>,
    op: Operator,
    right: Box<Expr>,
) -> Result<Expr> {
    let mode = OperatorMode::new(&op);

    if let Expr::Literal(ScalarValue::Utf8(Some(pattern))) = right.as_ref() {
        // Handle the special case for ".*" pattern
        if pattern == ANY_CHAR_REGEX_PATTERN {
            let new_expr = if mode.not {
                // not empty
                let empty_lit = Box::new(lit(""));
                Expr::BinaryExpr(BinaryExpr {
                    left,
                    op: Operator::Eq,
                    right: empty_lit,
                })
            } else {
                // not null
                left.is_not_null()
            };
            return Ok(new_expr);
        }

        match regex_syntax::Parser::new().parse(pattern) {
            Ok(hir) => {
                let kind = hir.kind();
                if let HirKind::Alternation(alts) = kind {
                    if alts.len() <= MAX_REGEX_ALTERNATIONS_EXPANSION {
                        if let Some(expr) = lower_alt(&mode, &left, alts) {
                            return Ok(expr);
                        }
                    }
                } else if let Some(expr) = lower_simple(&mode, &left, &hir) {
                    return Ok(expr);
                }
            }
            Err(e) => {
                // error out early since the execution may fail anyways
                return Err(DataFusionError::Context(
                    "Invalid regex".to_owned(),
                    Box::new(DataFusionError::External(Box::new(e))),
                ));
            }
        }
    }

    // Leave untouched if optimization didn't work
    Ok(Expr::BinaryExpr(BinaryExpr { left, op, right }))
}

#[derive(Debug)]
struct OperatorMode {
    /// Negative match.
    not: bool,
    /// Ignore case (`true` for case-insensitive).
    i: bool,
}

impl OperatorMode {
    fn new(op: &Operator) -> Self {
        let not = match op {
            Operator::RegexMatch | Operator::RegexIMatch => false,
            Operator::RegexNotMatch | Operator::RegexNotIMatch => true,
            _ => unreachable!(),
        };

        let i = match op {
            Operator::RegexMatch | Operator::RegexNotMatch => false,
            Operator::RegexIMatch | Operator::RegexNotIMatch => true,
            _ => unreachable!(),
        };

        Self { not, i }
    }

    /// Creates an [`LIKE`](Expr::Like) from the given `LIKE` pattern.
    fn expr(&self, expr: Box<Expr>, pattern: String) -> Expr {
        let like = Like {
            negated: self.not,
            expr,
            pattern: Box::new(Expr::Literal(ScalarValue::from(pattern))),
            escape_char: None,
            case_insensitive: self.i,
        };

        Expr::Like(like)
    }

    /// Creates an [`Expr::BinaryExpr`] of "`left` = `right`" or "`left` != `right`".
    fn expr_matches_literal(&self, left: Box<Expr>, right: Box<Expr>) -> Expr {
        let op = if self.not {
            Operator::NotEq
        } else {
            Operator::Eq
        };
        Expr::BinaryExpr(BinaryExpr { left, op, right })
    }
}

fn collect_concat_to_like_string(parts: &[Hir]) -> Option<String> {
    let mut s = String::with_capacity(parts.len() + 2);
    s.push('%');

    for sub in parts {
        if let HirKind::Literal(l) = sub.kind() {
            s.push_str(like_str_from_literal(l)?);
        } else {
            return None;
        }
    }

    s.push('%');
    Some(s)
}

/// Returns a str represented by `Literal` if it contains a valid utf8
/// sequence and is safe for like (has no '%' and '_')
fn like_str_from_literal(l: &Literal) -> Option<&str> {
    // if not utf8, no good
    let s = std::str::from_utf8(&l.0).ok()?;

    if s.chars().all(is_safe_for_like) {
        Some(s)
    } else {
        None
    }
}

/// Returns a str represented by `Literal` if it contains a valid utf8
fn str_from_literal(l: &Literal) -> Option<&str> {
    // if not utf8, no good
    let s = std::str::from_utf8(&l.0).ok()?;

    Some(s)
}

fn is_safe_for_like(c: char) -> bool {
    (c != '%') && (c != '_')
}

/// Returns true if the elements in a `Concat` pattern are:
/// - `[Look::Start, Look::End]`
/// - `[Look::Start, Literal(_), Look::End]`
fn is_anchored_literal(v: &[Hir]) -> bool {
    match v.len() {
        2..=3 => (),
        _ => return false,
    };

    let first_last = (
        v.first().expect("length checked"),
        v.last().expect("length checked"),
    );
    if !matches!(first_last,
        (s, e) if s.kind() == &HirKind::Look(Look::Start)
        && e.kind() == &HirKind::Look(Look::End)
    ) {
        return false;
    }

    v.iter()
        .skip(1)
        .take(v.len() - 2)
        .all(|h| matches!(h.kind(), HirKind::Literal(_)))
}

/// Returns true if the elements in a `Concat` pattern are:
/// - `[Look::Start, Capture(Alternation(Literals...)), Look::End]`
fn is_anchored_capture(v: &[Hir]) -> bool {
    if v.len() != 3
        || !matches!(
            (v.first().unwrap().kind(), v.last().unwrap().kind()),
            (&HirKind::Look(Look::Start), &HirKind::Look(Look::End))
        )
    {
        return false;
    }

    if let HirKind::Capture(cap, ..) = v[1].kind() {
        let Capture { sub, .. } = cap;
        if let HirKind::Alternation(alters) = sub.kind() {
            let has_non_literal = alters
                .iter()
                .any(|v| !matches!(v.kind(), &HirKind::Literal(_)));
            if has_non_literal {
                return false;
            }
        }
    }

    true
}

/// Returns the `LIKE` pattern if the `Concat` pattern is partial anchored:
/// - `[Look::Start, Literal(_)]`
/// - `[Literal(_), Look::End]`
///
/// Full anchored patterns are handled by [`anchored_literal_to_expr`].
fn partial_anchored_literal_to_like(v: &[Hir]) -> Option<String> {
    if v.len() != 2 {
        return None;
    }

    let (lit, match_begin) = match (&v[0].kind(), &v[1].kind()) {
        (HirKind::Look(Look::Start), HirKind::Literal(l)) => {
            (like_str_from_literal(l)?, true)
        }
        (HirKind::Literal(l), HirKind::Look(Look::End)) => {
            (like_str_from_literal(l)?, false)
        }
        _ => return None,
    };

    if match_begin {
        Some(format!("{}%", lit))
    } else {
        Some(format!("%{}", lit))
    }
}

/// Extracts a string literal expression assuming that [`is_anchored_literal`]
/// returned true.
fn anchored_literal_to_expr(v: &[Hir]) -> Option<Expr> {
    match v.len() {
        2 => Some(lit("")),
        3 => {
            let HirKind::Literal(l) = v[1].kind() else {
                return None;
            };
            like_str_from_literal(l).map(lit)
        }
        _ => None,
    }
}

fn anchored_alternation_to_exprs(v: &[Hir]) -> Option<Vec<Expr>> {
    if 3 != v.len() {
        return None;
    }

    if let HirKind::Capture(cap, ..) = v[1].kind() {
        let Capture { sub, .. } = cap;
        if let HirKind::Alternation(alters) = sub.kind() {
            let mut literals = Vec::with_capacity(alters.len());
            for hir in alters {
                let mut is_safe = false;
                if let HirKind::Literal(l) = hir.kind() {
                    if let Some(safe_literal) = str_from_literal(l).map(lit) {
                        literals.push(safe_literal);
                        is_safe = true;
                    }
                }

                if !is_safe {
                    return None;
                }
            }

            return Some(literals);
        } else if let HirKind::Literal(l) = sub.kind() {
            if let Some(safe_literal) = str_from_literal(l).map(lit) {
                return Some(vec![safe_literal]);
            }
            return None;
        }
    }
    None
}

/// Tries to lower (transform) a simple regex pattern to a LIKE expression.
fn lower_simple(mode: &OperatorMode, left: &Expr, hir: &Hir) -> Option<Expr> {
    match hir.kind() {
        HirKind::Empty => {
            return Some(mode.expr(Box::new(left.clone()), "%".to_owned()));
        }
        HirKind::Literal(l) => {
            let s = like_str_from_literal(l)?;
            return Some(mode.expr(Box::new(left.clone()), format!("%{s}%")));
        }
        HirKind::Concat(inner) if is_anchored_literal(inner) => {
            return anchored_literal_to_expr(inner).map(|right| {
                mode.expr_matches_literal(Box::new(left.clone()), Box::new(right))
            });
        }
        HirKind::Concat(inner) if is_anchored_capture(inner) => {
            return anchored_alternation_to_exprs(inner)
                .map(|right| left.clone().in_list(right, mode.not));
        }
        HirKind::Concat(inner) => {
            if let Some(pattern) = partial_anchored_literal_to_like(inner)
                .or(collect_concat_to_like_string(inner))
            {
                return Some(mode.expr(Box::new(left.clone()), pattern));
            }
        }
        _ => {}
    }
    None
}

/// Calls [`lower_simple`] for each alternative and combine the results with `or` or `and`
/// based on [`OperatorMode`]. Any fail attempt to lower an alternative will makes this
/// function to return `None`.
fn lower_alt(mode: &OperatorMode, left: &Expr, alts: &[Hir]) -> Option<Expr> {
    let mut accu: Option<Expr> = None;

    for part in alts {
        if let Some(expr) = lower_simple(mode, left, part) {
            accu = match accu {
                Some(accu) => {
                    if mode.not {
                        Some(accu.and(expr))
                    } else {
                        Some(accu.or(expr))
                    }
                }
                None => Some(expr),
            };
        } else {
            return None;
        }
    }

    Some(accu.expect("at least two alts"))
}
