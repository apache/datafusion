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

use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{BinaryExpr, Expr, Like, Operator};
use regex_syntax::hir::{Hir, HirKind, Literal};

/// Maximum number of regex alternations (`foo|bar|...`) that will be expanded into multiple `LIKE` expressions.
const MAX_REGEX_ALTERNATIONS_EXPANSION: usize = 4;

pub fn simplify_regex_expr(
    left: Box<Expr>,
    op: Operator,
    right: Box<Expr>,
) -> Result<Expr, DataFusionError> {
    let mode = OperatorMode::new(&op);

    if let Expr::Literal(ScalarValue::Utf8(Some(pattern))) = right.as_ref() {
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

    // leave untouched if optimization didn't work
    Ok(Expr::BinaryExpr(BinaryExpr { left, op, right }))
}

struct OperatorMode {
    not: bool,
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

    fn expr(&self, expr: Box<Expr>, pattern: String) -> Expr {
        let like = Like {
            negated: self.not,
            expr,
            pattern: Box::new(Expr::Literal(ScalarValue::Utf8(Some(pattern)))),
            escape_char: None,
        };

        if self.i {
            Expr::ILike(like)
        } else {
            Expr::Like(like)
        }
    }
}

fn collect_concat_to_like_string(parts: &[Hir]) -> Option<String> {
    let mut s = String::with_capacity(parts.len() + 2);
    s.push('%');

    for sub in parts {
        if let HirKind::Literal(Literal::Unicode(c)) = sub.kind() {
            if !is_safe_for_like(*c) {
                return None;
            }
            s.push(*c);
        } else {
            return None;
        }
    }

    s.push('%');
    Some(s)
}

fn is_safe_for_like(c: char) -> bool {
    (c != '%') && (c != '_')
}

fn lower_simple(mode: &OperatorMode, left: &Expr, hir: &Hir) -> Option<Expr> {
    match hir.kind() {
        HirKind::Empty => {
            return Some(mode.expr(Box::new(left.clone()), "%".to_owned()));
        }
        HirKind::Literal(Literal::Unicode(c)) if is_safe_for_like(*c) => {
            return Some(mode.expr(Box::new(left.clone()), format!("%{c}%")));
        }
        HirKind::Concat(inner) => {
            if let Some(pattern) = collect_concat_to_like_string(inner) {
                return Some(mode.expr(Box::new(left.clone()), pattern));
            }
        }
        _ => {}
    }

    None
}

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
