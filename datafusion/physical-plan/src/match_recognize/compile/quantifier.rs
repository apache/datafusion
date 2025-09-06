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

//! Utilities for handling repetition quantifiers within MATCH_RECOGNIZE patterns.

use super::builder::NfaBuilder;
use datafusion_common::Result;
use datafusion_expr::match_recognize::Pattern;
use datafusion_expr::match_recognize::RepetitionQuantifier;

/// Parameters extracted from a `RepetitionQuantifier` for NFA construction.
#[derive(Debug, Clone)]
pub(crate) struct QuantifierParams {
    /// Minimum number of repetitions required
    pub(crate) min: usize,
    /// Maximum number of repetitions allowed (None = unbounded)
    pub(crate) max: Option<usize>,
    /// Whether this quantifier supports looping (for +, *, {m,})
    pub(crate) looping: bool,
}

impl QuantifierParams {
    /// Extract parameters from a `RepetitionQuantifier`.
    pub(crate) fn from_quantifier(quant: &RepetitionQuantifier) -> Self {
        use datafusion_expr::match_recognize::RepetitionQuantifier::*;

        match quant {
            OneOrMore => Self {
                min: 1,
                max: None,
                looping: true,
            },
            ZeroOrMore => Self {
                min: 0,
                max: None,
                looping: true,
            },
            AtMostOne => Self {
                min: 0,
                max: Some(1),
                looping: false,
            },
            Exactly(n) => Self {
                min: *n as usize,
                max: Some(*n as usize),
                looping: false,
            },
            AtLeast(n) => Self {
                min: *n as usize,
                max: None,
                looping: true,
            },
            AtMost(n) => Self {
                min: 0,
                max: Some(*n as usize),
                looping: false,
            },
            Range(a, b) => Self {
                min: *a as usize,
                max: Some(*b as usize),
                looping: false,
            },
        }
    }
}

/// Compile a quantified sub-pattern.
/// This free function takes a mutable reference to the arena‐owning
/// `NfaBuilder` and emits the NFA fragment that corresponds to a repetition
/// (`*`, `+`, `{m,n}` …) of `inner`.
pub(crate) fn compile_quantifier(
    b: &mut NfaBuilder,
    inner: &Pattern,
    quant: &RepetitionQuantifier,
) -> Result<(usize, usize)> {
    let params = QuantifierParams::from_quantifier(quant);

    // Degenerate {0} pattern – single empty state.
    if params.min == 0 && params.max == Some(0) {
        let s = b.new_state(false);
        return Ok((s, s));
    }

    // 1. Optional dummy entry node when min == 0 so we always have a stable start.
    let entry = if params.min == 0 {
        Some(b.new_state(false))
    } else {
        None
    };

    let mut first_start: usize = entry.unwrap_or(usize::MAX);
    let mut last_end: usize = entry.unwrap_or(usize::MAX);
    let mut last_copy_start: usize = usize::MAX; // for looping

    // 2. Mandatory copies.
    for i in 0..params.min {
        let (s, e) = b.build_pattern(inner)?;
        if i == 0 {
            first_start = if let Some(entry) = entry { entry } else { s };
        }
        if last_end != usize::MAX {
            b.add_epsilon(last_end, s);
        }
        last_end = e;
        last_copy_start = s;
    }

    // 3. Optional bounded copies up to max.
    if let Some(max_bound) = params.max {
        let mut remaining = max_bound.saturating_sub(params.min);
        while remaining > 0 {
            let (s, e) = b.build_pattern(inner)?;
            b.add_epsilon(last_end, s);
            last_end = e;
            remaining -= 1;
        }
        if params.min == 0 {
            b.add_epsilon(first_start, last_end);
        }
    }

    // 4. Unbounded loop for + / * / {m,}
    if params.max.is_none() && params.looping {
        if last_copy_start == usize::MAX {
            // ZeroOrMore without prior mandatory copy
            let (s, e) = b.build_pattern(inner)?;
            b.add_epsilon(first_start, s);
            last_copy_start = s;
            last_end = e;
        }
        b.add_epsilon(last_end, last_copy_start);
        if params.min == 0 {
            b.add_epsilon(first_start, last_end);
        }
    }

    Ok((first_start, last_end))
}
