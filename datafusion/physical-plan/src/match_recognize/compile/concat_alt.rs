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

//! Helpers for compiling concatenation (`...`) and alternation (`|`) patterns
//! into fragments of the NFA under construction.

use datafusion_common::Result;
use datafusion_expr::match_recognize::Pattern;

use super::builder::NfaBuilder;

/// Compile a concatenation pattern into the given builder.
pub(crate) fn compile_concat(
    b: &mut NfaBuilder,
    parts: &[Pattern],
) -> Result<(usize, usize)> {
    if parts.is_empty() {
        return Err(datafusion_common::DataFusionError::Internal(
            "Concatenation must contain at least one sub-pattern".into(),
        ));
    }

    let mut iter = parts.iter();
    let (start, mut end) = b.build_pattern(iter.next().unwrap())?;
    for p in iter {
        let (s, e) = b.build_pattern(p)?;
        b.add_epsilon(end, s);
        end = e;
    }
    Ok((start, end))
}

/// Compile an alternation pattern (`A | B | ...`) into the builder.
pub(crate) fn compile_alternation(
    b: &mut NfaBuilder,
    alts: &[Pattern],
) -> Result<(usize, usize)> {
    if alts.is_empty() {
        return Err(datafusion_common::DataFusionError::Internal(
            "Alternation must contain at least one branch".into(),
        ));
    }

    let start = b.new_state(false);
    let mut ends = Vec::new();
    for (idx, sub) in alts.iter().enumerate() {
        let (s, e) = b.build_pattern(sub)?;
        b.add_epsilon(start, s);
        ends.push(e);

        // Tag the terminal state of this branch so that the matcher can
        // enforce left-most precedence later on.
        b.mark_alt_branch(e, idx as u32);
    }

    let end = b.new_state(false);
    for e in ends {
        b.add_epsilon(e, end);
    }
    Ok((start, end))
}
