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

//! Scalar-level aggregation utilities for statistics merging.
//!
//! Provides in-place accumulation helpers that reuse the existing
//! [`ScalarValue`] accumulator when possible.

use crate::stats::Precision;
use crate::{Result, ScalarValue};

/// Adds `rhs` into `lhs`, mutating the accumulator in place when
/// possible and otherwise falling back to `ScalarValue::add_checked`.
pub(crate) fn scalar_add(lhs: &mut ScalarValue, rhs: &ScalarValue) -> Result<()> {
    if lhs.try_add_checked_in_place(rhs)? {
        return Ok(());
    }

    *lhs = lhs.add_checked(rhs)?;
    Ok(())
}

/// [`Precision`]-aware sum that mutates `lhs` in place when possible.
///
/// Mirrors the semantics of `Precision<ScalarValue>::add`, including
/// checked overflow handling, but avoids allocating a fresh
/// [`ScalarValue`] for the common numeric fast path.
pub(crate) fn precision_add(
    lhs: &mut Precision<ScalarValue>,
    rhs: &Precision<ScalarValue>,
) {
    let (mut lhs_value, lhs_is_exact) = match std::mem::take(lhs) {
        Precision::Exact(value) => (value, true),
        Precision::Inexact(value) => (value, false),
        Precision::Absent => return,
    };

    let (rhs_value, rhs_is_exact) = match rhs {
        Precision::Exact(value) => (value, true),
        Precision::Inexact(value) => (value, false),
        Precision::Absent => return,
    };

    if scalar_add(&mut lhs_value, rhs_value).is_err() {
        return;
    }

    *lhs = if lhs_is_exact && rhs_is_exact {
        Precision::Exact(lhs_value)
    } else {
        Precision::Inexact(lhs_value)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_add_null_propagates() -> Result<()> {
        let mut lhs = ScalarValue::Int32(Some(42));

        scalar_add(&mut lhs, &ScalarValue::Int32(None))?;

        assert_eq!(lhs, ScalarValue::Int32(None));
        Ok(())
    }

    #[test]
    fn test_scalar_add_overflow_returns_error() {
        let mut lhs = ScalarValue::Int32(Some(i32::MAX));

        let err = scalar_add(&mut lhs, &ScalarValue::Int32(Some(1)))
            .unwrap_err()
            .strip_backtrace();

        assert_eq!(
            err,
            "Arrow error: Arithmetic overflow: Overflow happened on: 2147483647 + 1"
        );
    }

    #[test]
    fn test_precision_add_null_propagates() {
        let mut lhs = Precision::Exact(ScalarValue::Int32(Some(42)));

        precision_add(&mut lhs, &Precision::Exact(ScalarValue::Int32(None)));

        assert_eq!(lhs, Precision::Exact(ScalarValue::Int32(None)));
    }

    #[test]
    fn test_precision_add_overflow_becomes_absent() {
        let mut lhs = Precision::Exact(ScalarValue::Int32(Some(i32::MAX)));

        precision_add(&mut lhs, &Precision::Exact(ScalarValue::Int32(Some(1))));

        assert_eq!(lhs, Precision::Absent);
    }

    #[test]
    fn test_precision_add_rhs_absent_absorbs() {
        let mut lhs = Precision::Exact(ScalarValue::Int32(Some(42)));

        precision_add(&mut lhs, &Precision::Absent);

        assert_eq!(lhs, Precision::Absent);
    }

    #[test]
    fn test_precision_add_mixed_exactness() {
        let mut lhs = Precision::Exact(ScalarValue::Int32(Some(10)));

        precision_add(&mut lhs, &Precision::Inexact(ScalarValue::Int32(Some(5))));

        assert_eq!(lhs, Precision::Inexact(ScalarValue::Int32(Some(15))));
    }
}
