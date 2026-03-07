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
//! Provides a cheap pairwise [`ScalarValue`] addition that directly
//! extracts inner primitive values, avoiding the expensive
//! `ScalarValue::add` path (which round-trips through Arrow arrays).

use crate::stats::Precision;
use crate::{Result, ScalarValue};

/// Add two [`ScalarValue`]s by directly extracting and adding their
/// inner primitive values.
///
/// This avoids `ScalarValue::add` which converts both operands to
/// single-element Arrow arrays, runs the `add_wrapping` kernel, and
/// converts the result back — 3 heap allocations per call.
///
/// For non-primitive types, falls back to `ScalarValue::add`.
pub(crate) fn scalar_add(lhs: &ScalarValue, rhs: &ScalarValue) -> Result<ScalarValue> {
    macro_rules! add_wrapping {
        ($lhs:expr, $rhs:expr, $VARIANT:ident) => {
            match ($lhs, $rhs) {
                (ScalarValue::$VARIANT(Some(a)), ScalarValue::$VARIANT(Some(b))) => {
                    Ok(ScalarValue::$VARIANT(Some(a.wrapping_add(*b))))
                }
                (ScalarValue::$VARIANT(None), other)
                | (other, ScalarValue::$VARIANT(None)) => Ok(other.clone()),
                _ => unreachable!(),
            }
        };
    }

    macro_rules! add_decimal {
        ($lhs:expr, $rhs:expr, $VARIANT:ident) => {
            match ($lhs, $rhs) {
                (
                    ScalarValue::$VARIANT(Some(a), p, s),
                    ScalarValue::$VARIANT(Some(b), _, _),
                ) => Ok(ScalarValue::$VARIANT(Some(a.wrapping_add(*b)), *p, *s)),
                (ScalarValue::$VARIANT(None, _, _), other)
                | (other, ScalarValue::$VARIANT(None, _, _)) => Ok(other.clone()),
                _ => unreachable!(),
            }
        };
    }

    macro_rules! add_float {
        ($lhs:expr, $rhs:expr, $VARIANT:ident) => {
            match ($lhs, $rhs) {
                (ScalarValue::$VARIANT(Some(a)), ScalarValue::$VARIANT(Some(b))) => {
                    Ok(ScalarValue::$VARIANT(Some(*a + *b)))
                }
                (ScalarValue::$VARIANT(None), other)
                | (other, ScalarValue::$VARIANT(None)) => Ok(other.clone()),
                _ => unreachable!(),
            }
        };
    }

    match lhs {
        ScalarValue::Int8(_) => add_wrapping!(lhs, rhs, Int8),
        ScalarValue::Int16(_) => add_wrapping!(lhs, rhs, Int16),
        ScalarValue::Int32(_) => add_wrapping!(lhs, rhs, Int32),
        ScalarValue::Int64(_) => add_wrapping!(lhs, rhs, Int64),
        ScalarValue::UInt8(_) => add_wrapping!(lhs, rhs, UInt8),
        ScalarValue::UInt16(_) => add_wrapping!(lhs, rhs, UInt16),
        ScalarValue::UInt32(_) => add_wrapping!(lhs, rhs, UInt32),
        ScalarValue::UInt64(_) => add_wrapping!(lhs, rhs, UInt64),
        ScalarValue::Float16(_) => add_float!(lhs, rhs, Float16),
        ScalarValue::Float32(_) => add_float!(lhs, rhs, Float32),
        ScalarValue::Float64(_) => add_float!(lhs, rhs, Float64),
        ScalarValue::Decimal32(_, _, _) => add_decimal!(lhs, rhs, Decimal32),
        ScalarValue::Decimal64(_, _, _) => add_decimal!(lhs, rhs, Decimal64),
        ScalarValue::Decimal128(_, _, _) => add_decimal!(lhs, rhs, Decimal128),
        ScalarValue::Decimal256(_, _, _) => add_decimal!(lhs, rhs, Decimal256),
        // Fallback: use the existing ScalarValue::add
        _ => lhs.add(rhs),
    }
}

/// [`Precision`]-aware sum of two [`ScalarValue`] precisions using
/// cheap direct addition via [`scalar_add`].
///
/// Mirrors the semantics of `Precision<ScalarValue>::add` but avoids
/// the expensive `ScalarValue::add` round-trip through Arrow arrays.
pub(crate) fn precision_add(
    lhs: &Precision<ScalarValue>,
    rhs: &Precision<ScalarValue>,
) -> Precision<ScalarValue> {
    match (lhs, rhs) {
        (Precision::Exact(a), Precision::Exact(b)) => scalar_add(a, b)
            .map(Precision::Exact)
            .unwrap_or(Precision::Absent),
        (Precision::Inexact(a), Precision::Exact(b))
        | (Precision::Exact(a), Precision::Inexact(b))
        | (Precision::Inexact(a), Precision::Inexact(b)) => scalar_add(a, b)
            .map(Precision::Inexact)
            .unwrap_or(Precision::Absent),
        (_, _) => Precision::Absent,
    }
}
