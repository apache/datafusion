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

//! Constraint propagator/solver for custom PhysicalExpr graphs.

use arrow::datatypes::DataType;
use datafusion_common::{internal_err, Result};
use datafusion_expr::interval_arithmetic::{apply_operator, satisfy_greater, Interval};
use datafusion_expr::Operator;

use super::utils::{
    convert_duration_type_to_interval, convert_interval_type_to_duration, get_inverse_op,
};

/// This function refines intervals `left_child` and `right_child` by applying
/// constraint propagation through `parent` via operation. The main idea is
/// that we can shrink ranges of variables x and y using parent interval p.
///
/// Assuming that x,y and p has ranges [xL, xU], [yL, yU], and [pL, pU], we
/// apply the following operations:
/// - For plus operation, specifically, we would first do
///     - [xL, xU] <- ([pL, pU] - [yL, yU]) ∩ [xL, xU], and then
///     - [yL, yU] <- ([pL, pU] - [xL, xU]) ∩ [yL, yU].
/// - For minus operation, specifically, we would first do
///     - [xL, xU] <- ([yL, yU] + [pL, pU]) ∩ [xL, xU], and then
///     - [yL, yU] <- ([xL, xU] - [pL, pU]) ∩ [yL, yU].
/// - For multiplication operation, specifically, we would first do
///     - [xL, xU] <- ([pL, pU] / [yL, yU]) ∩ [xL, xU], and then
///     - [yL, yU] <- ([pL, pU] / [xL, xU]) ∩ [yL, yU].
/// - For division operation, specifically, we would first do
///     - [xL, xU] <- ([yL, yU] * [pL, pU]) ∩ [xL, xU], and then
///     - [yL, yU] <- ([xL, xU] / [pL, pU]) ∩ [yL, yU].
pub fn propagate_arithmetic(
    op: &Operator,
    parent: &Interval,
    left_child: &Interval,
    right_child: &Interval,
) -> Result<Option<(Interval, Interval)>> {
    let inverse_op = get_inverse_op(*op)?;
    match (left_child.data_type(), right_child.data_type()) {
        // If we have a child whose type is a time interval (i.e. DataType::Interval),
        // we need special handling since timestamp differencing results in a
        // Duration type.
        (DataType::Timestamp(..), DataType::Interval(_)) => {
            propagate_time_interval_at_right(
                left_child,
                right_child,
                parent,
                op,
                &inverse_op,
            )
        }
        (DataType::Interval(_), DataType::Timestamp(..)) => {
            propagate_time_interval_at_left(
                left_child,
                right_child,
                parent,
                op,
                &inverse_op,
            )
        }
        _ => {
            // First, propagate to the left:
            match apply_operator(&inverse_op, parent, right_child)?
                .intersect(left_child)?
            {
                // Left is feasible:
                Some(value) => Ok(
                    // Propagate to the right using the new left.
                    propagate_right(&value, parent, right_child, op, &inverse_op)?
                        .map(|right| (value, right)),
                ),
                // If the left child is infeasible, short-circuit.
                None => Ok(None),
            }
        }
    }
}

/// This function refines intervals `left_child` and `right_child` by applying
/// comparison propagation through `parent` via operation. The main idea is
/// that we can shrink ranges of variables x and y using parent interval p.
/// Two intervals can be ordered in 6 ways for a Gt `>` operator:
/// ```text
///                           (1): Infeasible, short-circuit
/// left:   |        ================                                               |
/// right:  |                           ========================                    |
///
///                             (2): Update both interval
/// left:   |              ======================                                   |
/// right:  |                             ======================                    |
///                                          |
///                                          V
/// left:   |                             =======                                   |
/// right:  |                             =======                                   |
///
///                             (3): Update left interval
/// left:   |                  ==============================                       |
/// right:  |                           ==========                                  |
///                                          |
///                                          V
/// left:   |                           =====================                       |
/// right:  |                           ==========                                  |
///
///                             (4): Update right interval
/// left:   |                           ==========                                  |
/// right:  |                   ===========================                         |
///                                          |
///                                          V
/// left:   |                           ==========                                  |
/// right   |                   ==================                                  |
///
///                                   (5): No change
/// left:   |                       ============================                    |
/// right:  |               ===================                                     |
///
///                                   (6): No change
/// left:   |                                    ====================               |
/// right:  |                ===============                                        |
///
///         -inf --------------------------------------------------------------- +inf
/// ```
pub fn propagate_comparison(
    op: &Operator,
    parent: &Interval,
    left_child: &Interval,
    right_child: &Interval,
) -> Result<Option<(Interval, Interval)>> {
    if parent == &Interval::CERTAINLY_TRUE {
        match op {
            Operator::Eq => left_child.intersect(right_child).map(|result| {
                result.map(|intersection| (intersection.clone(), intersection))
            }),
            Operator::Gt => satisfy_greater(left_child, right_child, true),
            Operator::GtEq => satisfy_greater(left_child, right_child, false),
            Operator::Lt => satisfy_greater(right_child, left_child, true)
                .map(|t| t.map(reverse_tuple)),
            Operator::LtEq => satisfy_greater(right_child, left_child, false)
                .map(|t| t.map(reverse_tuple)),
            _ => internal_err!(
                "The operator must be a comparison operator to propagate intervals"
            ),
        }
    } else if parent == &Interval::CERTAINLY_FALSE {
        match op {
            Operator::Eq => {
                // TODO: Propagation is not possible until we support interval sets.
                Ok(None)
            }
            Operator::Gt => satisfy_greater(right_child, left_child, false),
            Operator::GtEq => satisfy_greater(right_child, left_child, true),
            Operator::Lt => satisfy_greater(left_child, right_child, false)
                .map(|t| t.map(reverse_tuple)),
            Operator::LtEq => satisfy_greater(left_child, right_child, true)
                .map(|t| t.map(reverse_tuple)),
            _ => internal_err!(
                "The operator must be a comparison operator to propagate intervals"
            ),
        }
    } else {
        // Uncertainty cannot change any end-point of the intervals.
        Ok(None)
    }
}

/// During the propagation of [`Interval`] values on an ExprIntervalGraph,
/// if there exists a `timestamp - timestamp` operation, the result would be
/// of type `Duration`. However, we may encounter a situation where a time interval
/// is involved in an arithmetic operation with a `Duration` type. This function
/// offers special handling for such cases, where the time interval resides on
/// the right side of the operation.
fn propagate_time_interval_at_right(
    left_child: &Interval,
    right_child: &Interval,
    parent: &Interval,
    op: &Operator,
    inverse_op: &Operator,
) -> Result<Option<(Interval, Interval)>> {
    // We check if the child's time interval(s) has a non-zero month or day field(s).
    // If so, we return it as is without propagating. Otherwise, we first convert
    // the time intervals to the `Duration` type, then propagate, and then convert
    // the bounds to time intervals again.
    let result = if let Some(duration) = convert_interval_type_to_duration(right_child) {
        match apply_operator(inverse_op, parent, &duration)?.intersect(left_child)? {
            Some(value) => {
                propagate_right(left_child, parent, &duration, op, inverse_op)?
                    .and_then(|right| convert_duration_type_to_interval(&right))
                    .map(|right| (value, right))
            }
            None => None,
        }
    } else {
        apply_operator(inverse_op, parent, right_child)?
            .intersect(left_child)?
            .map(|value| (value, right_child.clone()))
    };
    Ok(result)
}

/// This is a subfunction of the `propagate_arithmetic` function that propagates to the right child.
fn propagate_right(
    left: &Interval,
    parent: &Interval,
    right: &Interval,
    op: &Operator,
    inverse_op: &Operator,
) -> Result<Option<Interval>> {
    match op {
        Operator::Minus => apply_operator(op, left, parent),
        Operator::Plus => apply_operator(inverse_op, parent, left),
        Operator::Divide => apply_operator(op, left, parent),
        Operator::Multiply => apply_operator(inverse_op, parent, left),
        _ => internal_err!("Interval arithmetic does not support the operator {}", op),
    }?
    .intersect(right)
}

/// During the propagation of [`Interval`] values on an ExprIntervalGraph,
/// if there exists a `timestamp - timestamp` operation, the result would be
/// of type `Duration`. However, we may encounter a situation where a time interval
/// is involved in an arithmetic operation with a `Duration` type. This function
/// offers special handling for such cases, where the time interval resides on
/// the left side of the operation.
fn propagate_time_interval_at_left(
    left_child: &Interval,
    right_child: &Interval,
    parent: &Interval,
    op: &Operator,
    inverse_op: &Operator,
) -> Result<Option<(Interval, Interval)>> {
    // We check if the child's time interval(s) has a non-zero month or day field(s).
    // If so, we return it as is without propagating. Otherwise, we first convert
    // the time intervals to the `Duration` type, then propagate, and then convert
    // the bounds to time intervals again.
    let result = if let Some(duration) = convert_interval_type_to_duration(left_child) {
        match apply_operator(inverse_op, parent, right_child)?.intersect(duration)? {
            Some(value) => {
                let left = convert_duration_type_to_interval(&value);
                let right = propagate_right(&value, parent, right_child, op, inverse_op)?;
                match (left, right) {
                    (Some(left), Some(right)) => Some((left, right)),
                    _ => None,
                }
            }
            None => None,
        }
    } else {
        propagate_right(left_child, parent, right_child, op, inverse_op)?
            .map(|right| (left_child.clone(), right))
    };
    Ok(result)
}

fn reverse_tuple<T, U>((first, second): (T, U)) -> (U, T) {
    (second, first)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::TimeUnit;
    use datafusion_common::ScalarValue;

    use super::*;

    #[test]
    fn test_propagate_comparison() -> Result<()> {
        // In the examples below:
        // `left` is unbounded: [?, ?],
        // `right` is known to be [1000,1000]
        // so `left` < `right` results in no new knowledge of `right` but knowing that `left` is now < 1000:` [?, 999]
        let left = Interval::make_unbounded(&DataType::Int64)?;
        let right = Interval::make(Some(1000_i64), Some(1000_i64))?;
        assert_eq!(
            (Some((
                Interval::make(None, Some(999_i64))?,
                Interval::make(Some(1000_i64), Some(1000_i64))?,
            ))),
            propagate_comparison(
                &Operator::Lt,
                &Interval::CERTAINLY_TRUE,
                &left,
                &right
            )?
        );

        let left =
            Interval::make_unbounded(&DataType::Timestamp(TimeUnit::Nanosecond, None))?;
        let right = Interval::try_new(
            ScalarValue::TimestampNanosecond(Some(1000), None),
            ScalarValue::TimestampNanosecond(Some(1000), None),
        )?;
        assert_eq!(
            (Some((
                Interval::try_new(
                    ScalarValue::try_from(&DataType::Timestamp(
                        TimeUnit::Nanosecond,
                        None
                    ))
                    .unwrap(),
                    ScalarValue::TimestampNanosecond(Some(999), None),
                )?,
                Interval::try_new(
                    ScalarValue::TimestampNanosecond(Some(1000), None),
                    ScalarValue::TimestampNanosecond(Some(1000), None),
                )?
            ))),
            propagate_comparison(
                &Operator::Lt,
                &Interval::CERTAINLY_TRUE,
                &left,
                &right
            )?
        );

        let left = Interval::make_unbounded(&DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("+05:00".into()),
        ))?;
        let right = Interval::try_new(
            ScalarValue::TimestampNanosecond(Some(1000), Some("+05:00".into())),
            ScalarValue::TimestampNanosecond(Some(1000), Some("+05:00".into())),
        )?;
        assert_eq!(
            (Some((
                Interval::try_new(
                    ScalarValue::try_from(&DataType::Timestamp(
                        TimeUnit::Nanosecond,
                        Some("+05:00".into()),
                    ))
                    .unwrap(),
                    ScalarValue::TimestampNanosecond(Some(999), Some("+05:00".into())),
                )?,
                Interval::try_new(
                    ScalarValue::TimestampNanosecond(Some(1000), Some("+05:00".into())),
                    ScalarValue::TimestampNanosecond(Some(1000), Some("+05:00".into())),
                )?
            ))),
            propagate_comparison(
                &Operator::Lt,
                &Interval::CERTAINLY_TRUE,
                &left,
                &right
            )?
        );

        Ok(())
    }
}
