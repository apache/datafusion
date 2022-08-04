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

//! Join related functionality used both on logical and physical plans

use crate::error::{DataFusionError, Result};
use crate::logical_plan::JoinType;
use crate::physical_plan::expressions::Column;
use arrow::datatypes::{Field, Schema};
use arrow::error::ArrowError;
use datafusion_physical_expr::PhysicalExpr;
use futures::future::{BoxFuture, Shared};
use futures::{ready, FutureExt};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};

/// The on clause of the join, as vector of (left, right) columns.
pub type JoinOn = Vec<(Column, Column)>;
/// Reference for JoinOn.
pub type JoinOnRef<'a> = &'a [(Column, Column)];

/// Checks whether the schemas "left" and "right" and columns "on" represent a valid join.
/// They are valid whenever their columns' intersection equals the set `on`
pub fn check_join_is_valid(left: &Schema, right: &Schema, on: JoinOnRef) -> Result<()> {
    let left: HashSet<Column> = left
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, f)| Column::new(f.name(), idx))
        .collect();
    let right: HashSet<Column> = right
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, f)| Column::new(f.name(), idx))
        .collect();

    check_join_set_is_valid(&left, &right, on)
}

/// Checks whether the sets left, right and on compose a valid join.
/// They are valid whenever their intersection equals the set `on`
fn check_join_set_is_valid(
    left: &HashSet<Column>,
    right: &HashSet<Column>,
    on: &[(Column, Column)],
) -> Result<()> {
    let on_left = &on.iter().map(|on| on.0.clone()).collect::<HashSet<_>>();
    let left_missing = on_left.difference(left).collect::<HashSet<_>>();

    let on_right = &on.iter().map(|on| on.1.clone()).collect::<HashSet<_>>();
    let right_missing = on_right.difference(right).collect::<HashSet<_>>();

    if !left_missing.is_empty() | !right_missing.is_empty() {
        return Err(DataFusionError::Plan(format!(
                "The left or right side of the join does not have all columns on \"on\": \nMissing on the left: {:?}\nMissing on the right: {:?}",
                left_missing,
                right_missing,
            )));
    };

    Ok(())
}

/// Used in ColumnIndex to distinguish which side the index is for
#[derive(Debug, Clone)]
pub enum JoinSide {
    /// Left side of the join
    Left,
    /// Right side of the join
    Right,
}

/// Information about the index and placement (left or right) of the columns
#[derive(Debug, Clone)]
pub struct ColumnIndex {
    /// Index of the column
    pub index: usize,
    /// Whether the column is at the left or right side
    pub side: JoinSide,
}

/// Filter applied before join output
#[derive(Debug, Clone)]
pub struct JoinFilter {
    /// Filter expression
    expression: Arc<dyn PhysicalExpr>,
    /// Column indices required to construct intermediate batch for filtering
    column_indices: Vec<ColumnIndex>,
    /// Physical schema of intermediate batch
    schema: Schema,
}

impl JoinFilter {
    /// Creates new JoinFilter
    pub fn new(
        expression: Arc<dyn PhysicalExpr>,
        column_indices: Vec<ColumnIndex>,
        schema: Schema,
    ) -> JoinFilter {
        JoinFilter {
            expression,
            column_indices,
            schema,
        }
    }

    /// Helper for building ColumnIndex vector from left and right indices
    pub fn build_column_indices(
        left_indices: Vec<usize>,
        right_indices: Vec<usize>,
    ) -> Vec<ColumnIndex> {
        left_indices
            .into_iter()
            .map(|i| ColumnIndex {
                index: i,
                side: JoinSide::Left,
            })
            .chain(right_indices.into_iter().map(|i| ColumnIndex {
                index: i,
                side: JoinSide::Right,
            }))
            .collect()
    }

    /// Filter expression
    pub fn expression(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expression
    }

    /// Column indices for intermediate batch creation
    pub fn column_indices(&self) -> &[ColumnIndex] {
        &self.column_indices
    }

    /// Intermediate batch schema
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

/// Returns the output field given the input field. Outer joins may
/// insert nulls even if the input was not null
///
fn output_join_field(old_field: &Field, join_type: &JoinType, is_left: bool) -> Field {
    let force_nullable = match join_type {
        JoinType::Inner => false,
        JoinType::Left => !is_left, // right input is padded with nulls
        JoinType::Right => is_left, // left input is padded with nulls
        JoinType::Full => true,     // both inputs can be padded with nulls
        JoinType::Semi => false,    // doesn't introduce nulls
        JoinType::Anti => false,    // doesn't introduce nulls (or can it??)
    };

    if force_nullable {
        old_field.clone().with_nullable(true)
    } else {
        old_field.clone()
    }
}

/// Creates a schema for a join operation.
/// The fields from the left side are first
pub fn build_join_schema(
    left: &Schema,
    right: &Schema,
    join_type: &JoinType,
) -> (Schema, Vec<ColumnIndex>) {
    let (fields, column_indices): (Vec<Field>, Vec<ColumnIndex>) = match join_type {
        JoinType::Inner | JoinType::Left | JoinType::Full | JoinType::Right => {
            let left_fields = left
                .fields()
                .iter()
                .map(|f| output_join_field(f, join_type, true))
                .enumerate()
                .map(|(index, f)| {
                    (
                        f,
                        ColumnIndex {
                            index,
                            side: JoinSide::Left,
                        },
                    )
                });
            let right_fields = right
                .fields()
                .iter()
                .map(|f| output_join_field(f, join_type, false))
                .enumerate()
                .map(|(index, f)| {
                    (
                        f,
                        ColumnIndex {
                            index,
                            side: JoinSide::Right,
                        },
                    )
                });

            // left then right
            left_fields.chain(right_fields).unzip()
        }
        JoinType::Semi | JoinType::Anti => left
            .fields()
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, f)| {
                (
                    f,
                    ColumnIndex {
                        index,
                        side: JoinSide::Left,
                    },
                )
            })
            .unzip(),
    };

    (Schema::new(fields), column_indices)
}

/// A [`OnceAsync`] can be used to run an async closure once, with subsequent calls
/// to [`OnceAsync::once`] returning a [`OnceFut`] to the same asynchronous computation
///
/// This is useful for joins where the results of one child are buffered in memory
/// and shared across potentially multiple output partitions
pub(crate) struct OnceAsync<T> {
    fut: Mutex<Option<OnceFut<T>>>,
}

impl<T> Default for OnceAsync<T> {
    fn default() -> Self {
        Self {
            fut: Mutex::new(None),
        }
    }
}

impl<T> std::fmt::Debug for OnceAsync<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OnceAsync")
    }
}

impl<T: 'static> OnceAsync<T> {
    /// If this is the first call to this function on this object, will invoke
    /// `f` to obtain a future and return a [`OnceFut`] referring to this
    ///
    /// If this is not the first call, will return a [`OnceFut`] referring
    /// to the same future as was returned by the first call
    pub(crate) fn once<F, Fut>(&self, f: F) -> OnceFut<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.fut
            .lock()
            .get_or_insert_with(|| OnceFut::new(f()))
            .clone()
    }
}

/// The shared future type used internally within [`OnceAsync`]
type OnceFutPending<T> = Shared<BoxFuture<'static, Arc<Result<T>>>>;

/// A [`OnceFut`] represents a shared asynchronous computation, that will be evaluated
/// once for all [`Clone`]'s, with [`OnceFut::get`] providing a non-consuming interface
/// to drive the underlying [`Future`] to completion
pub(crate) struct OnceFut<T> {
    state: OnceFutState<T>,
}

impl<T> Clone for OnceFut<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

enum OnceFutState<T> {
    Pending(OnceFutPending<T>),
    Ready(Arc<Result<T>>),
}

impl<T> Clone for OnceFutState<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Pending(p) => Self::Pending(p.clone()),
            Self::Ready(r) => Self::Ready(r.clone()),
        }
    }
}

impl<T: 'static> OnceFut<T> {
    /// Create a new [`OnceFut`] from a [`Future`]
    pub(crate) fn new<Fut>(fut: Fut) -> Self
    where
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        Self {
            state: OnceFutState::Pending(fut.map(Arc::new).boxed().shared()),
        }
    }

    /// Get the result of the computation if it is ready, without consuming it
    pub(crate) fn get(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<&T, ArrowError>> {
        if let OnceFutState::Pending(fut) = &mut self.state {
            let r = ready!(fut.poll_unpin(cx));
            self.state = OnceFutState::Ready(r);
        }

        // Cannot use loop as this would trip up the borrow checker
        match &self.state {
            OnceFutState::Pending(_) => unreachable!(),
            OnceFutState::Ready(r) => Poll::Ready(
                r.as_ref()
                    .as_ref()
                    .map_err(|e| ArrowError::ExternalError(e.to_string().into())),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn check(left: &[Column], right: &[Column], on: &[(Column, Column)]) -> Result<()> {
        let left = left
            .iter()
            .map(|x| x.to_owned())
            .collect::<HashSet<Column>>();
        let right = right
            .iter()
            .map(|x| x.to_owned())
            .collect::<HashSet<Column>>();
        check_join_set_is_valid(&left, &right, on)
    }

    #[test]
    fn check_valid() -> Result<()> {
        let left = vec![Column::new("a", 0), Column::new("b1", 1)];
        let right = vec![Column::new("a", 0), Column::new("b2", 1)];
        let on = &[(Column::new("a", 0), Column::new("a", 0))];

        check(&left, &right, on)?;
        Ok(())
    }

    #[test]
    fn check_not_in_right() {
        let left = vec![Column::new("a", 0), Column::new("b", 1)];
        let right = vec![Column::new("b", 0)];
        let on = &[(Column::new("a", 0), Column::new("a", 0))];

        assert!(check(&left, &right, on).is_err());
    }

    #[test]
    fn check_not_in_left() {
        let left = vec![Column::new("b", 0)];
        let right = vec![Column::new("a", 0)];
        let on = &[(Column::new("a", 0), Column::new("a", 0))];

        assert!(check(&left, &right, on).is_err());
    }

    #[test]
    fn check_collision() {
        // column "a" would appear both in left and right
        let left = vec![Column::new("a", 0), Column::new("c", 1)];
        let right = vec![Column::new("a", 0), Column::new("b", 1)];
        let on = &[(Column::new("a", 0), Column::new("b", 1))];

        assert!(check(&left, &right, on).is_ok());
    }

    #[test]
    fn check_in_right() {
        let left = vec![Column::new("a", 0), Column::new("c", 1)];
        let right = vec![Column::new("b", 0)];
        let on = &[(Column::new("a", 0), Column::new("b", 0))];

        assert!(check(&left, &right, on).is_ok());
    }

    #[test]
    fn test_join_schema() -> Result<()> {
        let a = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let a_nulls = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let b = Schema::new(vec![Field::new("b", DataType::Int32, false)]);
        let b_nulls = Schema::new(vec![Field::new("b", DataType::Int32, true)]);

        let cases = vec![
            (&a, &b, JoinType::Inner, &a, &b),
            (&a, &b_nulls, JoinType::Inner, &a, &b_nulls),
            (&a_nulls, &b, JoinType::Inner, &a_nulls, &b),
            (&a_nulls, &b_nulls, JoinType::Inner, &a_nulls, &b_nulls),
            // right input of a `LEFT` join can be null, regardless of input nullness
            (&a, &b, JoinType::Left, &a, &b_nulls),
            (&a, &b_nulls, JoinType::Left, &a, &b_nulls),
            (&a_nulls, &b, JoinType::Left, &a_nulls, &b_nulls),
            (&a_nulls, &b_nulls, JoinType::Left, &a_nulls, &b_nulls),
            // left input of a `RIGHT` join can be null, regardless of input nullness
            (&a, &b, JoinType::Right, &a_nulls, &b),
            (&a, &b_nulls, JoinType::Right, &a_nulls, &b_nulls),
            (&a_nulls, &b, JoinType::Right, &a_nulls, &b),
            (&a_nulls, &b_nulls, JoinType::Right, &a_nulls, &b_nulls),
            // Either input of a `FULL` join can be null
            (&a, &b, JoinType::Full, &a_nulls, &b_nulls),
            (&a, &b_nulls, JoinType::Full, &a_nulls, &b_nulls),
            (&a_nulls, &b, JoinType::Full, &a_nulls, &b_nulls),
            (&a_nulls, &b_nulls, JoinType::Full, &a_nulls, &b_nulls),
        ];

        for (left_in, right_in, join_type, left_out, right_out) in cases {
            let (schema, _) = build_join_schema(left_in, right_in, &join_type);

            let expected_fields = left_out
                .fields()
                .iter()
                .cloned()
                .chain(right_out.fields().iter().cloned())
                .collect();

            let expected_schema = Schema::new(expected_fields);
            assert_eq!(
                schema,
                expected_schema,
                "Mismatch with left_in={}:{}, right_in={}:{}, join_type={:?}",
                left_in.fields()[0].name(),
                left_in.fields()[0].is_nullable(),
                right_in.fields()[0].name(),
                right_in.fields()[0].is_nullable(),
                join_type
            );
        }

        Ok(())
    }
}
