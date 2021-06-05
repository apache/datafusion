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

//! Functionality used both on logical and physical plans

use crate::error::{DataFusionError, Result};
use arrow::datatypes::{Field, Schema};
use std::collections::HashSet;

use crate::physical_plan::expressions::Column;

/// All valid types of joins.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinType {
    /// Inner Join
    Inner,
    /// Left Join
    Left,
    /// Right Join
    Right,
    /// Full Join
    Full,
    /// Semi Join
    Semi,
    /// Anti Join
    Anti,
}

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

    let remaining = right
        .difference(on_right)
        .cloned()
        .collect::<HashSet<Column>>();

    let collisions = left.intersection(&remaining).collect::<HashSet<_>>();

    if !collisions.is_empty() {
        return Err(DataFusionError::Plan(format!(
                "The left schema and the right schema have the following columns with the same name without being on the ON statement: {:?}. Consider aliasing them.",
                collisions,
            )));
    };

    Ok(())
}

/// Creates a schema for a join operation.
/// The fields from the left side are first
pub fn build_join_schema(
    left: &Schema,
    right: &Schema,
    on: JoinOnRef,
    join_type: &JoinType,
) -> Schema {
    let fields: Vec<Field> = match join_type {
        JoinType::Inner | JoinType::Left | JoinType::Full => {
            // remove right-side join keys if they have the same names as the left-side
            let duplicate_keys = &on
                .iter()
                .filter(|(l, r)| l.name() == r.name())
                .map(|on| on.1.name())
                .collect::<HashSet<_>>();

            let left_fields = left.fields().iter();

            let right_fields = right
                .fields()
                .iter()
                .filter(|f| !duplicate_keys.contains(f.name().as_str()));

            // left then right
            left_fields.chain(right_fields).cloned().collect()
        }
        JoinType::Right => {
            // remove left-side join keys if they have the same names as the right-side
            let duplicate_keys = &on
                .iter()
                .filter(|(l, r)| l.name() == r.name())
                .map(|on| on.1.name())
                .collect::<HashSet<_>>();

            let left_fields = left
                .fields()
                .iter()
                .filter(|f| !duplicate_keys.contains(f.name().as_str()));

            let right_fields = right.fields().iter();

            // left then right
            left_fields.chain(right_fields).cloned().collect()
        }
        JoinType::Semi | JoinType::Anti => left.fields().clone(),
    };
    Schema::new(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

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

        assert!(check(&left, &right, on).is_err());
    }

    #[test]
    fn check_in_right() {
        let left = vec![Column::new("a", 0), Column::new("c", 1)];
        let right = vec![Column::new("b", 0)];
        let on = &[(Column::new("a", 0), Column::new("b", 0))];

        assert!(check(&left, &right, on).is_ok());
    }
}
