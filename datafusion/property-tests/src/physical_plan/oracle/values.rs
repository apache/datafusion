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

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::cast;
use arrow::compute::kernels::cmp::not_distinct;
use datafusion_common::{Result, ScalarValue};
use datafusion_physical_expr::{AcrossPartitions, ConstExpr, PhysicalExpr};

/// Returns the distinct values of `expr` on the rows of `batches`, in the
/// order in which they first appear. Null is a value, distinct from every
/// non-null value.
#[allow(clippy::allow_attributes, clippy::mutable_key_type)] // ScalarValue has interior mutability but is intentionally used as hash key
pub fn distinct_values(
    batches: &[RecordBatch],
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<Vec<ScalarValue>> {
    let mut seen = HashSet::new();
    let mut values = vec![];
    for batch in batches {
        let array = expr.evaluate(batch)?.into_array(batch.num_rows())?;
        for row in 0..array.len() {
            let value = ScalarValue::try_from_array(&array, row)?;
            if seen.insert(value.clone()) {
                values.push(value);
            }
        }
    }
    Ok(values)
}

/// The position of the first row of `batches` on which `left` and `right`
/// have different values, with the value of each, or `None` if they are equal
/// on every row. Positions count rows across all batches, in order. Two nulls
/// are equal, as in `IS NOT DISTINCT FROM`. A value of `right` is cast to the
/// type of `left` if the types differ.
///
/// Returns an error if an expression cannot be evaluated on the batches, or
/// `right` cannot be cast to the type of `left`.
pub fn first_unequal_row(
    batches: &[RecordBatch],
    left: &Arc<dyn PhysicalExpr>,
    right: &Arc<dyn PhysicalExpr>,
) -> Result<Option<(usize, ScalarValue, ScalarValue)>> {
    let mut offset = 0;
    for batch in batches {
        let rows = batch.num_rows();
        let left = left.evaluate(batch)?.into_array(rows)?;
        let mut right = right.evaluate(batch)?.into_array(rows)?;
        if right.data_type() != left.data_type() {
            right = cast(&right, left.data_type())?;
        }
        // The comparison kernels do not support every nested type, so nested
        // values are compared one row at a time
        let unequal = if left.data_type().is_nested() {
            let mut unequal = None;
            for row in 0..rows {
                if ScalarValue::try_from_array(&left, row)?
                    != ScalarValue::try_from_array(&right, row)?
                {
                    unequal = Some(row);
                    break;
                }
            }
            unequal
        } else {
            let equal = not_distinct(&left, &right)?;
            (0..rows).find(|row| !equal.value(*row))
        };
        if let Some(row) = unequal {
            return Ok(Some((
                offset + row,
                ScalarValue::try_from_array(&left, row)?,
                ScalarValue::try_from_array(&right, row)?,
            )));
        }
        offset += rows;
    }
    Ok(None)
}

/// Each way in which `partitions`, the batches of each partition, contradict
/// `constant`, such as `a@0 is constant, but partition 2 has 3 distinct
/// values, such as 2 and NULL`, in partition order.
///
/// A constant has a single value within each partition. If it is uniform
/// across partitions, it has the same value in every partition, and that
/// value is the given one if there is one, compared after casting it to the
/// type of the expression, since it can have another type, such as a wider
/// integer type. Null is a value, and partitions without rows have none.
///
/// Returns an error if the expression cannot be evaluated on the batches.
pub fn constant_violations(
    partitions: &[Vec<RecordBatch>],
    constant: &ConstExpr,
) -> Result<Vec<String>> {
    let expr = &constant.expr;
    let mut violations = vec![];
    // The value of each partition that has rows and a single value
    let mut single_values = vec![];
    for (p, batches) in partitions.iter().enumerate() {
        let values = distinct_values(batches, expr)?;
        match values.as_slice() {
            [] => {}
            [value] => single_values.push((p, value.clone())),
            [first, second, ..] => violations.push(format!(
                "{expr} is constant, but partition {p} has {} distinct values, such as \
                 {first} and {second}",
                values.len()
            )),
        }
    }
    match &constant.across_partitions {
        AcrossPartitions::Heterogeneous => {}
        AcrossPartitions::Uniform(Some(expected)) => {
            for (p, value) in &single_values {
                if !expected
                    .cast_to(&value.data_type())
                    .is_ok_and(|expected| expected == *value)
                {
                    violations.push(format!(
                        "{expr} is constant with the value {expected} in every partition, \
                         but partition {p} has the value {value}"
                    ));
                }
            }
        }
        AcrossPartitions::Uniform(None) => {
            if let Some(((first_p, first), others)) = single_values.split_first() {
                for (p, value) in others {
                    if value != first {
                        violations.push(format!(
                            "{expr} is constant with the same value in every partition, \
                             but partition {first_p} has the value {first} and partition \
                             {p} has the value {value}"
                        ));
                    }
                }
            }
        }
    }
    Ok(violations)
}
