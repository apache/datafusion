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

use crate::aggregate::min_max::{max, min};
use arrow_schema::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue};

// A Enum that specifies which operator could use in segment tree.
pub enum Operator {
    ADD,
    MIN,
    MAX,
}

impl Operator {
    // The operation that is performed to combine two intervals in the segment tree.
    //
    // This function must be associative, that is `combine(combine(a, b), c) =
    // combine(a, combine(b, c))`.
    pub fn combine(&self, a: &ScalarValue, b: &ScalarValue) -> Result<ScalarValue> {
        match self {
            Operator::ADD => a.add(b),
            Operator::MIN => min(a, b),
            Operator::MAX => max(a, b),
        }
    }
}
// A segment tree is a binary tree where each node contains the combination of the
// children under the operation.
pub struct SegmentTree {
    buf: Vec<ScalarValue>,
    count: usize,
    op: Operator,
    data_type: DataType,
}

impl SegmentTree {
    // Builds a tree using the given buffer with ScalarValues.
    pub fn build(
        mut buf: Vec<ScalarValue>,
        op: Operator,
        data_type: DataType,
    ) -> Result<Self> {
        let len = buf.len();
        buf.reserve_exact(len);
        for i in 0..len {
            let clone = unsafe { buf.get_unchecked(i).clone() }; // SAFETY: will never out of bound.
            buf.push(clone);
        }
        SegmentTree::build_inner(buf, op, data_type)
    }

    fn build_inner(
        mut buf: Vec<ScalarValue>,
        op: Operator,
        data_type: DataType,
    ) -> Result<Self> {
        let len = buf.len();
        let count = len >> 1;
        if len & 1 == 1 {
            panic!("SegmentTree::build_inner: odd size");
        }
        for i in (1..count).rev() {
            let res = op.combine(&buf[i << 1], &buf[i << 1 | 1])?;
            buf[i] = res;
        }
        Ok(SegmentTree {
            buf,
            count,
            op,
            data_type,
        })
    }

    // Computes `a[l] op a[l+1] op ... op a[r-1]`.
    // Uses `O(log(len))` time.
    // If `l >= r`, this method returns error.
    pub fn query(&self, mut l: usize, mut r: usize) -> Result<ScalarValue> {
        if l >= r {
            return Err(DataFusionError::Internal(
                "Query SegmentTree l must <= r".to_string(),
            ));
        }
        let mut res = ScalarValue::try_from(&self.data_type)?;
        l += self.count;
        r += self.count;
        while l < r {
            if l & 1 == 1 {
                res = self.op.combine(&res, &self.buf[l])?;
                l += 1;
            }
            if r & 1 == 1 {
                r -= 1;
                res = self.op.combine(&res, &self.buf[r])?;
            }
            l >>= 1;
            r >>= 1;
        }
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use crate::window::segment_tree::{Operator, SegmentTree};
    use arrow_schema::DataType;
    use datafusion_common::ScalarValue;
    use rand::Rng;

    #[test]
    fn test_query_segment_tree() {
        let test_size = 1000;
        let val_range = 10000;
        let mut rng = rand::thread_rng();
        let rand_vals: Vec<i32> = (0..test_size)
            .map(|_| rng.gen_range(0..val_range))
            .collect();
        let rand_scalar: Vec<ScalarValue> = rand_vals
            .iter()
            .map(|v| ScalarValue::from(v.clone()))
            .collect();

        let segment_tree_add =
            SegmentTree::build(rand_scalar.clone(), Operator::ADD, DataType::Int32)
                .unwrap();

        let segment_tree_min =
            SegmentTree::build(rand_scalar.clone(), Operator::MIN, DataType::Int32)
                .unwrap();
        let segment_tree_max =
            SegmentTree::build(rand_scalar, Operator::MAX, DataType::Int32).unwrap();

        for _i in 0..1000 {
            let start: usize = rng.gen_range(0..test_size - 1);
            let end: usize = rng.gen_range(start + 1..test_size);

            let add_result = segment_tree_add.query(start, end).unwrap();
            let min_result = segment_tree_min.query(start, end).unwrap();
            let max_result = segment_tree_max.query(start, end).unwrap();

            assert_eq!(
                add_result,
                ScalarValue::from(rand_vals[start..end].iter().sum::<i32>())
            );
            assert_eq!(
                min_result,
                ScalarValue::from(rand_vals[start..end].iter().min().unwrap().clone())
            );
            assert_eq!(
                max_result,
                ScalarValue::from(rand_vals[start..end].iter().max().unwrap().clone())
            );
        }
    }
}
