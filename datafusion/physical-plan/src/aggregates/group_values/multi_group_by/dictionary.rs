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

use crate::aggregates::group_values::multi_group_by::GroupColumn;

use arrow::array::Int32Array;
use std::sync::Arc;
pub struct DictionaryGroupValuesColumn {
    inner: Box<dyn GroupColumn>,
}
impl DictionaryGroupValuesColumn {
    pub fn new(inner: Box<dyn GroupColumn>) -> Self {
        Self { inner }
    }
}

impl GroupColumn for DictionaryGroupValuesColumn {
    fn equal_to(
        &self,
        lhs_row: usize,
        array: &arrow::array::ArrayRef,
        rhs_row: usize,
    ) -> bool {
        false
    }
    fn append_val(
        &mut self,
        array: &arrow::array::ArrayRef,
        row: usize,
    ) -> datafusion_common::Result<()> {
        Ok(())
    }
    fn vectorized_equal_to(
        &self,
        lhs_rows: &[usize],
        array: &arrow::array::ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut arrow::array::BooleanBufferBuilder,
    ) {
    }
    fn vectorized_append(
        &mut self,
        array: &arrow::array::ArrayRef,
        rows: &[usize],
    ) -> datafusion_common::Result<()> {
        Ok(())
    }
    fn len(&self) -> usize {
        0
    }
    fn size(&self) -> usize {
        0
    }
    fn build(self: Box<Self>) -> arrow::array::ArrayRef {
        Arc::new(Int32Array::from(vec![12, 3, 3]))
    }
    fn take_n(&mut self, n: usize) -> arrow::array::ArrayRef {
        Arc::new(Int32Array::from(vec![12, 3, 3]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregates::group_values::multi_group_by::bytes::ByteGroupValueBuilder;
    use arrow::array::{
        Array, ArrayRef, BooleanBufferBuilder, DictionaryArray, Int32Array, StringArray,
    };
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Int32Type};
    use datafusion_physical_expr::binary_map::OutputType;
    use std::sync::Arc;

    fn utf8_col() -> DictionaryGroupValuesColumn {
        DictionaryGroupValuesColumn::new(Box::new(
            ByteGroupValueBuilder::<i32>::new(OutputType::Utf8),
        ))
    }

    fn dict_arr(keys: &[Option<i32>], values: &[&str]) -> ArrayRef {
        Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(keys.to_vec()),
            Arc::new(StringArray::from(values.to_vec())),
        ))
    }

    fn str_values(arr: &ArrayRef) -> Vec<Option<String>> {
        let plain = cast(arr.as_ref(), &DataType::Utf8).unwrap();
        let s = plain.as_any().downcast_ref::<StringArray>().unwrap();
        (0..s.len())
            .map(|i| {
                if s.is_null(i) {
                    None
                } else {
                    Some(s.value(i).to_owned())
                }
            })
            .collect()
    }

    fn make_true_buf(n: usize) -> BooleanBufferBuilder {
        let mut b = BooleanBufferBuilder::new(n);
        b.append_n(n, true);
        b
    }

    fn to_vec(buf: &BooleanBufferBuilder) -> Vec<bool> {
        (0..buf.len()).map(|i| buf.get_bit(i)).collect()
    }

    mod null_handling {
        use super::*;

        #[test]
        fn take_n_then_append_preserves_nulls() {
            let mut col = utf8_col();
            let arr = dict_arr(&[Some(0), None, Some(1)], &["a", "b"]);
            for i in 0..3 {
                col.append_val(&arr, i).unwrap();
            }

            let taken = col.take_n(2);
            assert_eq!(str_values(&taken), vec![Some("a".into()), None]);
            assert_eq!(col.len(), 1);

            let arr2 = dict_arr(&[None, Some(0)], &["c"]);
            col.append_val(&arr2, 0).unwrap();
            col.append_val(&arr2, 1).unwrap();
            assert_eq!(col.len(), 3);

            assert!(col.equal_to(1, &arr2, 0));
            assert!(!col.equal_to(1, &arr2, 1));
            assert!(col.equal_to(2, &arr2, 1));

            let out = Box::new(col).build();
            assert_eq!(
                str_values(&out),
                vec![Some("b".into()), None, Some("c".into())]
            );
        }

        #[test]
        fn null_equal_to_all_combinations() {
            let mut col = utf8_col();
            let arr = dict_arr(&[None, Some(0)], &["a"]);
            col.append_val(&arr, 0).unwrap();
            col.append_val(&arr, 1).unwrap();
            assert_eq!(col.len(), 2);
            assert!(col.size() > 0);

            assert!(col.equal_to(0, &arr, 0));
            assert!(!col.equal_to(0, &arr, 1));
            assert!(!col.equal_to(1, &arr, 0));
            assert!(col.equal_to(1, &arr, 1));

            let mut buf = make_true_buf(4);
            col.vectorized_equal_to(&[0, 0, 1, 1], &arr, &[0, 1, 0, 1], &mut buf);
            assert_eq!(to_vec(&buf), vec![true, false, false, true]);
        }
    }

    mod comparison {
        use super::*;

        #[test]
        fn append_and_equal_to() {
            let mut col = utf8_col();
            let arr = dict_arr(&[Some(0), Some(1), Some(0)], &["a", "b"]);
            col.append_val(&arr, 0).unwrap();
            col.append_val(&arr, 1).unwrap();
            col.append_val(&arr, 2).unwrap();
            assert_eq!(col.len(), 3);

            assert!(col.equal_to(0, &arr, 0));
            assert!(col.equal_to(0, &arr, 2));
            assert!(!col.equal_to(0, &arr, 1));

            let mut col2 = utf8_col();
            let ooo = dict_arr(&[Some(1), Some(0)], &["first", "second"]);
            col2.append_val(&ooo, 0).unwrap();
            col2.append_val(&ooo, 1).unwrap();
            assert!(col2.equal_to(0, &ooo, 0));
            assert!(col2.equal_to(1, &ooo, 1));
            assert!(!col2.equal_to(0, &ooo, 1));
        }

        #[test]
        fn vectorized_append_and_equal_to() {
            let mut col = utf8_col();
            let arr = dict_arr(&[Some(0), Some(1), Some(0)], &["x", "y"]);
            col.vectorized_append(&arr, &[0, 1, 2]).unwrap();
            assert_eq!(col.len(), 3);

            let mut buf = make_true_buf(3);
            col.vectorized_equal_to(&[0, 1, 2], &arr, &[0, 1, 0], &mut buf);
            assert_eq!(to_vec(&buf), vec![true, true, true]);

            let mut buf2 = make_true_buf(2);
            col.vectorized_equal_to(&[0, 1], &arr, &[1, 0], &mut buf2);
            assert_eq!(to_vec(&buf2), vec![false, false]);
        }
    }

    mod emit {
        use super::*;

        #[test]
        fn take_n_with_nulls_then_build() {
            let mut col = utf8_col();
            let arr = dict_arr(&[None, Some(0), None, Some(1), Some(2)], &["a", "b", "c"]);
            for i in 0..5 {
                col.append_val(&arr, i).unwrap();
            }
            assert_eq!(col.len(), 5);

            let taken = col.take_n(2);
            assert_eq!(str_values(&taken), vec![None, Some("a".into())]);
            assert_eq!(col.len(), 3);

            assert!(col.equal_to(0, &arr, 0));
            assert!(col.equal_to(1, &arr, 3));

            let arr2 = dict_arr(&[Some(0)], &["d"]);
            col.append_val(&arr2, 0).unwrap();
            assert_eq!(col.len(), 4);

            let out = Box::new(col).build();
            assert_eq!(
                str_values(&out),
                vec![None, Some("b".into()), Some("c".into()), Some("d".into())]
            );
        }

        #[test]
        fn interleaved_append_compare_take_build() {
            let mut col = utf8_col();
            let arr = dict_arr(&[Some(0), Some(1), Some(0)], &["alpha", "beta"]);
            col.append_val(&arr, 0).unwrap();
            col.append_val(&arr, 1).unwrap();
            col.append_val(&arr, 2).unwrap();

            assert!(col.equal_to(0, &arr, 0));
            assert!(!col.equal_to(1, &arr, 2));

            let taken = col.take_n(1);
            assert_eq!(str_values(&taken), vec![Some("alpha".into())]);
            assert_eq!(col.len(), 2);

            assert!(col.equal_to(0, &arr, 1));
            assert!(!col.equal_to(0, &arr, 0));

            let arr2 = dict_arr(&[Some(0)], &["gamma"]);
            col.append_val(&arr2, 0).unwrap();
            assert_eq!(col.len(), 3);

            let out = Box::new(col).build();
            assert_eq!(
                str_values(&out),
                vec![
                    Some("beta".into()),
                    Some("alpha".into()),
                    Some("gamma".into())
                ]
            );
        }
    }
}
