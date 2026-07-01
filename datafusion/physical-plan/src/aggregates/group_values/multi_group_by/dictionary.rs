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

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanBufferBuilder, DictionaryArray, PrimitiveArray,
};
use arrow::datatypes::{ArrowDictionaryKeyType, ArrowNativeType, Field};
use datafusion_common::Result;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct DictionaryGroupValuesColumn<K: ArrowDictionaryKeyType + Send + Sync> {
    inner: Box<dyn GroupColumn>,
    null_array: ArrayRef,
    _phantom: PhantomData<K>,
}

impl<K: ArrowDictionaryKeyType + Send + Sync> DictionaryGroupValuesColumn<K> {
    pub fn new(inner: Box<dyn GroupColumn>, field: &Field) -> Self {
        let null_array = arrow::array::new_null_array(field.data_type(), 1);
        Self {
            inner,
            null_array,
            _phantom: PhantomData,
        }
    }

    #[inline]
    fn into_dict(values: ArrayRef) -> ArrayRef {
        // at some point in the future we may want to support bumping the key types
        // https://github.com/apache/datafusion/issues/23127
        let keys: PrimitiveArray<K> = (0..values.len())
            .map(|i| {
                if values.is_null(i) {
                    None
                } else {
                    Some(K::Native::usize_as(i))
                }
            })
            .collect();
        Arc::new(DictionaryArray::<K>::new(keys, values))
    }
}

impl<K: ArrowDictionaryKeyType + Send + Sync> GroupColumn
    for DictionaryGroupValuesColumn<K>
{
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        let dict = array.as_dictionary::<K>();
        match dict.key(rhs_row) {
            None => self.inner.equal_to(lhs_row, &self.null_array, 0),
            Some(key) => self.inner.equal_to(lhs_row, dict.values(), key),
        }
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) -> Result<()> {
        let dict = array.as_dictionary::<K>();
        match dict.key(row) {
            None => self.inner.append_val(&self.null_array, 0),
            Some(key) => self.inner.append_val(dict.values(), key),
        }
    }

    fn vectorized_equal_to(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut BooleanBufferBuilder,
    ) {
        let dict = array.as_dictionary::<K>();
        let keys = dict.keys();
        let values = dict.values();

        if keys.null_count() == 0 {
            let key_indices: Vec<usize> =
                rhs_rows.iter().map(|&r| keys.value(r).as_usize()).collect();
            self.inner.vectorized_equal_to(
                lhs_rows,
                values,
                &key_indices,
                equal_to_results,
            );
        } else {
            for (i, (lhs_row, rhs_row)) in lhs_rows.iter().zip(rhs_rows).enumerate() {
                if equal_to_results.get_bit(i) {
                    let result = match dict.key(*rhs_row) {
                        None => self.inner.equal_to(*lhs_row, &self.null_array, 0),
                        Some(key) => self.inner.equal_to(*lhs_row, values, key),
                    };
                    equal_to_results.set_bit(i, result);
                }
            }
        }
    }

    fn vectorized_append(&mut self, array: &ArrayRef, rows: &[usize]) -> Result<()> {
        let dict = array.as_dictionary::<K>();
        let keys = dict.keys();

        if keys.null_count() == 0 {
            let key_indices: Vec<usize> =
                rows.iter().map(|&r| keys.value(r).as_usize()).collect();
            self.inner.vectorized_append(dict.values(), &key_indices)
        } else {
            let values = dict.values();
            for &row in rows {
                match dict.key(row) {
                    None => self.inner.append_val(&self.null_array, 0)?,
                    Some(k) => self.inner.append_val(values, k)?,
                }
            }
            Ok(())
        }
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        Self::into_dict(self.inner.build())
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        Self::into_dict(self.inner.take_n(n))
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

    fn utf8_col() -> DictionaryGroupValuesColumn<Int32Type> {
        let field = Field::new("", DataType::Utf8, true);
        DictionaryGroupValuesColumn::<Int32Type>::new(
            Box::new(ByteGroupValueBuilder::<i32>::new(OutputType::Utf8)),
            &field,
        )
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

    fn assert_is_dict_utf8(arr: &ArrayRef) {
        assert!(
            matches!(
                arr.data_type(),
                DataType::Dictionary(k, v)
                    if k.as_ref() == &DataType::Int32 && v.as_ref() == &DataType::Utf8
            ),
            "expected Dictionary(Int32, Utf8), got {:?}",
            arr.data_type()
        );
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

        // null in the values array: key 0 points to a null string in the values
        #[test]
        fn null_in_values_array() {
            let mut col = utf8_col();
            let arr: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
                Int32Array::from(vec![Some(0), Some(1), Some(0)]),
                Arc::new(StringArray::from(vec![None::<&str>, Some("a")])),
            ));
            for i in 0..3 {
                col.append_val(&arr, i).unwrap(); // null, "a", null
            }
            assert_eq!(col.len(), 3);

            assert!(col.equal_to(0, &arr, 0)); // null == null
            assert!(!col.equal_to(0, &arr, 1)); // null != "a"
            assert!(col.equal_to(2, &arr, 0)); // null == null
            assert!(col.equal_to(1, &arr, 1)); // "a" == "a"

            let out = Box::new(col).build();
            assert_eq!(str_values(&out), vec![None, Some("a".into()), None]);
        }

        // null key (null dict entry) and null value (null in values array) coexist
        #[test]
        fn null_key_and_null_value() {
            let mut col = utf8_col();
            // row 0: null key, row 1: key=0 → null value, row 2: key=1 → "b"
            let arr: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
                Int32Array::from(vec![None, Some(0), Some(1)]),
                Arc::new(StringArray::from(vec![None::<&str>, Some("b")])),
            ));
            for i in 0..3 {
                col.append_val(&arr, i).unwrap();
            }
            assert_eq!(col.len(), 3);

            assert!(col.equal_to(0, &arr, 0)); // null key == null key
            assert!(col.equal_to(1, &arr, 1)); // null value == null value
            assert!(col.equal_to(0, &arr, 1)); // null key == null value (both null)
            assert!(col.equal_to(2, &arr, 2)); // "b" == "b"
            assert!(!col.equal_to(0, &arr, 2)); // null != "b"
            assert!(!col.equal_to(2, &arr, 1)); // "b" != null

            let out = Box::new(col).build();
            assert_eq!(str_values(&out), vec![None, None, Some("b".into())]);
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
            let arr =
                dict_arr(&[None, Some(0), None, Some(1), Some(2)], &["a", "b", "c"]);
            for i in 0..5 {
                col.append_val(&arr, i).unwrap();
            }
            assert_eq!(col.len(), 5);

            let taken = col.take_n(2);
            assert_is_dict_utf8(&taken);
            assert_eq!(str_values(&taken), vec![None, Some("a".into())]);
            assert_eq!(col.len(), 3);

            assert!(col.equal_to(0, &arr, 0));
            assert!(col.equal_to(1, &arr, 3));

            let arr2 = dict_arr(&[Some(0)], &["d"]);
            col.append_val(&arr2, 0).unwrap();
            assert_eq!(col.len(), 4);

            let out = Box::new(col).build();
            assert_is_dict_utf8(&out);
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
            assert_is_dict_utf8(&taken);
            assert_eq!(str_values(&taken), vec![Some("alpha".into())]);
            assert_eq!(col.len(), 2);

            assert!(col.equal_to(0, &arr, 1));
            assert!(!col.equal_to(0, &arr, 0));

            let arr2 = dict_arr(&[Some(0)], &["gamma"]);
            col.append_val(&arr2, 0).unwrap();
            assert_eq!(col.len(), 3);

            let out = Box::new(col).build();
            assert_is_dict_utf8(&out);
            assert_eq!(
                str_values(&out),
                vec![
                    Some("beta".into()),
                    Some("alpha".into()),
                    Some("gamma".into())
                ]
            );
        }

        #[test]
        fn build_output_is_exact_dict_type() {
            let mut col = utf8_col();
            let arr = dict_arr(&[Some(0), None, Some(1)], &["hello", "world"]);
            for i in 0..3 {
                col.append_val(&arr, i).unwrap();
            }
            let out = Box::new(col).build();
            assert_is_dict_utf8(&out);
            assert_eq!(
                str_values(&out),
                vec![Some("hello".into()), None, Some("world".into())]
            );
        }

        #[test]
        fn take_n_output_is_exact_dict_type() {
            let mut col = utf8_col();
            let arr = dict_arr(&[Some(0), None, Some(1), Some(0)], &["foo", "bar"]);
            for i in 0..4 {
                col.append_val(&arr, i).unwrap();
            }
            let taken = col.take_n(2);
            assert_is_dict_utf8(&taken);
            assert_eq!(str_values(&taken), vec![Some("foo".into()), None]);

            let remaining = Box::new(col).build();
            assert_is_dict_utf8(&remaining);
            assert_eq!(
                str_values(&remaining),
                vec![Some("bar".into()), Some("foo".into())]
            );
        }
    }

    // Regression tests — add new cases here when a bug is fixed.
    mod regressions {
        use super::*;

        #[test]
        fn take_n_zero_is_noop() {
            let mut col = utf8_col();
            let arr = dict_arr(&[Some(0), Some(1)], &["a", "b"]);
            col.append_val(&arr, 0).unwrap();
            col.append_val(&arr, 1).unwrap();
            assert_eq!(col.len(), 2);

            let taken = col.take_n(0);
            assert_eq!(taken.len(), 0);
            assert_eq!(col.len(), 2);
        }

        #[test]
        fn take_n_all_then_build_empty() {
            let mut col = utf8_col();
            let arr = dict_arr(&[Some(0), Some(1)], &["a", "b"]);
            col.append_val(&arr, 0).unwrap();
            col.append_val(&arr, 1).unwrap();

            let taken = col.take_n(2);
            assert_is_dict_utf8(&taken);
            assert_eq!(str_values(&taken), vec![Some("a".into()), Some("b".into())]);
            assert_eq!(col.len(), 0);

            let out = Box::new(col).build();
            assert_is_dict_utf8(&out);
            assert_eq!(out.len(), 0);
        }
    }
}
