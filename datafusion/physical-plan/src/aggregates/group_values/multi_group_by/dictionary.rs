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
use arrow::compute::concat;
use arrow::datatypes::{ArrowDictionaryKeyType, ArrowNativeType, Field};
use datafusion_common::Result;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct DictionaryGroupValuesColumn<K: ArrowDictionaryKeyType + Send + Sync> {
    inner: Box<dyn GroupColumn>,
    null_array: ArrayRef,
    cached_values: Option<ArrayRef>,
    cached_combined: Option<ArrayRef>,
    _phantom: PhantomData<K>,
}

impl<K: ArrowDictionaryKeyType + Send + Sync> DictionaryGroupValuesColumn<K> {
    pub fn new(inner: Box<dyn GroupColumn>, field: &Field) -> Self {
        let null_array = arrow::array::new_null_array(field.data_type(), 1);
        Self {
            inner,
            null_array,
            cached_values: None,
            cached_combined: None,
            _phantom: PhantomData,
        }
    }

    #[inline]
    fn get_combined(&mut self, values: &ArrayRef) -> Result<ArrayRef> {
        let is_cached = self
            .cached_values
            .as_ref()
            .is_some_and(|cached| Arc::ptr_eq(cached, values));
        if !is_cached {
            self.cached_combined =
                Some(concat(&[values.as_ref(), self.null_array.as_ref()])?);
            self.cached_values = Some(Arc::clone(values));
        }
        Ok(Arc::clone(self.cached_combined.as_ref().unwrap()))
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
            let combined = self.get_combined(dict.values())?;
            // last element of combined is always the null sentinel appended by get_combined
            let null_idx = combined.len() - 1;
            let key_indices: Vec<usize> = rows
                .iter()
                .map(|&r| dict.key(r).unwrap_or(null_idx))
                .collect();
            self.inner.vectorized_append(&combined, &key_indices)
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
            .map(|i| s.is_valid(i).then(|| s.value(i).to_owned()))
            .collect()
    }

    fn assert_is_dict_utf8(arr: &ArrayRef) {
        assert!(
            matches!(arr.data_type(),
                DataType::Dictionary(k, v)
                    if k.as_ref() == &DataType::Int32 && v.as_ref() == &DataType::Utf8),
            "expected Dictionary(Int32, Utf8), got {:?}",
            arr.data_type()
        );
    }

    fn true_buf(n: usize) -> BooleanBufferBuilder {
        let mut b = BooleanBufferBuilder::new(n);
        b.append_n(n, true);
        b
    }

    fn buf_to_vec(buf: &BooleanBufferBuilder) -> Vec<bool> {
        (0..buf.len()).map(|i| buf.get_bit(i)).collect()
    }

    // Null key (missing dict entry) and null value (null inside values array) are both
    // treated as null by equal_to and vectorized_equal_to.
    #[test]
    fn null_key_and_null_value_are_both_null() {
        let mut col = utf8_col();
        // row 0: null key; row 1: key=0, null value in values; row 2: key=1, "b"
        let arr: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![None, Some(0), Some(1)]),
            Arc::new(StringArray::from(vec![None::<&str>, Some("b")])),
        ));
        for i in 0..3 {
            col.append_val(&arr, i).unwrap();
        }
        assert_eq!(col.len(), 3);
        assert!(col.size() > 0);

        assert!(col.equal_to(0, &arr, 0));
        assert!(col.equal_to(1, &arr, 1));
        assert!(col.equal_to(0, &arr, 1)); // null key == null value
        assert!(col.equal_to(2, &arr, 2));
        assert!(!col.equal_to(0, &arr, 2));
        assert!(!col.equal_to(2, &arr, 0));

        let mut buf = true_buf(3);
        col.vectorized_equal_to(&[0, 1, 2], &arr, &[2, 2, 1], &mut buf);
        assert_eq!(buf_to_vec(&buf), vec![false, false, false]);

        let out = Box::new(col).build();
        assert_is_dict_utf8(&out);
        assert_eq!(str_values(&out), vec![None, None, Some("b".into())]);
    }

    // Full vectorized lifecycle using UInt64 values: vectorized_append,
    // vectorized_equal_to, take_n, then a second intern round and build.
    #[test]
    fn vectorized_full_lifecycle_u64() {
        use crate::aggregates::group_values::multi_group_by::primitive::PrimitiveGroupValueBuilder;
        use arrow::array::UInt64Array;
        use arrow::datatypes::{DataType, UInt64Type};

        let field = Field::new("", DataType::UInt64, true);
        let mut col = DictionaryGroupValuesColumn::<Int32Type>::new(
            Box::new(PrimitiveGroupValueBuilder::<UInt64Type, true>::new(
                DataType::UInt64,
            )),
            &field,
        );

        // values=[10,20,30], keys=[0,1,null,2,0] appended as 10,20,null,30,10
        let arr1: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![Some(0), Some(1), None, Some(2), Some(0)]),
            Arc::new(UInt64Array::from(vec![10u64, 20, 30])),
        ));
        col.vectorized_append(&arr1, &[0, 1, 2, 3, 4]).unwrap();
        assert_eq!(col.len(), 5);
        assert!(col.size() > 0);

        let mut buf = true_buf(5);
        col.vectorized_equal_to(&[0, 1, 2, 3, 4], &arr1, &[0, 1, 2, 3, 0], &mut buf);
        assert_eq!(buf_to_vec(&buf), vec![true, true, true, true, true]);

        let mut buf2 = true_buf(2);
        col.vectorized_equal_to(&[0, 4], &arr1, &[1, 1], &mut buf2);
        assert_eq!(buf_to_vec(&buf2), vec![false, false]);

        let out1 = col.take_n(3);
        assert_eq!(col.len(), 2);
        let d1 = out1.as_dictionary::<Int32Type>();
        let vals1 = d1.values().as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(vals1.value(d1.key(0).unwrap()), 10);
        assert_eq!(vals1.value(d1.key(1).unwrap()), 20);
        assert!(d1.key(2).is_none());

        let arr2: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![None, Some(0)]),
            Arc::new(UInt64Array::from(vec![99u64])),
        ));
        col.vectorized_append(&arr2, &[0, 1]).unwrap();
        assert_eq!(col.len(), 4);

        let mut buf3 = true_buf(2);
        col.vectorized_equal_to(&[2, 3], &arr2, &[0, 1], &mut buf3);
        assert_eq!(buf_to_vec(&buf3), vec![true, true]);

        let out2 = Box::new(col).build();
        let d2 = out2.as_dictionary::<Int32Type>();
        let vals2 = d2.values().as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(vals2.value(d2.key(0).unwrap()), 30);
        assert_eq!(vals2.value(d2.key(1).unwrap()), 10);
        assert!(d2.key(2).is_none());
        assert_eq!(vals2.value(d2.key(3).unwrap()), 99);
    }

    // vectorized_equal_to with null keys must handle mixed null/non-null rows
    // and must not overwrite pre-existing false bits in equal_to_results.
    #[test]
    fn vectorized_equal_to_with_null_keys() {
        let mut col = utf8_col();
        let arr = dict_arr(&[None, Some(0), Some(1)], &["a", "b"]);
        col.vectorized_append(&arr, &[0, 1, 2]).unwrap();
        assert_eq!(col.len(), 3);

        let mut buf = true_buf(3);
        col.vectorized_equal_to(&[0, 1, 2], &arr, &[0, 1, 1], &mut buf);
        assert_eq!(buf_to_vec(&buf), vec![true, true, false]);

        let mut buf2 = true_buf(3);
        buf2.set_bit(0, false);
        col.vectorized_equal_to(&[0, 1, 2], &arr, &[0, 1, 2], &mut buf2);
        assert_eq!(buf_to_vec(&buf2), vec![false, true, true]);
    }

    // Repeated intern/take_n cycle; verifies len and size shrink after take_n
    // and that surviving rows remain correct across emit boundaries.
    #[test]
    fn repeated_intern_emit_len_and_size() {
        let mut col = utf8_col();
        let arr1 = dict_arr(&[Some(0), None, Some(1), Some(0), None], &["cat", "dog"]);
        col.vectorized_append(&arr1, &[0, 1, 2, 3, 4]).unwrap();
        assert_eq!(col.len(), 5);
        let size_after_first = col.size();
        assert!(size_after_first > 0);

        let out1 = col.take_n(3);
        assert_is_dict_utf8(&out1);
        assert_eq!(
            str_values(&out1),
            vec![Some("cat".into()), None, Some("dog".into())]
        );
        assert_eq!(col.len(), 2);
        assert!(col.size() < size_after_first);

        assert!(col.equal_to(0, &arr1, 0));
        assert!(col.equal_to(1, &arr1, 1));

        let arr2 = dict_arr(&[Some(0), None], &["fish"]);
        col.vectorized_append(&arr2, &[0, 1]).unwrap();
        assert_eq!(col.len(), 4);

        let out2 = col.take_n(4);
        assert_is_dict_utf8(&out2);
        assert_eq!(
            str_values(&out2),
            vec![Some("cat".into()), None, Some("fish".into()), None]
        );
        assert_eq!(col.len(), 0);

        let empty = Box::new(col).build();
        assert_is_dict_utf8(&empty);
        assert_eq!(empty.len(), 0);
    }

    // take_n(0) is a no-op; draining all rows leaves an empty dict of the correct type.
    #[test]
    fn take_n_edge_cases() {
        let mut col = utf8_col();
        let arr = dict_arr(&[Some(0), None], &["a"]);
        col.vectorized_append(&arr, &[0, 1]).unwrap();
        assert_eq!(col.len(), 2);

        let nothing = col.take_n(0);
        assert_eq!(nothing.len(), 0);
        assert_eq!(col.len(), 2);

        let all = col.take_n(2);
        assert_is_dict_utf8(&all);
        assert_eq!(str_values(&all), vec![Some("a".into()), None]);
        assert_eq!(col.len(), 0);

        let empty = Box::new(col).build();
        assert_is_dict_utf8(&empty);
        assert_eq!(empty.len(), 0);
    }
}
