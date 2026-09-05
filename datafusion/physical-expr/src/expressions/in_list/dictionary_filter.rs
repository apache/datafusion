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

use arrow::array::{Array, AsArray, BooleanArray};
use arrow::compute::take;
use datafusion_common::Result;

use super::static_filter::{StaticFilter, StaticFilterRef};

/// Adds dictionary-encoded needle support to a filter that expects plain arrays.
/// This wrapper is only used when the input expression returns dictionaries.
pub(super) struct DictionaryFilter {
    inner: StaticFilterRef,
}

impl DictionaryFilter {
    pub(super) fn new(inner: StaticFilterRef) -> Self {
        Self { inner }
    }
}

impl StaticFilter for DictionaryFilter {
    fn null_count(&self) -> usize {
        self.inner.null_count()
    }

    fn contains(&self, needles: &dyn Array, negated: bool) -> Result<BooleanArray> {
        let Some(dictionary) = needles.as_any_dictionary_opt() else {
            return self.inner.contains(needles, negated);
        };
        let values_contains = self.contains(dictionary.values().as_ref(), negated)?;
        let result = take(&values_contains, dictionary.keys(), None)?;
        Ok(result.as_boolean().clone())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, Decimal128Array, DictionaryArray, Int8Array, Int16Array, Int32Array,
        Int64Array, TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    };

    use super::super::strategy::instantiate_static_filter;
    use super::*;

    #[test]
    fn dictionary_needles_support_all_key_types() -> Result<()> {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let expected = BooleanArray::from(vec![Some(true), Some(false), None]);

        macro_rules! check_keys {
            ($keys:expr) => {{
                let needles = DictionaryArray::try_new($keys, Arc::clone(&values))?;
                let filter = instantiate_static_filter(
                    Arc::new(Int32Array::from(vec![1, 3])),
                    needles.data_type(),
                )?;
                assert_eq!(filter.contains(&needles, false)?, expected);
            }};
        }

        check_keys!(Int8Array::from(vec![Some(0), Some(1), None]));
        check_keys!(Int16Array::from(vec![Some(0), Some(1), None]));
        check_keys!(Int32Array::from(vec![Some(0), Some(1), None]));
        check_keys!(Int64Array::from(vec![Some(0), Some(1), None]));
        check_keys!(UInt8Array::from(vec![Some(0), Some(1), None]));
        check_keys!(UInt16Array::from(vec![Some(0), Some(1), None]));
        check_keys!(UInt32Array::from(vec![Some(0), Some(1), None]));
        check_keys!(UInt64Array::from(vec![Some(0), Some(1), None]));

        Ok(())
    }

    #[test]
    fn nested_dictionary_needles_preserve_nulls() -> Result<()> {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2), None]));
        let inner: ArrayRef = Arc::new(DictionaryArray::try_new(
            Int8Array::from(vec![0, 1, 2]),
            values,
        )?);
        let needles = DictionaryArray::try_new(
            Int16Array::from(vec![Some(0), Some(1), Some(2), None]),
            inner,
        )?;
        let filter = instantiate_static_filter(
            Arc::new(Int32Array::from(vec![1, 3])),
            needles.data_type(),
        )?;

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), Some(false), None, None])
        );
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), Some(true), None, None])
        );

        Ok(())
    }

    #[test]
    fn dictionary_timestamp_needles_keep_timezone_compatibility() -> Result<()> {
        let timestamps: ArrayRef =
            Arc::new(TimestampNanosecondArray::from(vec![1, 3]).with_timezone("UTC"));
        let values: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(vec![1, 2]).with_timezone("Europe/Paris"),
        );
        let needles = DictionaryArray::try_new(Int8Array::from(vec![0, 1]), values)?;
        let filter = instantiate_static_filter(timestamps, needles.data_type())?;
        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![true, false])
        );

        Ok(())
    }

    #[test]
    fn dictionary_decimal_needles_keep_precision_scale_compatibility() -> Result<()> {
        // Five list values use the hash-set filter.
        let decimals: ArrayRef = Arc::new(
            Decimal128Array::from(vec![1, 3, 5, 7, 9]).with_precision_and_scale(10, 2)?,
        );
        let values: ArrayRef =
            Arc::new(Decimal128Array::from(vec![1, 2]).with_precision_and_scale(11, 3)?);
        let needles = DictionaryArray::try_new(Int8Array::from(vec![0, 1]), values)?;
        let filter = instantiate_static_filter(decimals, needles.data_type())?;
        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![true, false])
        );

        Ok(())
    }
}
