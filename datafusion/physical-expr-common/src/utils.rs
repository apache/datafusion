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

use std::borrow::Cow;
use std::sync::Arc;

use crate::metrics::ExpressionEvaluatorMetrics;
use crate::physical_expr::PhysicalExpr;
use crate::tree_node::ExprContext;

use arrow::array::cast::AsArray;
use arrow::array::{
    Array, ArrayDataBuilder, ArrayRef, BooleanArray, BooleanBufferBuilder,
    DictionaryArray, FixedSizeBinaryArray, GenericByteArray, GenericByteViewArray,
    MutableArrayData, PrimitiveArray, make_array, new_null_array,
};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow::compute::{SlicesIterator, prep_null_mask_filter};
use arrow::datatypes::{
    ArrowDictionaryKeyType, ArrowNativeType, ArrowPrimitiveType, ByteArrayType,
    ByteViewType, DataType,
};
use arrow::record_batch::RecordBatch;
use arrow::{downcast_dictionary_array, downcast_primitive_array};
use datafusion_common::Result;
use datafusion_expr_common::sort_properties::ExprProperties;

/// Represents a [`PhysicalExpr`] node with associated properties (order and
/// range) in a context where properties are tracked.
pub type ExprPropertiesNode = ExprContext<ExprProperties>;

impl ExprPropertiesNode {
    /// Constructs a new `ExprPropertiesNode` with unknown properties for a
    /// given physical expression. This node initializes with default properties
    /// and recursively applies this to all child expressions.
    pub fn new_unknown(expr: Arc<dyn PhysicalExpr>) -> Self {
        let children = expr
            .children()
            .into_iter()
            .cloned()
            .map(Self::new_unknown)
            .collect();
        Self {
            expr,
            data: ExprProperties::new_unknown(),
            children,
        }
    }
}

/// If the mask selects more than this fraction of rows, use
/// `set_slices()` to copy contiguous ranges. Otherwise iterate
/// over individual positions using `set_indices()`.
const SCATTER_SLICES_SELECTIVITY_THRESHOLD: f64 = 0.8;

/// Scatter `truthy` array by boolean mask. When the mask evaluates `true`, next values of `truthy`
/// are taken, when the mask evaluates `false` values null values are filled.
///
/// # Arguments
/// * `mask` - Boolean values used to determine where to put the `truthy` values
/// * `truthy` - All values of this array are to scatter according to `mask` into final result.
pub fn scatter(mask: &BooleanArray, truthy: &dyn Array) -> Result<ArrayRef> {
    let mask = match mask.null_count() {
        0 => Cow::Borrowed(mask),
        _ => Cow::Owned(prep_null_mask_filter(mask)),
    };

    let output_len = mask.len();
    let count = mask.true_count();

    // Fast path: no true values mean all-null object
    if count == 0 {
        return Ok(new_null_array(truthy.data_type(), output_len));
    }

    // Fast path: all true means output = truthy
    if count == output_len {
        return Ok(truthy.slice(0, truthy.len()));
    }

    let selectivity = count as f64 / output_len as f64;
    let mask_buffer = mask.values();

    scatter_array(truthy, mask_buffer, output_len, selectivity)
}

fn scatter_array(
    truthy: &dyn Array,
    mask: &BooleanBuffer,
    output_len: usize,
    selectivity: f64,
) -> Result<ArrayRef> {
    downcast_primitive_array! {
        truthy => Ok(Arc::new(scatter_primitive(truthy, mask, output_len, selectivity))),
        DataType::Boolean => {
            Ok(Arc::new(scatter_boolean(truthy.as_boolean(), mask, output_len, selectivity)))
        }
        DataType::Utf8 => {
            Ok(Arc::new(scatter_bytes(truthy.as_string::<i32>(), mask, output_len, selectivity)))
        }
        DataType::LargeUtf8 => {
            Ok(Arc::new(scatter_bytes(truthy.as_string::<i64>(), mask, output_len, selectivity)))
        }
        DataType::Utf8View => {
            Ok(Arc::new(scatter_byte_view(truthy.as_string_view(), mask, output_len, selectivity)))
        }
        DataType::Binary => {
            Ok(Arc::new(scatter_bytes(truthy.as_binary::<i32>(), mask, output_len, selectivity)))
        }
        DataType::LargeBinary => {
            Ok(Arc::new(scatter_bytes(truthy.as_binary::<i64>(), mask, output_len, selectivity)))
        }
        DataType::BinaryView => {
            Ok(Arc::new(scatter_byte_view(truthy.as_binary_view(), mask, output_len, selectivity)))
        }
        DataType::FixedSizeBinary(_) => {
            Ok(Arc::new(scatter_fixed_size_binary(
                truthy.as_fixed_size_binary(), mask, output_len, selectivity,
            )))
        }
        DataType::Dictionary(_, _) => {
            downcast_dictionary_array! {
                truthy => Ok(Arc::new(scatter_dict(truthy, mask, output_len, selectivity))),
                _t => scatter_fallback(truthy, mask, output_len)
            }
        }
        _ => scatter_fallback(truthy, mask, output_len)
    }
}

#[inline(never)]
fn scatter_native<T: ArrowNativeType>(
    src: &[T],
    mask: &BooleanBuffer,
    output_len: usize,
    selectivity: f64,
) -> Buffer {
    let mut output = vec![T::default(); output_len];
    let mut src_offset = 0;

    if selectivity > SCATTER_SLICES_SELECTIVITY_THRESHOLD {
        for (start, end) in mask.set_slices() {
            let len = end - start;
            output[start..end].copy_from_slice(&src[src_offset..src_offset + len]);
            src_offset += len;
        }
    } else {
        for dst_idx in mask.set_indices() {
            output[dst_idx] = src[src_offset];
            src_offset += 1;
        }
    }

    output.into()
}

fn scatter_bits(
    src: &BooleanBuffer,
    mask: &BooleanBuffer,
    output_len: usize,
    selectivity: f64,
) -> Buffer {
    let mut builder = BooleanBufferBuilder::new(output_len);
    builder.advance(output_len);
    let mut src_offset = 0;

    if selectivity > SCATTER_SLICES_SELECTIVITY_THRESHOLD {
        for (start, end) in mask.set_slices() {
            for i in start..end {
                if src.value(src_offset) {
                    builder.set_bit(i, true);
                }
                src_offset += 1;
            }
        }
    } else {
        for dst_idx in mask.set_indices() {
            if src.value(src_offset) {
                builder.set_bit(dst_idx, true);
            }
            src_offset += 1;
        }
    }

    builder.finish().into_inner()
}

fn scatter_null_mask(
    src_nulls: Option<&NullBuffer>,
    mask: &BooleanBuffer,
    output_len: usize,
    selectivity: f64,
) -> Option<(usize, Buffer)> {
    let false_count = output_len - mask.count_set_bits();
    let src_null_count = src_nulls.map(|n| n.null_count()).unwrap_or(0);

    if src_null_count == 0 {
        if false_count == 0 {
            None
        } else {
            Some((false_count, mask.inner().clone()))
        }
    } else {
        let src_nulls = src_nulls.unwrap();
        let scattered = scatter_bits(src_nulls.inner(), mask, output_len, selectivity);
        let valid_count = scattered.count_set_bits_offset(0, output_len);
        let null_count = output_len - valid_count;
        if null_count == 0 {
            None
        } else {
            Some((null_count, scattered))
        }
    }
}

fn scatter_primitive<T: ArrowPrimitiveType>(
    truthy: &PrimitiveArray<T>,
    mask: &BooleanBuffer,
    output_len: usize,
    selectivity: f64,
) -> PrimitiveArray<T> {
    let values = scatter_native(truthy.values(), mask, output_len, selectivity);
    let mut builder = ArrayDataBuilder::new(truthy.data_type().clone())
        .len(output_len)
        .add_buffer(values);

    if let Some((null_count, nulls)) =
        scatter_null_mask(truthy.nulls(), mask, output_len, selectivity)
    {
        builder = builder.null_count(null_count).null_bit_buffer(Some(nulls));
    }

    let data = unsafe { builder.build_unchecked() };
    PrimitiveArray::from(data)
}

fn scatter_boolean(
    truthy: &BooleanArray,
    mask: &BooleanBuffer,
    output_len: usize,
    selectivity: f64,
) -> BooleanArray {
    let values = scatter_bits(truthy.values(), mask, output_len, selectivity);
    let mut builder = ArrayDataBuilder::new(DataType::Boolean)
        .len(output_len)
        .add_buffer(values);

    if let Some((null_count, nulls)) =
        scatter_null_mask(truthy.nulls(), mask, output_len, selectivity)
    {
        builder = builder.null_count(null_count).null_bit_buffer(Some(nulls));
    }

    let data = unsafe { builder.build_unchecked() };
    BooleanArray::from(data)
}

fn scatter_bytes<T: ByteArrayType>(
    truthy: &GenericByteArray<T>,
    mask: &BooleanBuffer,
    output_len: usize,
    selectivity: f64,
) -> GenericByteArray<T> {
    let src_offsets = truthy.value_offsets();
    let src_data = truthy.value_data();

    // Build output offsets: false positions get zero-length (offset stays same)
    let mut dst_offsets: Vec<T::Offset> = Vec::with_capacity(output_len + 1);
    let mut cur_offset = T::Offset::default();
    dst_offsets.push(cur_offset);

    let mut src_idx = 0;
    for i in 0..output_len {
        if mask.value(i) {
            let len =
                src_offsets[src_idx + 1].as_usize() - src_offsets[src_idx].as_usize();
            cur_offset += T::Offset::from_usize(len).unwrap();
            src_idx += 1;
        }
        dst_offsets.push(cur_offset);
    }

    let byte_start = src_offsets[0].as_usize();
    let byte_end = src_offsets[src_idx].as_usize();
    let dst_data: Buffer = src_data[byte_start..byte_end].into();

    let offsets_buffer: Buffer = dst_offsets.into();
    let mut builder = ArrayDataBuilder::new(truthy.data_type().clone())
        .len(output_len)
        .add_buffer(offsets_buffer)
        .add_buffer(dst_data);

    if let Some((null_count, nulls)) =
        scatter_null_mask(truthy.nulls(), mask, output_len, selectivity)
    {
        builder = builder.null_count(null_count).null_bit_buffer(Some(nulls));
    }

    let data = unsafe { builder.build_unchecked() };
    GenericByteArray::from(data)
}

fn scatter_byte_view<T: ByteViewType>(
    truthy: &GenericByteViewArray<T>,
    mask: &BooleanBuffer,
    output_len: usize,
    selectivity: f64,
) -> GenericByteViewArray<T> {
    let new_views = scatter_native(truthy.views(), mask, output_len, selectivity);

    let mut builder = ArrayDataBuilder::new(T::DATA_TYPE)
        .len(output_len)
        .add_buffer(new_views)
        .add_buffers(truthy.data_buffers().to_vec());

    if let Some((null_count, nulls)) =
        scatter_null_mask(truthy.nulls(), mask, output_len, selectivity)
    {
        builder = builder.null_count(null_count).null_bit_buffer(Some(nulls));
    }

    GenericByteViewArray::from(unsafe { builder.build_unchecked() })
}

fn scatter_fixed_size_binary(
    truthy: &FixedSizeBinaryArray,
    mask: &BooleanBuffer,
    output_len: usize,
    selectivity: f64,
) -> FixedSizeBinaryArray {
    let value_length = truthy.value_length() as usize;
    let mut output = vec![0u8; output_len * value_length];
    let mut src_idx = 0;

    if selectivity > SCATTER_SLICES_SELECTIVITY_THRESHOLD {
        for (start, end) in mask.set_slices() {
            for dst_idx in start..end {
                let src_bytes = truthy.value(src_idx);
                let dst_start = dst_idx * value_length;
                output[dst_start..dst_start + value_length].copy_from_slice(src_bytes);
                src_idx += 1;
            }
        }
    } else {
        for dst_idx in mask.set_indices() {
            let src_bytes = truthy.value(src_idx);
            let dst_start = dst_idx * value_length;
            output[dst_start..dst_start + value_length].copy_from_slice(src_bytes);
            src_idx += 1;
        }
    }

    let mut builder = ArrayDataBuilder::new(truthy.data_type().clone())
        .len(output_len)
        .add_buffer(Buffer::from(output));

    if let Some((null_count, nulls)) =
        scatter_null_mask(truthy.nulls(), mask, output_len, selectivity)
    {
        builder = builder.null_count(null_count).null_bit_buffer(Some(nulls));
    }

    let data = unsafe { builder.build_unchecked() };
    FixedSizeBinaryArray::from(data)
}

fn scatter_dict<K: ArrowDictionaryKeyType>(
    truthy: &DictionaryArray<K>,
    mask: &BooleanBuffer,
    output_len: usize,
    selectivity: f64,
) -> DictionaryArray<K> {
    let scattered_keys = scatter_primitive(truthy.keys(), mask, output_len, selectivity);
    let builder = scattered_keys
        .into_data()
        .into_builder()
        .data_type(truthy.data_type().clone())
        .child_data(vec![truthy.values().to_data()]);
    DictionaryArray::from(unsafe { builder.build_unchecked() })
}

fn scatter_fallback(
    truthy: &dyn Array,
    mask: &BooleanBuffer,
    output_len: usize,
) -> Result<ArrayRef> {
    let truthy_data = truthy.to_data();
    let mut mutable = MutableArrayData::new(vec![&truthy_data], true, output_len);

    let mut filled = 0;
    let mut true_pos = 0;

    let mask_array = BooleanArray::new(mask.clone(), None);
    SlicesIterator::new(&mask_array).for_each(|(start, end)| {
        if start > filled {
            mutable.extend_nulls(start - filled);
        }
        let len = end - start;
        mutable.extend(0, true_pos, true_pos + len);
        true_pos += len;
        filled = end;
    });

    if filled < output_len {
        mutable.extend_nulls(output_len - filled);
    }

    let data = mutable.freeze();
    Ok(make_array(data))
}

/// Evaluates expressions against a record batch.
/// This will convert the resulting ColumnarValues to ArrayRefs,
/// duplicating any ScalarValues that may have been returned,
/// and validating that the returned arrays all have the same
/// number of rows as the input batch.
#[inline]
pub fn evaluate_expressions_to_arrays<'a>(
    exprs: impl IntoIterator<Item = &'a Arc<dyn PhysicalExpr>>,
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>> {
    evaluate_expressions_to_arrays_with_metrics(exprs, batch, None)
}

/// Same as [`evaluate_expressions_to_arrays`] but records optional per-expression metrics.
///
/// For metrics tracking, see [`ExpressionEvaluatorMetrics`] for details.
#[inline]
pub fn evaluate_expressions_to_arrays_with_metrics<'a>(
    exprs: impl IntoIterator<Item = &'a Arc<dyn PhysicalExpr>>,
    batch: &RecordBatch,
    metrics: Option<&ExpressionEvaluatorMetrics>,
) -> Result<Vec<ArrayRef>> {
    let num_rows = batch.num_rows();
    exprs
        .into_iter()
        .enumerate()
        .map(|(idx, e)| {
            let _timer = metrics.and_then(|m| m.scoped_timer(idx));
            e.evaluate(batch)
                .and_then(|col| col.into_array_of_size(num_rows))
        })
        .collect::<Result<Vec<ArrayRef>>>()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray, StringViewArray, as_string_array};
    use arrow::compute::filter;
    use datafusion_common::cast::{as_boolean_array, as_int32_array};

    use super::*;

    #[test]
    fn scatter_int() -> Result<()> {
        let truthy = Arc::new(Int32Array::from(vec![1, 10, 11, 100]));
        let mask = BooleanArray::from(vec![true, true, false, false, true]);

        let expected =
            Int32Array::from_iter(vec![Some(1), Some(10), None, None, Some(11)]);
        let result = scatter(&mask, truthy.as_ref())?;
        let result = as_int32_array(&result)?;

        assert_eq!(&expected, result);
        Ok(())
    }

    #[test]
    fn scatter_int_end_with_false() -> Result<()> {
        let truthy = Arc::new(Int32Array::from(vec![1, 10, 11, 100]));
        let mask = BooleanArray::from(vec![true, false, true, false, false, false]);

        let expected =
            Int32Array::from_iter(vec![Some(1), None, Some(10), None, None, None]);
        let result = scatter(&mask, truthy.as_ref())?;
        let result = as_int32_array(&result)?;

        assert_eq!(&expected, result);
        Ok(())
    }

    #[test]
    fn scatter_with_null_mask() -> Result<()> {
        let truthy = Arc::new(Int32Array::from(vec![1, 10, 11]));
        let mask: BooleanArray = vec![Some(false), None, Some(true), Some(true), None]
            .into_iter()
            .collect();

        let expected = Int32Array::from_iter(vec![None, None, Some(1), Some(10), None]);
        let result = scatter(&mask, truthy.as_ref())?;
        let result = as_int32_array(&result)?;

        assert_eq!(&expected, result);
        Ok(())
    }

    #[test]
    fn scatter_boolean() -> Result<()> {
        let truthy = Arc::new(BooleanArray::from(vec![false, false, false, true]));
        let mask = BooleanArray::from(vec![true, true, false, false, true]);

        let expected = BooleanArray::from_iter(vec![
            Some(false),
            Some(false),
            None,
            None,
            Some(false),
        ]);
        let result = scatter(&mask, truthy.as_ref())?;
        let result = as_boolean_array(&result)?;

        assert_eq!(&expected, result);
        Ok(())
    }

    #[test]
    fn scatter_all_true() -> Result<()> {
        let truthy = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let mask = BooleanArray::from(vec![true, true, true]);

        let result = scatter(&mask, truthy.as_ref())?;
        let result = as_int32_array(&result)?;
        assert_eq!(&Int32Array::from(vec![1, 2, 3]), result);
        Ok(())
    }

    #[test]
    fn scatter_all_false() -> Result<()> {
        let truthy = Arc::new(Int32Array::from(Vec::<i32>::new()));
        let mask = BooleanArray::from(vec![false, false, false]);

        let result = scatter(&mask, truthy.as_ref())?;
        let result = as_int32_array(&result)?;
        let expected = Int32Array::from(vec![None, None, None]);
        assert_eq!(&expected, result);
        Ok(())
    }

    #[test]
    fn scatter_empty() -> Result<()> {
        let truthy = Arc::new(Int32Array::from(Vec::<i32>::new()));
        let mask = BooleanArray::from(Vec::<bool>::new());

        let result = scatter(&mask, truthy.as_ref())?;
        assert_eq!(result.len(), 0);
        Ok(())
    }

    #[test]
    fn scatter_primitive_with_source_nulls() -> Result<()> {
        let truthy = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let mask = BooleanArray::from(vec![true, false, true, true, false]);

        let expected = Int32Array::from_iter(vec![Some(1), None, None, Some(3), None]);
        let result = scatter(&mask, truthy.as_ref())?;
        let result = as_int32_array(&result)?;

        assert_eq!(&expected, result);
        Ok(())
    }

    #[test]
    fn scatter_string_test() -> Result<()> {
        let truthy = Arc::new(StringArray::from(vec!["hello", "world"]));
        let mask = BooleanArray::from(vec![true, false, false, true]);

        let result = scatter(&mask, truthy.as_ref())?;
        let result = as_string_array(&result);

        assert_eq!(result.len(), 4);
        assert!(result.is_valid(0));
        assert_eq!(result.value(0), "hello");
        assert!(result.is_null(1));
        assert!(result.is_null(2));
        assert!(result.is_valid(3));
        assert_eq!(result.value(3), "world");
        Ok(())
    }

    #[test]
    fn scatter_string_view_test() -> Result<()> {
        let truthy = Arc::new(StringViewArray::from(vec![
            "short",
            "a longer string that exceeds inline",
        ]));
        let mask = BooleanArray::from(vec![false, true, true, false]);

        let result = scatter(&mask, truthy.as_ref())?;
        let result = result.as_any().downcast_ref::<StringViewArray>().unwrap();

        assert_eq!(result.len(), 4);
        assert!(result.is_null(0));
        assert_eq!(result.value(1), "short");
        assert_eq!(result.value(2), "a longer string that exceeds inline");
        assert!(result.is_null(3));
        Ok(())
    }

    #[test]
    fn scatter_dictionary_test() -> Result<()> {
        use arrow::datatypes::Int8Type;

        let values = StringArray::from(vec!["a", "b"]);
        let truthy = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                arrow::array::Int8Array::from(vec![0, 1, 0]),
                Arc::new(values),
            )
            .unwrap(),
        );
        let mask = BooleanArray::from(vec![true, false, true, true, false]);

        let result = scatter(&mask, truthy.as_ref())?;
        let result = result
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .unwrap();

        assert_eq!(result.len(), 5);
        assert!(result.is_valid(0));
        assert!(result.is_null(1));
        assert!(result.is_valid(2));
        assert!(result.is_valid(3));
        assert!(result.is_null(4));
        Ok(())
    }

    #[test]
    fn scatter_filter_roundtrip() -> Result<()> {
        let original = Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50]));
        let mask = BooleanArray::from(vec![true, false, true, false, true]);

        // filter compacts: [10, 30, 50]
        let filtered = filter(original.as_ref(), &mask).unwrap();
        // scatter expands back: [10, null, 30, null, 50]
        let scattered = scatter(&mask, filtered.as_ref())?;
        let scattered = as_int32_array(&scattered)?;

        assert_eq!(scattered.len(), 5);
        assert_eq!(scattered.value(0), 10);
        assert!(scattered.is_null(1));
        assert_eq!(scattered.value(2), 30);
        assert!(scattered.is_null(3));
        assert_eq!(scattered.value(4), 50);
        Ok(())
    }
}
