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

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{
    layout, make_array, new_empty_array, new_null_array, Array, ArrayRef, BooleanArray,
    Int32Array, Scalar, UnionArray,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, FieldRef, UnionFields, UnionMode};

use arrow::buffer::{BooleanBuffer, MutableBuffer, NullBuffer, ScalarBuffer};
use arrow::util::bit_util;
use datafusion_common::cast::as_union_array;
use datafusion_common::{
    exec_datafusion_err, exec_err, internal_err, ExprSchema, Result, ScalarValue,
};
use datafusion_expr::{ColumnarValue, Expr};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct UnionExtractFun {
    signature: Signature,
}

impl Default for UnionExtractFun {
    fn default() -> Self {
        Self::new()
    }
}

impl UnionExtractFun {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UnionExtractFun {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "union_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        // should be using return_type_from_exprs and not calling the default implementation
        internal_err!("union_extract should return type from exprs")
    }

    fn return_type_from_exprs(
        &self,
        args: &[Expr],
        _: &dyn ExprSchema,
        arg_types: &[DataType],
    ) -> Result<DataType> {
        if args.len() != 2 {
            return exec_err!(
                "union_extract expects 2 arguments, got {} instead",
                args.len()
            );
        }

        let fields = if let DataType::Union(fields, _) = &arg_types[0] {
            fields
        } else {
            return exec_err!(
                "union_extract first argument must be a union, got {} instead",
                arg_types[0]
            );
        };

        let field_name = if let Expr::Literal(ScalarValue::Utf8(Some(field_name))) =
            &args[1]
        {
            field_name
        } else {
            return exec_err!(
                "union_extract second argument must be a non-null string literal, got {} instead",
                arg_types[1]
            );
        };

        let field = find_field(fields, field_name)?.1;

        Ok(field.data_type().clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!(
                "union_extract expects 2 arguments, got {} instead",
                args.len()
            );
        }

        let union = &args[0];

        let target_name = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(target_name))) => Ok(target_name),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => exec_err!("union_extract second argument must be a non-null string literal, got a null instead"),
            _ => exec_err!("union_extract second argument must be a non-null string literal, got {} instead", &args[1].data_type()),
        };

        match union {
            ColumnarValue::Array(array) => {
                let union_array = as_union_array(&array).map_err(|_| {
                    exec_datafusion_err!(
                        "union_extract first argument must be a union, got {} instead",
                        array.data_type()
                    )
                })?;

                let (fields, mode) = match union_array.data_type() {
                    DataType::Union(fields, mode) => (fields, mode),
                    _ => unreachable!(),
                };

                let target_type_id = find_field(fields, target_name?)?.0;

                match mode {
                    UnionMode::Sparse => {
                        Ok(extract_sparse(union_array, fields, target_type_id)?)
                    }
                    UnionMode::Dense => {
                        Ok(extract_dense(union_array, fields, target_type_id)?)
                    }
                }
            }
            ColumnarValue::Scalar(ScalarValue::Union(value, fields, _)) => {
                let target_name = target_name?;
                let (target_type_id, target) = find_field(fields, target_name)?;

                let result = match value {
                    Some((type_id, value)) if target_type_id == *type_id => {
                        *value.clone()
                    }
                    _ => ScalarValue::try_from(target.data_type())?,
                };

                Ok(ColumnarValue::Scalar(result))
            }
            other => exec_err!(
                "union_extract first argument must be a union, got {} instead",
                other.data_type()
            ),
        }
    }
}

fn find_field<'a>(fields: &'a UnionFields, name: &str) -> Result<(i8, &'a FieldRef)> {
    fields
        .iter()
        .find(|field| field.1.name() == name)
        .ok_or_else(|| exec_datafusion_err!("field {name} not found on union"))
}

fn extract_sparse(
    union_array: &UnionArray,
    fields: &UnionFields,
    target_type_id: i8,
) -> Result<ColumnarValue> {
    let target = union_array.child(target_type_id);

    if fields.len() == 1 // case 1.1: if there is a single field, all type ids are the same, and since union doesn't have a null mask, the result array is exactly the same as it only child
        || union_array.is_empty() // case 1.2: sparse union length and childrens length must match, if the union is empty, so is any children
        || target.null_count() == target.len() || target.data_type().is_null()
    // case 1.3: if all values of the target children are null, regardless of selected type ids, the result will also be completely null
    {
        Ok(ColumnarValue::Array(Arc::clone(target)))
    } else {
        match eq_scalar(union_array.type_ids(), target_type_id) {
            // case 2: all type ids equals our target, and since unions doesn't have a null mask, the result array is exactly the same as our target
            BoolValue::Scalar(true) => Ok(ColumnarValue::Array(Arc::clone(target))),
            // case 3: none type_id matches our target, the result is a null array
            BoolValue::Scalar(false) => {
                if layout(target.data_type()).can_contain_null_mask {
                    // case 3.1: target array can contain a null mask
                    //SAFETY: The only change to the array data is the addition of a null mask, and if the target data type can contain a null mask was just checked above
                    let data = unsafe {
                        target
                            .into_data()
                            .into_builder()
                            .nulls(Some(NullBuffer::new_null(target.len())))
                            .build_unchecked()
                    };

                    Ok(ColumnarValue::Array(make_array(data)))
                } else {
                    // case 3.2: target can't contain a null mask
                    Ok(new_null_columnar_value(target.data_type(), target.len()))
                }
            }
            // case 4: some but not all type_id matches our target
            BoolValue::Buffer(selected) => {
                if layout(target.data_type()).can_contain_null_mask {
                    // case 4.1: target array can contain a null mask
                    let nulls = match target.nulls().filter(|n| n.null_count() > 0) {
                        // case 4.1.1: our target child has nulls and types other than our target are selected, union the masks
                        // the case where n.null_count() == n.len() is cheaply handled at case 1.3
                        Some(nulls) => &selected & nulls.inner(),
                        // case 4.1.2: target child has no nulls, but types other than our target are selected, use the selected mask as a null mask
                        None => selected,
                    };

                    //SAFETY: The only change to the array data is the addition of a null mask, and if the target data type can contain a null mask was just checked above
                    let data = unsafe {
                        assert_eq!(nulls.len(), target.len());

                        target
                            .into_data()
                            .into_builder()
                            .nulls(Some(nulls.into()))
                            .build_unchecked()
                    };

                    Ok(ColumnarValue::Array(make_array(data)))
                } else {
                    // case 4.2: target can't containt a null mask, zip the values that match with a null value
                    Ok(ColumnarValue::Array(arrow::compute::kernels::zip::zip(
                        &BooleanArray::new(selected, None),
                        target,
                        &Scalar::new(new_null_array(target.data_type(), 1)),
                    )?))
                }
            }
        }
    }
}

fn extract_dense(
    union_array: &UnionArray,
    fields: &UnionFields,
    target_type_id: i8,
) -> Result<ColumnarValue> {
    let target = union_array.child(target_type_id);
    let offsets = union_array.offsets().unwrap();

    if union_array.is_empty() {
        // case 1: the union is empty
        if target.is_empty() {
            // case 1.1: the target is also empty, do a cheap Arc::clone instead of allocating a new empty array
            Ok(ColumnarValue::Array(Arc::clone(target)))
        } else {
            // case 1.2: the target is not empty, allocate a new empty array
            Ok(ColumnarValue::Array(new_empty_array(target.data_type())))
        }
    } else if target.is_empty() {
        // case 2: the union is not empty but the target is, which implies that none type_id points to it. The result is a null array
        Ok(new_null_columnar_value(
            target.data_type(),
            union_array.len(),
        ))
    } else if target.null_count() == target.len() || target.data_type().is_null() {
        // case 3: since all values on our target are null, regardless of selected type ids and offsets, the result is a null array
        match target.len().cmp(&union_array.len()) {
            // case 3.1: since the target is smaller than the union, allocate a new correclty sized null array
            Ordering::Less => Ok(new_null_columnar_value(
                target.data_type(),
                union_array.len(),
            )),
            // case 3.2: target equals the union len, return it direcly
            Ordering::Equal => Ok(ColumnarValue::Array(Arc::clone(target))),
            // case 3.3: target len is bigger than the union len, slice it
            Ordering::Greater => {
                Ok(ColumnarValue::Array(target.slice(0, union_array.len())))
            }
        }
    } else if fields.len() == 1 // case A: since there's a single field, our target, every type id must matches our target
        || fields
            .iter()
            .filter(|(field_type_id, _)| *field_type_id != target_type_id)
            .all(|(sibling_type_id, _)| union_array.child(sibling_type_id).is_empty())
    // case B: since siblings are empty, every type id must matches our target
    {
        // case 4: every type id matches our target
        Ok(ColumnarValue::Array(extract_dense_all_selected(
            union_array,
            target,
            offsets,
        )?))
    } else {
        match eq_scalar(union_array.type_ids(), target_type_id) {
            // case 4C: all type ids matches our target.
            // Non empty sibling without any selected value may happen after slicing the parent union,
            // since only type_ids and offsets are sliced, not the children
            BoolValue::Scalar(true) => Ok(ColumnarValue::Array(
                extract_dense_all_selected(union_array, target, offsets)?,
            )),
            BoolValue::Scalar(false) => {
                // case 5: none type_id matches our target, so the result array will be completely null
                // Non empty target without any selected value may happen after slicing the parent union,
                // since only type_ids and offsets are sliced, not the children
                match (target.len().cmp(&union_array.len()), layout(target.data_type()).can_contain_null_mask) {
                    (Ordering::Less, _) // case 5.1A: our target is smaller than the parent union, allocate a new correclty sized null array
                    | (_, false) => { // case 5.1B: target array can't contain a null mask
                        Ok(new_null_columnar_value(target.data_type(), union_array.len()))
                    }
                    // case 5.2: target and parent union lengths are equal, and the target can contain a null mask, let's set it to a all-null null-buffer
                    (Ordering::Equal, true) => {
                        //SAFETY: The only change to the array data is the addition of a null mask, and if the target data type can contain a null mask was just checked above
                        let data = unsafe {
                            target
                                .into_data()
                                .into_builder()
                                .nulls(Some(NullBuffer::new_null(union_array.len())))
                                .build_unchecked()
                        };

                        Ok(ColumnarValue::Array(make_array(data)))
                    }
                    // case 5.3: target is bigger than it's parent union and can contain a null mask, let's slice it, and set it's nulls to a all-null null-buffer
                    (Ordering::Greater, true) => {
                        //SAFETY: The only change to the array data is the addition of a null mask, and if the target data type can contain a null mask was just checked above
                        let data = unsafe {
                            target
                                .into_data()
                                .slice(0, union_array.len())
                                .into_builder()
                                .nulls(Some(NullBuffer::new_null(union_array.len())))
                                .build_unchecked()
                        };

                        Ok(ColumnarValue::Array(make_array(data)))
                    }
                }
            }
            BoolValue::Buffer(selected) => {
                //case 6: some type_ids matches our target, but not all. For selected values, take the value pointed by the offset. For unselected, take a valid null
                Ok(ColumnarValue::Array(take(
                    target,
                    &Int32Array::new(offsets.clone(), Some(selected.into())),
                    None,
                )?))
            }
        }
    }
}

fn extract_dense_all_selected(
    union_array: &UnionArray,
    target: &Arc<dyn Array>,
    offsets: &ScalarBuffer<i32>,
) -> Result<ArrayRef> {
    let sequential =
        target.len() - offsets[0] as usize >= union_array.len() && is_sequential(offsets);

    if sequential && target.len() == union_array.len() {
        // case 1: all offsets are sequential and both lengths match, return the array directly
        Ok(Arc::clone(target))
    } else if sequential && target.len() > union_array.len() {
        // case 2: All offsets are sequential, but our target is bigger than our union, slice it, starting at the first offset
        Ok(target.slice(offsets[0] as usize, union_array.len()))
    } else {
        // case 3: Since offsets are not sequential, take them from the child to a new sequential and correcly sized array
        let indices = Int32Array::try_new(offsets.clone(), None)?;

        Ok(take(target, &indices, None)?)
    }
}

const EQ_SCALAR_CHUNK_SIZE: usize = 512;

#[doc(hidden)]
#[derive(Debug, PartialEq)]
pub enum BoolValue {
    Scalar(bool),
    Buffer(BooleanBuffer),
}

fn eq_scalar(type_ids: &[i8], target: i8) -> BoolValue {
    eq_scalar_generic::<EQ_SCALAR_CHUNK_SIZE>(type_ids, target)
}

// This is like MutableBuffer::collect_bool(type_ids.len(), |i| type_ids[i] == target) with fast paths for all true or all false values.
#[doc(hidden)]
pub fn eq_scalar_generic<const N: usize>(type_ids: &[i8], target: i8) -> BoolValue {
    fn count_sequence<const N: usize>(
        type_ids: &[i8],
        mut f: impl FnMut(i8) -> bool,
    ) -> usize {
        type_ids
            .chunks(N)
            .take_while(|chunk| chunk.iter().copied().fold(true, |b, v| b & f(v)))
            .map(|chunk| chunk.len())
            .sum()
    }

    let true_bits = count_sequence::<N>(type_ids, |v| v == target);

    let (set_bits, val) = if true_bits == type_ids.len() {
        return BoolValue::Scalar(true);
    } else if true_bits == 0 {
        let false_bits = count_sequence::<N>(type_ids, |v| v != target);

        if false_bits == type_ids.len() {
            return BoolValue::Scalar(false);
        } else {
            (false_bits, false)
        }
    } else {
        (true_bits, true)
    };

    // restrict to chunk boundaries
    let set_bits = set_bits - set_bits % 64;

    let mut buffer = MutableBuffer::new(bit_util::ceil(type_ids.len(), 64) * 8)
        .with_bitset(set_bits / 8, val);

    buffer.extend(type_ids[set_bits..].chunks(64).map(|chunk| {
        chunk
            .iter()
            .copied()
            .enumerate()
            .fold(0, |packed, (bit_idx, v)| {
                packed | ((v == target) as u64) << bit_idx
            })
    }));

    buffer.truncate(bit_util::ceil(type_ids.len(), 8));

    BoolValue::Buffer(BooleanBuffer::new(buffer.into(), 0, type_ids.len()))
}

const IS_SEQUENTIAL_CHUNK_SIZE: usize = 64;

fn is_sequential(offsets: &[i32]) -> bool {
    is_sequential_generic::<IS_SEQUENTIAL_CHUNK_SIZE>(offsets)
}

#[doc(hidden)]
pub fn is_sequential_generic<const N: usize>(offsets: &[i32]) -> bool {
    if offsets.is_empty() {
        return true;
    }

    // the most common form of non sequential offsets is when sequential nulls reuses the same value,
    // pointed by the same offset, while valid values offsets increases one by one
    // this also checks if the last chunk/remainder is sequential
    if offsets[0] + offsets.len() as i32 - 1 != offsets[offsets.len() - 1] {
        return false;
    }

    let chunks = offsets.chunks_exact(N);

    let remainder = chunks.remainder();

    chunks.enumerate().all(|(i, chunk)| {
        let chunk_array = <&[i32; N]>::try_from(chunk).unwrap();

        //checks if values within chunk are sequential
        chunk_array
            .iter()
            .copied()
            .enumerate()
            .fold(true, |b, (i, o)| b & (o == chunk_array[0] + i as i32))
            && offsets[0] + (i * N) as i32 == chunk_array[0] //checks if chunk is sequential relative to the first offset
    }) && remainder
        .iter()
        .copied()
        .enumerate()
        .fold(true, |b, (i, o)| b & (o == remainder[0] + i as i32)) //if the remainder is sequential relative to the first offset is checked at the start of the function
}

fn new_null_columnar_value(data_type: &DataType, len: usize) -> ColumnarValue {
    match ScalarValue::try_from(data_type) {
        Ok(null_scalar) => ColumnarValue::Scalar(null_scalar),
        Err(_) => ColumnarValue::Array(new_null_array(data_type, len)),
    }
}

#[cfg(test)]
mod tests {
    use crate::core::union_extract::{
        eq_scalar_generic, is_sequential_generic, new_null_columnar_value, BoolValue,
    };

    use std::sync::Arc;

    use arrow::array::{new_null_array, Array, Int8Array};
    use arrow::buffer::BooleanBuffer;
    use arrow::datatypes::{DataType, Field, UnionFields, UnionMode};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use super::UnionExtractFun;

    // when it becomes possible to construct union scalars in SQL, this should go to sqllogictests
    #[test]
    fn test_scalar_value() -> Result<()> {
        let fun = UnionExtractFun::new();

        let fields = UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        );

        fn assert_scalar(value: ColumnarValue, expected: ScalarValue) {
            match value {
                ColumnarValue::Array(array) => panic!("expected scalar got {array:?}"),
                ColumnarValue::Scalar(scalar) => assert_eq!(scalar, expected),
            }
        }

        let result = fun.invoke(&[
            ColumnarValue::Scalar(ScalarValue::Union(
                None,
                fields.clone(),
                UnionMode::Dense,
            )),
            ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
        ])?;

        assert_scalar(result, ScalarValue::Utf8(None));

        let result = fun.invoke(&[
            ColumnarValue::Scalar(ScalarValue::Union(
                Some((3, Box::new(ScalarValue::Int32(Some(42))))),
                fields.clone(),
                UnionMode::Dense,
            )),
            ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
        ])?;

        assert_scalar(result, ScalarValue::Utf8(None));

        let result = fun.invoke(&[
            ColumnarValue::Scalar(ScalarValue::Union(
                Some((1, Box::new(ScalarValue::new_utf8("42")))),
                fields.clone(),
                UnionMode::Dense,
            )),
            ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
        ])?;

        assert_scalar(result, ScalarValue::new_utf8("42"));

        Ok(())
    }

    #[test]
    fn test_eq_scalar() {
        //multiple all equal chunks, so it's loop and sum logic it's tested
        //multiple chunks after, so it's loop logic it's tested
        const ARRAY_LEN: usize = 64 * 4;

        //so out of 64 boundaries chunks can be generated and checked for
        const EQ_SCALAR_CHUNK_SIZE: usize = 3;

        fn eq_scalar(type_ids: &[i8], target: i8) -> BoolValue {
            eq_scalar_generic::<EQ_SCALAR_CHUNK_SIZE>(type_ids, target)
        }

        fn eq(left: &[i8], right: i8) -> BooleanBuffer {
            arrow::compute::kernels::cmp::eq(
                &Int8Array::from(left.to_vec()),
                &Int8Array::new_scalar(right),
            )
            .unwrap()
            .into_parts()
            .0
        }

        assert_eq!(eq_scalar(&[], 1), BoolValue::Scalar(true));

        assert_eq!(eq_scalar(&[1], 1), BoolValue::Scalar(true));
        assert_eq!(eq_scalar(&[2], 1), BoolValue::Scalar(false));

        let mut values = [1; ARRAY_LEN];

        assert_eq!(eq_scalar(&values, 1), BoolValue::Scalar(true));
        assert_eq!(eq_scalar(&values, 2), BoolValue::Scalar(false));

        //every subslice should return the same value
        for i in 1..ARRAY_LEN {
            assert_eq!(eq_scalar(&values[..i], 1), BoolValue::Scalar(true));
            assert_eq!(eq_scalar(&values[..i], 2), BoolValue::Scalar(false));
        }

        // test that a single change anywhere is checked for
        for i in 0..ARRAY_LEN {
            values[i] = 2;

            assert_eq!(eq_scalar(&values, 1), BoolValue::Buffer(eq(&values, 1)));
            assert_eq!(eq_scalar(&values, 2), BoolValue::Buffer(eq(&values, 2)));

            values[i] = 1;
        }
    }

    #[test]
    fn test_is_sequential() {
        /*
        the smallest value that satisfies:
        >1 so the fold logic of a exact chunk executes
        >2 so a >1 non-exact remainder can exist, and it's fold logic executes
         */
        const CHUNK_SIZE: usize = 3;
        //we test arrays of size up to 8 = 2 * CHUNK_SIZE + 2:
        //multiple(2) exact chunks, so the AND logic between them executes
        //a >1(2) remainder, so:
        //    the AND logic between all exact chunks and the remainder executes
        //    the remainder fold logic executes

        fn is_sequential(v: &[i32]) -> bool {
            is_sequential_generic::<CHUNK_SIZE>(v)
        }

        assert!(is_sequential(&[])); //empty
        assert!(is_sequential(&[1])); //single

        assert!(is_sequential(&[1, 2]));
        assert!(is_sequential(&[1, 2, 3]));
        assert!(is_sequential(&[1, 2, 3, 4]));
        assert!(is_sequential(&[1, 2, 3, 4, 5]));
        assert!(is_sequential(&[1, 2, 3, 4, 5, 6]));
        assert!(is_sequential(&[1, 2, 3, 4, 5, 6, 7]));
        assert!(is_sequential(&[1, 2, 3, 4, 5, 6, 7, 8]));

        assert!(!is_sequential(&[8, 7]));
        assert!(!is_sequential(&[8, 7, 6]));
        assert!(!is_sequential(&[8, 7, 6, 5]));
        assert!(!is_sequential(&[8, 7, 6, 5, 4]));
        assert!(!is_sequential(&[8, 7, 6, 5, 4, 3]));
        assert!(!is_sequential(&[8, 7, 6, 5, 4, 3, 2]));
        assert!(!is_sequential(&[8, 7, 6, 5, 4, 3, 2, 1]));

        assert!(!is_sequential(&[0, 2]));
        assert!(!is_sequential(&[1, 0]));

        assert!(!is_sequential(&[0, 2, 3]));
        assert!(!is_sequential(&[1, 0, 3]));
        assert!(!is_sequential(&[1, 2, 0]));

        assert!(!is_sequential(&[0, 2, 3, 4]));
        assert!(!is_sequential(&[1, 0, 3, 4]));
        assert!(!is_sequential(&[1, 2, 0, 4]));
        assert!(!is_sequential(&[1, 2, 3, 0]));

        assert!(!is_sequential(&[0, 2, 3, 4, 5]));
        assert!(!is_sequential(&[1, 0, 3, 4, 5]));
        assert!(!is_sequential(&[1, 2, 0, 4, 5]));
        assert!(!is_sequential(&[1, 2, 3, 0, 5]));
        assert!(!is_sequential(&[1, 2, 3, 4, 0]));

        assert!(!is_sequential(&[0, 2, 3, 4, 5, 6]));
        assert!(!is_sequential(&[1, 0, 3, 4, 5, 6]));
        assert!(!is_sequential(&[1, 2, 0, 4, 5, 6]));
        assert!(!is_sequential(&[1, 2, 3, 0, 5, 6]));
        assert!(!is_sequential(&[1, 2, 3, 4, 0, 6]));
        assert!(!is_sequential(&[1, 2, 3, 4, 5, 0]));

        assert!(!is_sequential(&[0, 2, 3, 4, 5, 6, 7]));
        assert!(!is_sequential(&[1, 0, 3, 4, 5, 6, 7]));
        assert!(!is_sequential(&[1, 2, 0, 4, 5, 6, 7]));
        assert!(!is_sequential(&[1, 2, 3, 0, 5, 6, 7]));
        assert!(!is_sequential(&[1, 2, 3, 4, 0, 6, 7]));
        assert!(!is_sequential(&[1, 2, 3, 4, 5, 0, 7]));
        assert!(!is_sequential(&[1, 2, 3, 4, 5, 6, 0]));

        assert!(!is_sequential(&[0, 2, 3, 4, 5, 6, 7, 8]));
        assert!(!is_sequential(&[1, 0, 3, 4, 5, 6, 7, 8]));
        assert!(!is_sequential(&[1, 2, 0, 4, 5, 6, 7, 8]));
        assert!(!is_sequential(&[1, 2, 3, 0, 5, 6, 7, 8]));
        assert!(!is_sequential(&[1, 2, 3, 4, 0, 6, 7, 8]));
        assert!(!is_sequential(&[1, 2, 3, 4, 5, 0, 7, 8]));
        assert!(!is_sequential(&[1, 2, 3, 4, 5, 6, 0, 8]));
        assert!(!is_sequential(&[1, 2, 3, 4, 5, 6, 7, 0]));
    }

    #[test]
    fn test_new_null_columnar_value() {
        match new_null_columnar_value(&DataType::Int8, 2) {
            ColumnarValue::Array(_) => {
                panic!("new_null_columnar_value should've returned a scalar for Int8")
            }
            ColumnarValue::Scalar(scalar) => assert_eq!(scalar, ScalarValue::Int8(None)),
        }

        let run_data_type = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int16, false)),
            Arc::new(Field::new("values", DataType::Utf8, false)),
        );

        match new_null_columnar_value(&run_data_type, 2) {
            ColumnarValue::Array(array) => assert_eq!(
                array.into_data(),
                new_null_array(&run_data_type, 2).into_data()
            ),
            ColumnarValue::Scalar(_) => panic!(
                "new_null_columnar_value should've returned a array for RunEndEncoded"
            ),
        }
    }
}
