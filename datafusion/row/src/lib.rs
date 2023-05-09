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

//! This module contains code to translate arrays back and forth to a
//! row based format. The row based format is backed by raw bytes
//! ([`[u8]`]) and used to optimize certain operations.
//!
//! In general, DataFusion is a so called "vectorized" execution
//! model, specifically it uses the optimized calculation kernels in
//! [`arrow`] to amortize dispatch overhead.
//!
//! However, as mentioned in [this paper], there are some "row
//! oriented" operations in a database that are not typically amenable
//! to vectorization. The "classics" are: hash table updates in joins
//! and hash aggregates, as well as comparing tuples in sort /
//! merging.
//!
//! [this paper]: https://db.in.tum.de/~kersten/vectorization_vs_compilation.pdf

use arrow::array::{make_builder, ArrayBuilder, ArrayRef};
use arrow::datatypes::Schema;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
pub use layout::row_supported;
use std::sync::Arc;

pub mod accessor;
pub mod layout;
pub mod reader;
mod validity;
pub mod writer;

/// Tell if schema contains no nullable field
pub(crate) fn schema_null_free(schema: &Schema) -> bool {
    schema.fields().iter().all(|f| !f.is_nullable())
}

/// Columnar Batch buffer that assists creating `RecordBatches`
pub struct MutableRecordBatch {
    arrays: Vec<Box<dyn ArrayBuilder>>,
    schema: Arc<Schema>,
}

impl MutableRecordBatch {
    /// new
    pub fn new(target_batch_size: usize, schema: Arc<Schema>) -> Self {
        let arrays = new_arrays(&schema, target_batch_size);
        Self { arrays, schema }
    }

    /// Finalize the batch, output and reset this buffer
    pub fn output(&mut self) -> ArrowResult<RecordBatch> {
        let result = make_batch(self.schema.clone(), self.arrays.drain(..).collect());
        result
    }

    pub fn output_as_columns(&mut self) -> Vec<ArrayRef> {
        get_columns(self.arrays.drain(..).collect())
    }
}

fn new_arrays(schema: &Schema, batch_size: usize) -> Vec<Box<dyn ArrayBuilder>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let dt = field.data_type();
            make_builder(dt, batch_size)
        })
        .collect::<Vec<_>>()
}

fn make_batch(
    schema: Arc<Schema>,
    mut arrays: Vec<Box<dyn ArrayBuilder>>,
) -> ArrowResult<RecordBatch> {
    let columns = arrays.iter_mut().map(|array| array.finish()).collect();
    RecordBatch::try_new(schema, columns)
}

fn get_columns(mut arrays: Vec<Box<dyn ArrayBuilder>>) -> Vec<ArrayRef> {
    arrays.iter_mut().map(|array| array.finish()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::RowLayout;
    use crate::reader::read_as_batch;
    use crate::writer::write_batch_unchecked;
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};
    use datafusion_common::Result;
    use DataType::*;

    macro_rules! fn_test_single_type {
        ($ARRAY: ident, $TYPE: expr, $VEC: expr) => {
            paste::item! {
                #[test]
                #[allow(non_snake_case)]
                fn [<test _single_ $TYPE>]() -> Result<()> {
                    let schema = Arc::new(Schema::new(vec![Field::new("a", $TYPE, true)]));
                    let record_width = RowLayout::new(schema.as_ref()).fixed_part_width();
                    let a = $ARRAY::from($VEC);
                    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
                    let mut vector = vec![0; record_width * batch.num_rows()];
                    let row_offsets =
                        { write_batch_unchecked(&mut vector, 0, &batch, 0, schema.clone()) };
                    let output_batch = { read_as_batch(&vector, schema, &row_offsets)? };
                    assert_eq!(batch, output_batch);
                    Ok(())
                }

                #[test]
                #[allow(non_snake_case)]
                fn [<test_single_ $TYPE _null_free>]() -> Result<()> {
                    let schema = Arc::new(Schema::new(vec![Field::new("a", $TYPE, false)]));
                    let record_width = RowLayout::new(schema.as_ref()).fixed_part_width();
                    let v = $VEC.into_iter().filter(|o| o.is_some()).collect::<Vec<_>>();
                    let a = $ARRAY::from(v);
                    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
                    let mut vector = vec![0; record_width * batch.num_rows()];
                    let row_offsets =
                        { write_batch_unchecked(&mut vector, 0, &batch, 0, schema.clone()) };
                    let output_batch = { read_as_batch(&vector, schema, &row_offsets)? };
                    assert_eq!(batch, output_batch);
                    Ok(())
                }
            }
        };
    }

    fn_test_single_type!(
        BooleanArray,
        Boolean,
        vec![Some(true), Some(false), None, Some(true), None]
    );

    fn_test_single_type!(
        Int8Array,
        Int8,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        Int16Array,
        Int16,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        Int32Array,
        Int32,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        Int64Array,
        Int64,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        UInt8Array,
        UInt8,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        UInt16Array,
        UInt16,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        UInt32Array,
        UInt32,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        UInt64Array,
        UInt64,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        Float32Array,
        Float32,
        vec![Some(5.0), Some(7.0), None, Some(0.0), Some(111.0)]
    );

    fn_test_single_type!(
        Float64Array,
        Float64,
        vec![Some(5.0), Some(7.0), None, Some(0.0), Some(111.0)]
    );

    fn_test_single_type!(
        Date32Array,
        Date32,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        Date64Array,
        Date64,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    #[test]
    fn test_single_decimal128() -> Result<()> {
        let v = vec![
            Some(0),
            Some(1),
            None,
            Some(-1),
            Some(i128::MIN),
            Some(i128::MAX),
        ];
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", Decimal128(38, 10), true)]));
        let record_width = RowLayout::new(schema.as_ref()).fixed_part_width();
        let a = Decimal128Array::from(v);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
        let mut vector = vec![0; record_width * batch.num_rows()];
        let row_offsets =
            { write_batch_unchecked(&mut vector, 0, &batch, 0, schema.clone()) };
        let output_batch = { read_as_batch(&vector, schema, &row_offsets)? };
        assert_eq!(batch, output_batch);
        Ok(())
    }

    #[test]
    fn test_single_decimal128_null_free() -> Result<()> {
        let v = vec![
            Some(0),
            Some(1),
            None,
            Some(-1),
            Some(i128::MIN),
            Some(i128::MAX),
        ];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            Decimal128(38, 10),
            false,
        )]));
        let record_width = RowLayout::new(schema.as_ref()).fixed_part_width();
        let v = v.into_iter().filter(|o| o.is_some()).collect::<Vec<_>>();
        let a = Decimal128Array::from(v);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
        let mut vector = vec![0; record_width * batch.num_rows()];
        let row_offsets =
            { write_batch_unchecked(&mut vector, 0, &batch, 0, schema.clone()) };
        let output_batch = { read_as_batch(&vector, schema, &row_offsets)? };
        assert_eq!(batch, output_batch);
        Ok(())
    }

    #[test]
    #[should_panic(expected = "not supported yet")]
    fn test_unsupported_type() {
        let a: ArrayRef = Arc::new(StringArray::from(vec!["hello", "world"]));
        let batch = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();
        let schema = batch.schema();
        let mut vector = vec![0; 1024];
        write_batch_unchecked(&mut vector, 0, &batch, 0, schema);
    }

    #[test]
    #[should_panic(expected = "not supported yet")]
    fn test_unsupported_type_write() {
        let a: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let batch = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();
        let schema = batch.schema();
        let mut vector = vec![0; 1024];
        write_batch_unchecked(&mut vector, 0, &batch, 0, schema);
    }

    #[test]
    #[should_panic(expected = "not supported yet")]
    fn test_unsupported_type_read() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", Utf8, false)]));
        let vector = vec![0; 1024];
        let row_offsets = vec![0];
        read_as_batch(&vector, schema, &row_offsets).unwrap();
    }
}
