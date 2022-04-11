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

//! An implementation of Row backed by raw bytes
//!
//! Each tuple consists of up to three parts: "`null bit set`" , "`values`"  and  "`var length data`"
//!
//! The null bit set is used for null tracking and is aligned to 1-byte. It stores
//! one bit per field.
//!
//! In the region of the values, we store the fields in the order they are defined in the schema.
//! - For fixed-length, sequential access fields, we store them directly.
//!       E.g., 4 bytes for int and 1 byte for bool.
//! - For fixed-length, update often fields, we store one 8-byte word per field.
//! - For fields of non-primitive or variable-length types,
//!       we append their actual content to the end of the var length region and
//!       store their offset relative to row base and their length, packed into an 8-byte word.
//!
//! ┌────────────────┬──────────────────────────┬───────────────────────┐        ┌───────────────────────┬────────────┐
//! │Validity Bitmask│    Fixed Width Field     │ Variable Width Field  │   ...  │     vardata area      │  padding   │
//! │ (byte aligned) │   (native type width)    │(vardata offset + len) │        │   (variable length)   │   bytes    │
//! └────────────────┴──────────────────────────┴───────────────────────┘        └───────────────────────┴────────────┘
//!
//!  For example, given the schema (Int8, Utf8, Float32, Utf8)
//!
//!  Encoding the tuple (1, "FooBar", NULL, "baz")
//!
//!  Requires 32 bytes (31 bytes payload and 1 byte padding to make each tuple 8-bytes aligned):
//!
//! ┌──────────┬──────────┬──────────────────────┬──────────────┬──────────────────────┬───────────────────────┬──────────┐
//! │0b00001011│   0x01   │0x00000016  0x00000006│  0x00000000  │0x0000001C  0x00000003│       FooBarbaz       │   0x00   │
//! └──────────┴──────────┴──────────────────────┴──────────────┴──────────────────────┴───────────────────────┴──────────┘
//! 0          1          2                     10              14                     22                     31         32
//!

use arrow::array::{make_builder, ArrayBuilder};
use arrow::datatypes::{DataType, Schema};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[cfg(feature = "jit")]
mod jit;
mod layout;
pub mod reader;
mod validity;
pub mod writer;

fn supported_type(dt: &DataType) -> bool {
    use DataType::*;
    matches!(
        dt,
        Boolean
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Int8
            | Int16
            | Int32
            | Int64
            | Float32
            | Float64
            | Date32
            | Date64
            | Utf8
            | Binary
    )
}

/// Tell if we can create raw-bytes based rows since we currently
/// has limited data type supports in the row format
pub fn row_supported(schema: &Arc<Schema>) -> bool {
    schema
        .fields()
        .iter()
        .all(|f| supported_type(f.data_type()))
}

fn var_length(dt: &DataType) -> bool {
    use DataType::*;
    matches!(dt, Utf8 | Binary)
}

/// Tell if the row is of fixed size
pub fn fixed_size(schema: &Arc<Schema>) -> bool {
    schema.fields().iter().all(|f| !var_length(f.data_type()))
}

/// Tell if schema contains no nullable field
pub fn schema_null_free(schema: &Arc<Schema>) -> bool {
    schema.fields().iter().all(|f| !f.is_nullable())
}

/// Columnar Batch buffer
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
}

fn new_arrays(schema: &Arc<Schema>, batch_size: usize) -> Vec<Box<dyn ArrayBuilder>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::file_format::parquet::ParquetFormat;
    use crate::datasource::file_format::FileFormat;
    use crate::datasource::listing::local_unpartitioned_file;
    use crate::error::Result;
    use crate::physical_plan::file_format::FileScanConfig;
    use crate::physical_plan::{collect, ExecutionPlan};
    use crate::prelude::SessionContext;
    use crate::row::reader::read_as_batch;
    use crate::row::writer::write_batch_unchecked;
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};
    use datafusion_data_access::object_store::local::LocalFileSystem;
    use datafusion_data_access::object_store::local::{
        local_object_reader, local_object_reader_stream,
    };
    use DataType::*;

    macro_rules! fn_test_single_type {
        ($ARRAY: ident, $TYPE: expr, $VEC: expr) => {
            paste::item! {
                #[test]
                #[allow(non_snake_case)]
                fn [<test_single_ $TYPE>]() -> Result<()> {
                    let schema = Arc::new(Schema::new(vec![Field::new("a", $TYPE, true)]));
                    let a = $ARRAY::from($VEC);
                    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
                    let mut vector = vec![0; 1024];
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
                    let v = $VEC.into_iter().filter(|o| o.is_some()).collect::<Vec<_>>();
                    let a = $ARRAY::from(v);
                    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
                    let mut vector = vec![0; 1024];
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

    fn_test_single_type!(
        StringArray,
        Utf8,
        vec![Some("hello"), Some("world"), None, Some(""), Some("")]
    );

    #[test]
    fn test_single_binary() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", Binary, true)]));
        let values: Vec<Option<&[u8]>> =
            vec![Some(b"one"), Some(b"two"), None, Some(b""), Some(b"three")];
        let a = BinaryArray::from_opt_vec(values);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
        let mut vector = vec![0; 8192];
        let row_offsets =
            { write_batch_unchecked(&mut vector, 0, &batch, 0, schema.clone()) };
        let output_batch = { read_as_batch(&vector, schema, &row_offsets)? };
        assert_eq!(batch, output_batch);
        Ok(())
    }

    #[test]
    fn test_single_binary_null_free() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", Binary, false)]));
        let values: Vec<&[u8]> = vec![b"one", b"two", b"", b"three"];
        let a = BinaryArray::from_vec(values);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
        let mut vector = vec![0; 8192];
        let row_offsets =
            { write_batch_unchecked(&mut vector, 0, &batch, 0, schema.clone()) };
        let output_batch = { read_as_batch(&vector, schema, &row_offsets)? };
        assert_eq!(batch, output_batch);
        Ok(())
    }

    #[tokio::test]
    async fn test_with_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let projection = Some(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let exec = get_exec("alltypes_plain.parquet", &projection, None).await?;
        let schema = exec.schema().clone();

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        let batch = &batches[0];

        let mut vector = vec![0; 20480];
        let row_offsets =
            { write_batch_unchecked(&mut vector, 0, batch, 0, schema.clone()) };
        let output_batch = { read_as_batch(&vector, schema, &row_offsets)? };
        assert_eq!(*batch, output_batch);

        Ok(())
    }

    #[test]
    #[should_panic(expected = "supported(schema)")]
    fn test_unsupported_type_write() {
        let a: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let batch = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();
        let schema = batch.schema();
        let mut vector = vec![0; 1024];
        write_batch_unchecked(&mut vector, 0, &batch, 0, schema);
    }

    #[test]
    #[should_panic(expected = "supported(schema)")]
    fn test_unsupported_type_read() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Decimal(5, 2),
            false,
        )]));
        let vector = vec![0; 1024];
        let row_offsets = vec![0];
        read_as_batch(&vector, schema, &row_offsets).unwrap();
    }

    async fn get_exec(
        file_name: &str,
        projection: &Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, file_name);
        let format = ParquetFormat::default();
        let file_schema = format
            .infer_schema(local_object_reader_stream(vec![filename.clone()]))
            .await
            .expect("Schema inference");
        let statistics = format
            .infer_stats(local_object_reader(filename.clone()), file_schema.clone())
            .await
            .expect("Stats inference");
        let file_groups = vec![vec![local_unpartitioned_file(filename.clone())]];
        let exec = format
            .create_physical_plan(
                FileScanConfig {
                    object_store: Arc::new(LocalFileSystem {}),
                    file_schema,
                    file_groups,
                    statistics,
                    projection: projection.clone(),
                    limit,
                    table_partition_cols: vec![],
                },
                &[],
            )
            .await?;
        Ok(exec)
    }
}
