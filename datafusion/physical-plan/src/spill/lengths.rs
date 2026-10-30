// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Spills byte arrays as the lengths of their values.
//!
//! The offsets of a `Utf8` array are a running total that zstd and lz4 barely
//! compress, while the lengths of similar values repeat, see
//! <https://github.com/apache/datafusion/issues/26627>. Compressed spill files
//! therefore store each top level `Utf8`, `LargeUtf8`, `Binary` and
//! `LargeBinary` column as a struct of its lengths, which keep its nulls, and
//! of all its value bytes as a single run:
//!
//! ```text
//! Utf8 ["ab", null, "cde"]
//!
//! Struct {
//!     lengths: Int32 [2, null, 3]
//!     values:  RunEndEncoded { run_ends: [3], values: Binary ["abcde"] }
//! }
//! ```
//!
//! Both are valid arrays, so the IPC reader decodes them as usual before
//! [`decode`] rebuilds the offsets with a running sum.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, GenericBinaryArray, GenericByteArray,
    Int64Array, OffsetSizeTrait, PrimitiveArray, RecordBatch, RunArray, StructArray,
    new_empty_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{
    ArrowNativeType, BinaryType, ByteArrayType, DataType, Field, Fields,
    GenericBinaryType, Int32Type, Int64Type, LargeBinaryType, LargeUtf8Type, Schema,
    SchemaRef, Utf8Type,
};
use datafusion_common::{Result, internal_err};

/// Returns the schema that `schema` is spilled as, or `None` if it has no byte
/// array columns
pub(super) fn encode_schema(schema: &Schema) -> Option<SchemaRef> {
    if !schema
        .fields()
        .iter()
        .any(|field| encoded_type(field.data_type()).is_some())
    {
        return None;
    }
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| match encoded_type(field.data_type()) {
            Some(data_type) => {
                Arc::new(Field::new(field.name(), data_type, field.is_nullable()))
            }
            None => Arc::clone(field),
        })
        .collect();
    Some(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

/// Encodes the byte array columns of `batch`, see the [module docs](self)
pub(super) fn encode(batch: &RecordBatch) -> Result<RecordBatch> {
    let Some(schema) = encode_schema(batch.schema_ref()) else {
        return Ok(batch.clone());
    };
    let columns = batch
        .columns()
        .iter()
        .map(encode_array)
        .collect::<Result<_>>()?;
    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Restores a batch that [`encode`] wrote for `schema`
pub(super) fn decode(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| decode_array(field.data_type(), column))
        .collect::<Result<_>>()?;
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

fn encoded_type(data_type: &DataType) -> Option<DataType> {
    let fields = match data_type {
        DataType::Utf8 | DataType::Binary => encoded_fields::<Int32Type>(),
        DataType::LargeUtf8 | DataType::LargeBinary => encoded_fields::<Int64Type>(),
        _ => return None,
    };
    Some(DataType::Struct(fields))
}

/// The fields of the struct a byte array with `L` offsets is spilled as
fn encoded_fields<L>() -> Fields
where
    L: ArrowPrimitiveType,
    L::Native: OffsetSizeTrait,
{
    let run_ends = Field::new("run_ends", DataType::Int64, false);
    let bytes = Field::new("values", GenericBinaryType::<L::Native>::DATA_TYPE, true);
    let values = DataType::RunEndEncoded(Arc::new(run_ends), Arc::new(bytes));
    Fields::from(vec![
        Field::new("lengths", L::DATA_TYPE, true),
        Field::new("values", values, false),
    ])
}

fn encode_array(array: &ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Utf8 => encode_bytes::<_, Int32Type>(array.as_string::<i32>()),
        DataType::LargeUtf8 => encode_bytes::<_, Int64Type>(array.as_string::<i64>()),
        DataType::Binary => encode_bytes::<_, Int32Type>(array.as_binary::<i32>()),
        DataType::LargeBinary => encode_bytes::<_, Int64Type>(array.as_binary::<i64>()),
        _ => Ok(Arc::clone(array)),
    }
}

fn decode_array(data_type: &DataType, array: &ArrayRef) -> Result<ArrayRef> {
    match data_type {
        DataType::Utf8 => decode_bytes::<Utf8Type, Int32Type>(array),
        DataType::LargeUtf8 => decode_bytes::<LargeUtf8Type, Int64Type>(array),
        DataType::Binary => decode_bytes::<BinaryType, Int32Type>(array),
        DataType::LargeBinary => decode_bytes::<LargeBinaryType, Int64Type>(array),
        _ => Ok(Arc::clone(array)),
    }
}

fn encode_bytes<T, L>(array: &GenericByteArray<T>) -> Result<ArrayRef>
where
    T: ByteArrayType,
    L: ArrowPrimitiveType<Native = T::Offset>,
{
    let fields = encoded_fields::<L>();
    if array.is_empty() {
        return Ok(new_empty_array(&DataType::Struct(fields)));
    }
    let offsets = array.offsets();
    let lengths = offsets.windows(2).map(|w| w[1] - w[0]).collect();
    let lengths = PrimitiveArray::<L>::new(lengths, array.nulls().cloned());

    let start = offsets[0].as_usize();
    let len = offsets[array.len()].as_usize() - start;
    let bytes = GenericBinaryArray::<T::Offset>::new(
        OffsetBuffer::from_lengths([len]),
        array.values().slice_with_length(start, len),
        None,
    );
    let run_ends = Int64Array::from(vec![array.len() as i64]);
    let values = RunArray::try_new(&run_ends, &bytes)?;

    let array =
        StructArray::try_new(fields, vec![Arc::new(lengths), Arc::new(values)], None)?;
    Ok(Arc::new(array))
}

fn decode_bytes<T, L>(array: &ArrayRef) -> Result<ArrayRef>
where
    T: ByteArrayType,
    L: ArrowPrimitiveType<Native = T::Offset>,
{
    let array = array.as_struct();
    let lengths = array.column(0).as_primitive::<L>();
    let mut end = T::Offset::usize_as(0);
    let mut offsets = Vec::with_capacity(lengths.len() + 1);
    offsets.push(end);
    offsets.extend(lengths.values().iter().map(|&len| {
        end += len;
        end
    }));

    let bytes = array.column(1).as_run::<Int64Type>().values();
    let bytes = bytes.as_binary::<T::Offset>();
    let bytes_offsets = bytes.value_offsets();
    let start = bytes_offsets[0].as_usize();
    let len = bytes_offsets[bytes_offsets.len() - 1].as_usize() - start;
    if end.as_usize() != len {
        return internal_err!(
            "Spilled {} lengths add up to {}, but there are {len} value bytes",
            T::DATA_TYPE,
            end.as_usize()
        );
    }

    // SAFETY: `encode_bytes` took the lengths and value bytes from a valid
    // array of type `T`, so their running sum gives back its offsets
    let array = unsafe {
        GenericByteArray::<T>::new_unchecked(
            OffsetBuffer::new_unchecked(offsets.into()),
            bytes.values().slice_with_length(start, len),
            lengths.nulls().cloned(),
        )
    };
    Ok(Arc::new(array))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::collect;
    use crate::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
    use crate::spill::spill_manager::SpillManager;
    use arrow::array::{Int32Array, ListArray, StringArray};
    use arrow::compute::cast;
    use arrow_ipc::CompressionType;
    use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
    use datafusion_common::config::SpillCompression;
    use datafusion_execution::runtime_env::RuntimeEnv;

    fn spill_manager(schema: SchemaRef, compression: SpillCompression) -> SpillManager {
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        SpillManager::new(env, metrics, schema).with_compression_type(compression)
    }

    /// Byte arrays read back as written, sliced, empty or with nulls, next to
    /// the columns that are spilled as they are
    #[tokio::test]
    async fn test_spill_lengths_round_trip() -> Result<()> {
        let utf8: ArrayRef = Arc::new(StringArray::from_iter(
            (0..1000).map(|i| (i % 7 != 0).then(|| "x".repeat(i % 5))),
        ));
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::from_lengths([1; 1000]),
            Arc::clone(&utf8),
            None,
        );
        let batch = RecordBatch::try_from_iter(vec![
            (
                "int32",
                Arc::new(Int32Array::from_iter_values(0..1000)) as _,
            ),
            ("large_utf8", cast(&utf8, &DataType::LargeUtf8)?),
            ("binary", cast(&utf8, &DataType::Binary)?),
            ("large_binary", cast(&utf8, &DataType::LargeBinary)?),
            ("utf8_view", cast(&utf8, &DataType::Utf8View)?),
            ("list", Arc::new(list)),
            ("utf8", utf8),
        ])?;
        let batches = [
            batch.clone(),
            batch.slice(3, 500),
            batch.slice(998, 2),
            batch.slice(0, 0),
        ];
        for compression in [
            SpillCompression::Uncompressed,
            SpillCompression::Lz4Frame,
            SpillCompression::Zstd,
        ] {
            let spill_manager = spill_manager(batch.schema(), compression);
            let spill_file = spill_manager
                .spill_record_batch_and_finish(&batches, "Test")?
                .unwrap();
            let stream = spill_manager.read_spill_as_stream(spill_file, None)?;
            assert_eq!(collect(stream).await?, batches, "{compression}");
        }
        Ok(())
    }

    /// The lengths of similar values compress to almost nothing, unlike their
    /// offsets
    #[test]
    fn test_spill_lengths_compress() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let values = (0..8192).map(|i| format!("item-{}", i / 64));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from_iter_values(values))],
        )?;

        let spill_manager = spill_manager(Arc::clone(&schema), SpillCompression::Zstd);
        spill_manager
            .spill_record_batch_and_finish(std::slice::from_ref(&batch), "Test")?;
        let spilled = spill_manager.metrics.spilled_bytes.value();

        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::ZSTD))?;
        let mut ipc = vec![];
        let mut writer = StreamWriter::try_new_with_options(&mut ipc, &schema, options)?;
        writer.write(&batch)?;
        writer.finish()?;
        drop(writer);
        assert!(
            spilled * 4 < ipc.len(),
            "spilled {spilled} bytes, against {} bytes of IPC",
            ipc.len()
        );
        Ok(())
    }
}
