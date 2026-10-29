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

//! Input-buffer bounds from the messages actually emitted by the IPC encoder.

use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Schema};
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};

/// Bound the message bodies and decoded data buffers without decoding or
/// allocating a bookkeeping container. Used only for supported recursive
/// schemas, which cannot retain dictionaries from earlier messages.
///
/// The reader's framer allocates each complete body once. Its slices may share
/// that allocation, so count bodyLength once, rather than each slice's capacity.
/// Also cover decompression Vecs and possible simultaneous alignment copies
/// for typed buffers. Byte payloads and bitmaps cannot need alignment copies.
/// The pinned codecs pre-size their Vecs to the declared decoded length. This
/// deliberately overcounts buffers which remain zero-copy. It does not cover codec state or array-object metadata.
pub(super) fn decoded_memory_bound(buffers: &[Buffer], schema: &Schema) -> Result<usize> {
    let mut cursor = Cursor {
        buffers,
        index: 0,
        offset: 0,
    };
    let mut total = 0usize;
    while cursor.remaining_len() != 0 {
        let first = u32::from_le_bytes(cursor.read::<4>()?);
        let metadata_len = if first == u32::MAX {
            u32::from_le_bytes(cursor.read::<4>()?) as usize
        } else {
            first as usize
        };
        if metadata_len == 0 {
            break;
        }
        // StreamEncoder writes each flatbuffer metadata vector contiguously;
        // its padding and body may be split over any number of output buffers.
        let message =
            arrow_ipc::root_as_message(cursor.remaining()?).map_err(|error| {
                internal_datafusion_err!("Invalid encoded IPC metadata: {error}")
            })?;
        let body_len = usize::try_from(message.bodyLength())
            .map_err(|_| internal_datafusion_err!("Invalid encoded IPC body length"))?;
        cursor.skip(metadata_len)?;
        let body = cursor;
        if message.header_as_dictionary_batch().is_some() {
            return Err(internal_datafusion_err!(
                "Recursive spill read bounds do not support dictionaries"
            ));
        }
        let record = message.header_as_record_batch();
        if let Some(record) = record {
            total = total.checked_add(body_len).ok_or_else(overflow)?;
            let data_buffers = record.buffers().ok_or_else(|| {
                internal_datafusion_err!("Encoded IPC batch has no buffers")
            })?;
            let mut index = 0;
            let mut consume = |alignment: usize| -> Result<()> {
                if index >= data_buffers.len() {
                    return Err(internal_datafusion_err!(
                        "Encoded IPC layout has too few buffers"
                    ));
                }
                let buffer = data_buffers.get(index);
                index += 1;
                let offset = usize::try_from(buffer.offset()).map_err(|_| {
                    internal_datafusion_err!("Invalid encoded IPC buffer offset")
                })?;
                let length = usize::try_from(buffer.length()).map_err(|_| {
                    internal_datafusion_err!("Invalid encoded IPC buffer length")
                })?;
                if offset.checked_add(length).is_none_or(|end| end > body_len) {
                    return Err(internal_datafusion_err!(
                        "Encoded IPC buffer exceeds its body"
                    ));
                }
                if length == 0 {
                    return Ok(());
                }
                let (decoded_len, decompressed) = if record.compression().is_some() {
                    if length < 8 {
                        return Err(internal_datafusion_err!(
                            "Encoded IPC compression prefix is truncated"
                        ));
                    }
                    let mut data = body;
                    data.skip(offset)?;
                    match i64::from_le_bytes(data.read::<8>()?) {
                        -1 => (length - 8, false),
                        value => (
                            usize::try_from(value).map_err(|_| {
                                internal_datafusion_err!(
                                    "Invalid IPC uncompressed buffer length"
                                )
                            })?,
                            true,
                        ),
                    }
                } else {
                    (length, false)
                };
                if decoded_len != 0 {
                    if decompressed {
                        // lz4_flex 0.14 read_to_end extends the pre-sized Vec;
                        // zstd 0.14 caps its allocation at this declared length.
                        let capacity = decoded_len;
                        total = total.checked_add(capacity).ok_or_else(overflow)?;
                    }
                    if alignment > 1 {
                        // Byte payloads and bitmaps never need alignment copies.
                        // Typed values can need a separate aligned Buffer while
                        // the body or decompression Vec is still alive.
                        let capacity = decoded_len
                            .checked_add(63)
                            .map(|n| n & !63)
                            .ok_or_else(overflow)?;
                        total = total.checked_add(capacity).ok_or_else(overflow)?;
                    }
                }
                Ok(())
            };
            let mut view_index = 0;
            let mut next_view = || -> Result<usize> {
                let counts = record.variadicBufferCounts().ok_or_else(|| {
                    internal_datafusion_err!("Missing IPC View buffer counts")
                })?;
                if view_index >= counts.len() {
                    return Err(internal_datafusion_err!(
                        "Missing IPC View buffer count"
                    ));
                }
                let count = usize::try_from(counts.get(view_index)).map_err(|_| {
                    internal_datafusion_err!("Invalid IPC View buffer count")
                })?;
                view_index += 1;
                Ok(count)
            };
            for field in schema.fields() {
                visit_alignments(field.data_type(), &mut consume, &mut next_view)?;
            }
            if record
                .variadicBufferCounts()
                .is_some_and(|counts| view_index != counts.len())
            {
                return Err(internal_datafusion_err!(
                    "Unexpected IPC View buffer counts"
                ));
            }
            if index != data_buffers.len() {
                return Err(internal_datafusion_err!(
                    "Encoded IPC layout has unexpected buffers"
                ));
            }
        }
        cursor.skip(body_len)?;
    }
    Ok(total)
}

/// Follow the supported IPC buffer order without allocating ArrayData or a
/// schema traversal stack. Variadic View buffers have byte alignment.
fn visit_alignments(
    data_type: &DataType,
    consume: &mut impl FnMut(usize) -> Result<()>,
    next_view: &mut impl FnMut() -> Result<usize>,
) -> Result<()> {
    if matches!(data_type, DataType::Null) {
        return Ok(());
    }
    consume(1)?;
    match data_type {
        DataType::Boolean | DataType::FixedSizeBinary(_) => consume(1),
        DataType::Utf8 | DataType::Binary => {
            consume(4)?;
            consume(1)
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            consume(8)?;
            consume(1)
        }
        DataType::Utf8View | DataType::BinaryView => {
            consume(16)?;
            for _ in 0..next_view()? {
                consume(1)?;
            }
            Ok(())
        }
        DataType::List(field) => {
            consume(4)?;
            visit_alignments(field.data_type(), consume, next_view)
        }
        DataType::LargeList(field) => {
            consume(8)?;
            visit_alignments(field.data_type(), consume, next_view)
        }
        DataType::Struct(fields) => {
            for field in fields {
                visit_alignments(field.data_type(), consume, next_view)?;
            }
            Ok(())
        }
        data_type => consume(data_type.primitive_width().ok_or_else(|| {
            internal_datafusion_err!("Unsupported recursive IPC field {data_type}")
        })?),
    }
}

fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted(
        "Spill decoded buffer estimate exceeds usize::MAX".to_string(),
    )
}

#[derive(Clone, Copy)]
struct Cursor<'a> {
    buffers: &'a [Buffer],
    index: usize,
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn normalize(&mut self) {
        while self
            .buffers
            .get(self.index)
            .is_some_and(|buffer| self.offset == buffer.len())
        {
            self.index += 1;
            self.offset = 0;
        }
    }

    fn remaining_len(&mut self) -> usize {
        self.normalize();
        self.buffers
            .get(self.index)
            .map_or(0, |buffer| buffer.len() - self.offset)
    }

    fn remaining(&mut self) -> Result<&'a [u8]> {
        self.normalize();
        self.buffers
            .get(self.index)
            .map(|buffer| &buffer[self.offset..])
            .ok_or_else(|| internal_datafusion_err!("Encoded IPC message is truncated"))
    }

    fn skip(&mut self, mut bytes: usize) -> Result<()> {
        while bytes != 0 {
            let available = self.remaining()?.len();
            let count = available.min(bytes);
            self.offset += count;
            bytes -= count;
            if self.offset == self.buffers[self.index].len() {
                self.index += 1;
                self.offset = 0;
            }
        }
        Ok(())
    }

    fn read<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut result = [0; N];
        let mut copied = 0;
        while copied < N {
            let part = self.remaining()?;
            let count = part.len().min(N - copied);
            result[copied..copied + count].copy_from_slice(&part[..count]);
            copied += count;
            self.skip(count)?;
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    fn encoded_read_bound_covers_stored_compressed_and_alignment_copies(
        #[values(
            datafusion_common::config::SpillCompression::Uncompressed,
            datafusion_common::config::SpillCompression::Lz4Frame,
            datafusion_common::config::SpillCompression::Zstd
        )]
        compression: datafusion_common::config::SpillCompression,
    ) -> Result<()> {
        use crate::spill::{
            IPCStreamEncoder, MessageFramer, get_record_batch_memory_size,
        };
        use arrow::array::{ArrayRef, AsArray, BinaryViewArray, StringViewArray};
        use arrow::record_batch::RecordBatch;
        use arrow_ipc::reader::StreamDecoder;
        use std::sync::Arc;
        let mut seed = 0x1234_5678_9abc_def0_u64;
        let random: ArrayRef =
            Arc::new(BinaryViewArray::from_iter_values((0..128).map(|_| {
                (0..63)
                    .map(|_| {
                        seed ^= seed << 13;
                        seed ^= seed >> 7;
                        seed ^= seed << 17;
                        seed as u8
                    })
                    .collect::<Vec<_>>()
            })));
        let repeat: ArrayRef = Arc::new(StringViewArray::from_iter_values(
            std::iter::repeat_n("x".repeat(63), 128),
        ));
        let input =
            RecordBatch::try_from_iter(vec![("random", random), ("repeat", repeat)])?;
        let mut encoder = IPCStreamEncoder::new(input.schema().as_ref(), compression)?;
        let (_, _, mut encoded) = encoder.encode(&input)?;
        let bound = encoder.read_memory_bound.unwrap();
        // Establish that the real codecs exercise both stored (-1) and truly
        // compressed buffers; the bound must not assume one of these cases.
        let mut cursor = Cursor {
            buffers: &encoded,
            index: 0,
            offset: 0,
        };
        let (mut stored, mut compressed) = (false, false);
        let mut prefix_locations = Vec::new();
        while cursor.remaining_len() != 0 {
            assert_eq!(u32::from_le_bytes(cursor.read::<4>()?), u32::MAX);
            let len = u32::from_le_bytes(cursor.read::<4>()?) as usize;
            let message = arrow_ipc::root_as_message(cursor.remaining()?).unwrap();
            cursor.skip(len)?;
            if let Some(record) = message.header_as_record_batch()
                && record.compression().is_some()
            {
                for buffer in record.buffers().unwrap() {
                    if buffer.length() == 0 {
                        continue;
                    }
                    let mut data = cursor;
                    data.skip(buffer.offset() as usize)?;
                    prefix_locations.push(data);
                    let prefix = i64::from_le_bytes(data.read::<8>()?);
                    stored |= prefix == -1;
                    compressed |= prefix > 0;
                }
            }
            cursor.skip(message.bodyLength() as usize)?;
        }
        if compression != datafusion_common::config::SpillCompression::Uncompressed {
            assert!(stored && compressed);
            let mut oversized = encoded.clone();
            for mut point in prefix_locations {
                for byte in i64::MAX.to_le_bytes() {
                    point.remaining()?;
                    let mut bytes = oversized[point.index].to_vec();
                    bytes[point.offset] = byte;
                    oversized[point.index] = Buffer::from(bytes);
                    point.skip(1)?;
                }
            }
            assert!(matches!(
                decoded_memory_bound(&oversized, input.schema().as_ref()),
                Err(DataFusionError::ResourcesExhausted(_))
            ));
        }
        encoded.extend(encoder.finish()?.1);
        for unaligned in [false, true] {
            let mut framer = MessageFramer::new();
            let mut decoder = StreamDecoder::new();
            let mut output = Vec::new();
            let mut body_id = None;
            for chunk in &encoded {
                let mut chunk = chunk.clone();
                while !chunk.is_empty() {
                    if let Some(framed) = framer.push(&mut chunk)? {
                        for (index, buffer) in framed.into_iter().enumerate() {
                            let mut buffer = if unaligned {
                                let mut bytes = Vec::with_capacity(buffer.len() + 1);
                                bytes.push(0);
                                bytes.extend_from_slice(buffer.as_slice());
                                let len = buffer.len();
                                let ptr = std::ptr::NonNull::new(bytes[1..].as_mut_ptr())
                                    .unwrap();
                                // SAFETY: the owner keeps all len+1 bytes alive;
                                // the immutable region starts one byte in and
                                // has exactly the framer's body_len capacity.
                                unsafe {
                                    Buffer::from_custom_allocation(
                                        ptr,
                                        len,
                                        Arc::new(bytes),
                                    )
                                }
                            } else {
                                buffer
                            };
                            if index == 1 {
                                body_id = Some(buffer.data_ptr());
                            }
                            while !buffer.is_empty() {
                                if let Some(batch) = decoder.decode(&mut buffer)? {
                                    output.push(batch);
                                }
                            }
                        }
                    }
                }
            }
            framer.finish()?;
            decoder.finish()?;
            assert_eq!(output, vec![input.clone()]);
            assert!(get_record_batch_memory_size(&output[0]) <= bound);
            if unaligned {
                let views = output[0].column(0).as_binary_view().views().inner();
                assert_ne!(
                    Some(views.data_ptr()),
                    body_id,
                    "force the alignment-copy path"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn cursor_handles_empty_buffers_and_fragmented_prefixes() {
        let buffers = [
            Buffer::from([]),
            Buffer::from([1, 2]),
            Buffer::from([]),
            Buffer::from([3]),
            Buffer::from([4, 5]),
            Buffer::from([]),
        ];
        let mut cursor = Cursor {
            buffers: &buffers,
            index: 0,
            offset: 0,
        };
        assert_eq!(cursor.read::<4>().unwrap(), [1, 2, 3, 4]);
        assert_eq!(cursor.read::<1>().unwrap(), [5]);
        assert_eq!(cursor.remaining_len(), 0);
        assert!(cursor.read::<1>().is_err());
    }
}
