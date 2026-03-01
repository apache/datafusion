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

//! Module containing helper methods for the various file formats
//! See write.rs for write related helper methods

use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use bytes::Buf;
use bytes::Bytes;
use datafusion_common::Result;
use futures::StreamExt as _;
use futures::stream::BoxStream;
use futures::{Stream, ready};
use std::collections::VecDeque;
use std::fmt;
use std::io::BufRead;
use std::task::Poll;

/// Trait defining a scheme for deserializing byte streams into structured data.
/// Implementors of this trait are responsible for converting raw bytes into
/// `RecordBatch` objects.
pub trait BatchDeserializer<T>: Send + fmt::Debug {
    /// Feeds a message for deserialization, updating the internal state of
    /// this `BatchDeserializer`. Note that one can call this function multiple
    /// times before calling `next`, which will queue multiple messages for
    /// deserialization.
    fn digest(&mut self, message: T);

    /// Attempts to deserialize any pending messages and returns a
    /// `DeserializerOutput` to indicate progress.
    fn next(&mut self) -> Result<DeserializerOutput, ArrowError>;

    /// Informs the deserializer that no more messages will be provided for
    /// deserialization.
    fn finish(&mut self);
}

/// Possible outputs of a [`BatchDeserializer`].
#[derive(Debug, PartialEq)]
pub enum DeserializerOutput {
    /// A successfully deserialized [`RecordBatch`].
    RecordBatch(RecordBatch),
    /// The deserializer requires more data to make progress.
    RequiresMoreData,
    /// The input data has been exhausted.
    InputExhausted,
}

/// A general interface for decoders such as [`arrow::json::reader::Decoder`] and
/// [`arrow::csv::reader::Decoder`]. Defines an interface similar to
/// [`Decoder::decode`] and [`Decoder::flush`] methods, but also includes
/// a method to check if the decoder can flush early. Intended to be used in
/// conjunction with [`DecoderDeserializer`].
///
/// [`arrow::json::reader::Decoder`]: ::arrow::json::reader::Decoder
/// [`arrow::csv::reader::Decoder`]: ::arrow::csv::reader::Decoder
/// [`Decoder::decode`]: ::arrow::json::reader::Decoder::decode
/// [`Decoder::flush`]: ::arrow::json::reader::Decoder::flush
pub trait Decoder: Send + fmt::Debug {
    /// See [`arrow::json::reader::Decoder::decode`].
    ///
    /// [`arrow::json::reader::Decoder::decode`]: ::arrow::json::reader::Decoder::decode
    fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError>;

    /// See [`arrow::json::reader::Decoder::flush`].
    ///
    /// [`arrow::json::reader::Decoder::flush`]: ::arrow::json::reader::Decoder::flush
    fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError>;

    /// Whether the decoder can flush early in its current state.
    fn can_flush_early(&self) -> bool;
}

/// A generic, decoder-based deserialization scheme for processing encoded data.
///
/// This struct is responsible for converting a stream of bytes, which represent
/// encoded data, into a stream of `RecordBatch` objects, following the specified
/// schema and formatting options. It also handles any buffering necessary to satisfy
/// the `Decoder` interface.
pub struct DecoderDeserializer<T: Decoder> {
    /// The underlying decoder used for deserialization
    pub(crate) decoder: T,
    /// The buffer used to store the remaining bytes to be decoded
    pub(crate) buffered_queue: VecDeque<Bytes>,
    /// Whether the input stream has been fully consumed
    pub(crate) finalized: bool,
}

impl<T: Decoder> DecoderDeserializer<T> {
    /// Creates a new `DecoderDeserializer` with the provided decoder.
    pub fn new(decoder: T) -> Self {
        DecoderDeserializer {
            decoder,
            buffered_queue: VecDeque::new(),
            finalized: false,
        }
    }
}

impl<T: Decoder> BatchDeserializer<Bytes> for DecoderDeserializer<T> {
    fn digest(&mut self, message: Bytes) {
        if !message.is_empty() {
            self.buffered_queue.push_back(message);
        }
    }

    fn next(&mut self) -> Result<DeserializerOutput, ArrowError> {
        while let Some(buffered) = self.buffered_queue.front_mut() {
            let decoded = self.decoder.decode(buffered)?;
            buffered.advance(decoded);

            if buffered.is_empty() {
                self.buffered_queue.pop_front();
            }

            // Flush when the stream ends or batch size is reached
            // Certain implementations can flush early
            if decoded == 0 || self.decoder.can_flush_early() {
                return match self.decoder.flush() {
                    Ok(Some(batch)) => Ok(DeserializerOutput::RecordBatch(batch)),
                    Ok(None) => continue,
                    Err(e) => Err(e),
                };
            }
        }
        if self.finalized {
            Ok(DeserializerOutput::InputExhausted)
        } else {
            Ok(DeserializerOutput::RequiresMoreData)
        }
    }

    fn finish(&mut self) {
        self.finalized = true;
        // Ensure the decoder is flushed:
        self.buffered_queue.push_back(Bytes::new());
    }
}

impl<T: Decoder> fmt::Debug for DecoderDeserializer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Deserializer")
            .field("buffered_queue", &self.buffered_queue)
            .field("finalized", &self.finalized)
            .finish()
    }
}

/// Deserializes a stream of bytes into a stream of [`RecordBatch`] objects using the
/// provided deserializer.
///
/// Returns a boxed stream of `Result<RecordBatch, ArrowError>`. The stream yields [`RecordBatch`]
/// objects as they are produced by the deserializer, or an [`ArrowError`] if an error
/// occurs while polling the input or deserializing.
pub fn deserialize_stream<'a>(
    mut input: impl Stream<Item = Result<Bytes>> + Unpin + Send + 'a,
    mut deserializer: impl BatchDeserializer<Bytes> + 'a,
) -> BoxStream<'a, Result<RecordBatch, ArrowError>> {
    futures::stream::poll_fn(move |cx| {
        loop {
            match ready!(input.poll_next_unpin(cx)).transpose()? {
                Some(b) => _ = deserializer.digest(b),
                None => deserializer.finish(),
            };

            return match deserializer.next()? {
                DeserializerOutput::RecordBatch(rb) => Poll::Ready(Some(Ok(rb))),
                DeserializerOutput::InputExhausted => Poll::Ready(None),
                DeserializerOutput::RequiresMoreData => continue,
            };
        }
    })
    .boxed()
}

/// Creates an iterator of [`RecordBatch`]es that consumes bytes from an inner [`BufRead`]
/// and deserializes them using the provided decoder.
pub fn deserialize_reader<'a>(
    mut reader: impl BufRead + Send + 'a,
    mut decoder: impl Decoder + Send + 'a,
) -> Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>> + Send + 'a> {
    let mut read = move || {
        loop {
            let buf = reader.fill_buf()?;

            let decoded = decoder.decode(buf)?;
            reader.consume(decoded);

            if decoded == 0 || decoder.can_flush_early() {
                break;
            }
        }

        decoder.flush()
    };

    Box::new(std::iter::from_fn(move || read().transpose()))
}
