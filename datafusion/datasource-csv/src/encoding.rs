use std::fmt::Debug;

use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use datafusion_common::Result;
use datafusion_datasource::decoder::Decoder;
use encoding_rs::{CoderResult, Encoding};

use self::buffer::Buffer;

/// Default capacity of the buffer used to decode non-UTF-8 charset streams
static DECODE_BUFFER_CAP: usize = 8 * 1024;

/// A `Decoder` that decodes input bytes from the specified character encoding
/// to UTF-8 before passing them onto the inner `Decoder`.
pub struct CharsetDecoder<T> {
    inner: T,
    charset_decoder: encoding_rs::Decoder,
    buffer: Buffer,
}

impl<T> CharsetDecoder<T> {
    pub fn new(inner: T, encoding: &'static Encoding) -> Self {
        Self {
            inner,
            charset_decoder: encoding.new_decoder(),
            buffer: Buffer::with_capacity(DECODE_BUFFER_CAP),
        }
    }
}

impl<T: Decoder> Decoder for CharsetDecoder<T> {
    fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        let last = buf.is_empty();
        let mut buf_offset = 0;

        if !self.buffer.is_empty() {
            let decoded = self.inner.decode(self.buffer.read_buf())?;
            self.buffer.consume(decoded);

            if decoded == 0 {
                return Ok(buf_offset);
            }
        }

        loop {
            self.buffer.backshift();

            let (res, read, written, _) = self.charset_decoder.decode_to_utf8(
                &buf[buf_offset..],
                self.buffer.write_buf(),
                last,
            );
            buf_offset += read;
            self.buffer.advance(written);

            let decoded = self.inner.decode(self.buffer.read_buf())?;
            self.buffer.consume(decoded);

            if res == CoderResult::InputEmpty || decoded == 0 {
                break;
            }
        }

        Ok(buf_offset)
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        self.inner.flush()
    }

    fn can_flush_early(&self) -> bool {
        self.inner.can_flush_early()
    }
}

impl<T: Debug> Debug for CharsetDecoder<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CharsetDecoder")
            .field("inner", &self.inner)
            .field("charset_decoder", self.charset_decoder.encoding())
            .finish()
    }
}

mod buffer {
    /// A fixed-sized buffer that maintains both
    /// a read position and a write position
    #[derive(Debug)]
    pub struct Buffer {
        buf: Box<[u8]>,
        read_ptr: usize,
        write_ptr: usize,
    }

    impl Buffer {
        /// Creates a new `Buffer` with the specified capacity
        #[inline]
        pub fn with_capacity(capacity: usize) -> Self {
            Self {
                buf: vec![0; capacity].into_boxed_slice(),
                read_ptr: 0,
                write_ptr: 0,
            }
        }

        /// Whether there are no more bytes available to be read
        pub fn is_empty(&self) -> bool {
            self.read_ptr == self.write_ptr
        }

        /// Returns the unread portion of the buffer
        pub fn read_buf(&self) -> &[u8] {
            &self.buf[self.read_ptr..self.write_ptr]
        }

        /// Advances the read position by `amount` bytes
        pub fn consume(&mut self, amount: usize) {
            self.read_ptr += amount;
            debug_assert!(self.read_ptr <= self.write_ptr);
        }

        /// Returns the portion of the buffer available for writing
        pub fn write_buf(&mut self) -> &mut [u8] {
            &mut self.buf[self.write_ptr..]
        }

        /// Advances the write position by `amount` bytes
        pub fn advance(&mut self, amount: usize) {
            self.write_ptr += amount;
            debug_assert!(self.write_ptr <= self.buf.len())
        }

        /// Moves any unread bytes to the start of the buffer,
        /// creating more space for writing new data
        pub fn backshift(&mut self) {
            self.buf.copy_within(self.read_ptr..self.write_ptr, 0);
            self.write_ptr -= self.read_ptr;
            self.read_ptr = 0;
        }
    }
}
