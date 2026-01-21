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

//! File Compression type abstraction

use std::str::FromStr;

use datafusion_common::error::{DataFusionError, Result};

use datafusion_common::GetExt;
use datafusion_common::parsers::CompressionTypeVariant::{self, *};

#[cfg(feature = "compression")]
use async_compression::tokio::bufread::{
    BzDecoder as AsyncBzDecoder, BzEncoder as AsyncBzEncoder,
    GzipDecoder as AsyncGzDecoder, GzipEncoder as AsyncGzEncoder,
    XzDecoder as AsyncXzDecoder, XzEncoder as AsyncXzEncoder,
    ZstdDecoder as AsyncZstdDecoer, ZstdEncoder as AsyncZstdEncoder,
};

#[cfg(feature = "compression")]
use async_compression::tokio::write::{BzEncoder, GzipEncoder, XzEncoder, ZstdEncoder};
use bytes::Bytes;
#[cfg(feature = "compression")]
use bzip2::read::MultiBzDecoder;
#[cfg(feature = "compression")]
use flate2::read::MultiGzDecoder;
use futures::StreamExt;
#[cfg(feature = "compression")]
use futures::TryStreamExt;
use futures::stream::BoxStream;
#[cfg(feature = "compression")]
use liblzma::read::XzDecoder;
use object_store::buffered::BufWriter;
use tokio::io::AsyncWrite;
#[cfg(feature = "compression")]
use tokio_util::io::{ReaderStream, StreamReader};
#[cfg(feature = "compression")]
use zstd::Decoder as ZstdDecoder;

/// Readable file compression type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileCompressionType {
    variant: CompressionTypeVariant,
}

impl GetExt for FileCompressionType {
    fn get_ext(&self) -> String {
        match self.variant {
            GZIP => ".gz".to_owned(),
            BZIP2 => ".bz2".to_owned(),
            XZ => ".xz".to_owned(),
            ZSTD => ".zst".to_owned(),
            UNCOMPRESSED => "".to_owned(),
        }
    }
}

impl From<CompressionTypeVariant> for FileCompressionType {
    fn from(t: CompressionTypeVariant) -> Self {
        Self { variant: t }
    }
}

impl From<FileCompressionType> for CompressionTypeVariant {
    fn from(t: FileCompressionType) -> Self {
        t.variant
    }
}

impl FromStr for FileCompressionType {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        let variant = CompressionTypeVariant::from_str(s).map_err(|_| {
            DataFusionError::NotImplemented(format!("Unknown FileCompressionType: {s}"))
        })?;
        Ok(Self { variant })
    }
}

/// `FileCompressionType` implementation
impl FileCompressionType {
    /// Gzip-ed file
    pub const GZIP: Self = Self { variant: GZIP };

    /// Bzip2-ed file
    pub const BZIP2: Self = Self { variant: BZIP2 };

    /// Xz-ed file (liblzma)
    pub const XZ: Self = Self { variant: XZ };

    /// Zstd-ed file
    pub const ZSTD: Self = Self { variant: ZSTD };

    /// Uncompressed file
    pub const UNCOMPRESSED: Self = Self {
        variant: UNCOMPRESSED,
    };

    /// Read only access to self.variant
    pub fn get_variant(&self) -> &CompressionTypeVariant {
        &self.variant
    }

    /// The file is compressed or not
    pub const fn is_compressed(&self) -> bool {
        self.variant.is_compressed()
    }

    /// Given a `Stream`, create a `Stream` which data are compressed with `FileCompressionType`.
    pub fn convert_to_compress_stream<'a>(
        &self,
        s: BoxStream<'a, Result<Bytes>>,
    ) -> Result<BoxStream<'a, Result<Bytes>>> {
        Ok(match self.variant {
            #[cfg(feature = "compression")]
            GZIP => ReaderStream::new(AsyncGzEncoder::new(StreamReader::new(s)))
                .map_err(DataFusionError::from)
                .boxed(),
            #[cfg(feature = "compression")]
            BZIP2 => ReaderStream::new(AsyncBzEncoder::new(StreamReader::new(s)))
                .map_err(DataFusionError::from)
                .boxed(),
            #[cfg(feature = "compression")]
            XZ => ReaderStream::new(AsyncXzEncoder::new(StreamReader::new(s)))
                .map_err(DataFusionError::from)
                .boxed(),
            #[cfg(feature = "compression")]
            ZSTD => ReaderStream::new(AsyncZstdEncoder::new(StreamReader::new(s)))
                .map_err(DataFusionError::from)
                .boxed(),
            #[cfg(not(feature = "compression"))]
            GZIP | BZIP2 | XZ | ZSTD => {
                return Err(DataFusionError::NotImplemented(
                    "Compression feature is not enabled".to_owned(),
                ));
            }
            UNCOMPRESSED => s.boxed(),
        })
    }

    /// Wrap the given `BufWriter` so that it performs compressed writes
    /// according to this `FileCompressionType` using the default compression level.
    pub fn convert_async_writer(
        &self,
        w: BufWriter,
    ) -> Result<Box<dyn AsyncWrite + Send + Unpin>> {
        self.convert_async_writer_with_level(w, None)
    }

    /// Wrap the given `BufWriter` so that it performs compressed writes
    /// according to this `FileCompressionType`.
    ///
    /// If `compression_level` is `Some`, the encoder will use the specified
    /// compression level. If `None`, the default level for each algorithm is used.
    pub fn convert_async_writer_with_level(
        &self,
        w: BufWriter,
        compression_level: Option<u32>,
    ) -> Result<Box<dyn AsyncWrite + Send + Unpin>> {
        #[cfg(feature = "compression")]
        use async_compression::Level;

        Ok(match self.variant {
            #[cfg(feature = "compression")]
            GZIP => match compression_level {
                Some(level) => {
                    Box::new(GzipEncoder::with_quality(w, Level::Precise(level as i32)))
                }
                None => Box::new(GzipEncoder::new(w)),
            },
            #[cfg(feature = "compression")]
            BZIP2 => match compression_level {
                Some(level) => {
                    Box::new(BzEncoder::with_quality(w, Level::Precise(level as i32)))
                }
                None => Box::new(BzEncoder::new(w)),
            },
            #[cfg(feature = "compression")]
            XZ => match compression_level {
                Some(level) => {
                    Box::new(XzEncoder::with_quality(w, Level::Precise(level as i32)))
                }
                None => Box::new(XzEncoder::new(w)),
            },
            #[cfg(feature = "compression")]
            ZSTD => match compression_level {
                Some(level) => {
                    Box::new(ZstdEncoder::with_quality(w, Level::Precise(level as i32)))
                }
                None => Box::new(ZstdEncoder::new(w)),
            },
            #[cfg(not(feature = "compression"))]
            GZIP | BZIP2 | XZ | ZSTD => {
                // compression_level is not used when compression feature is disabled
                let _ = compression_level;
                return Err(DataFusionError::NotImplemented(
                    "Compression feature is not enabled".to_owned(),
                ));
            }
            UNCOMPRESSED => Box::new(w),
        })
    }

    /// Given a `Stream`, create a `Stream` which data are decompressed with `FileCompressionType`.
    pub fn convert_stream<'a>(
        &self,
        s: BoxStream<'a, Result<Bytes>>,
    ) -> Result<BoxStream<'a, Result<Bytes>>> {
        Ok(match self.variant {
            #[cfg(feature = "compression")]
            GZIP => {
                let mut decoder = AsyncGzDecoder::new(StreamReader::new(s));
                decoder.multiple_members(true);

                ReaderStream::new(decoder)
                    .map_err(DataFusionError::from)
                    .boxed()
            }
            #[cfg(feature = "compression")]
            BZIP2 => ReaderStream::new(AsyncBzDecoder::new(StreamReader::new(s)))
                .map_err(DataFusionError::from)
                .boxed(),
            #[cfg(feature = "compression")]
            XZ => ReaderStream::new(AsyncXzDecoder::new(StreamReader::new(s)))
                .map_err(DataFusionError::from)
                .boxed(),
            #[cfg(feature = "compression")]
            ZSTD => ReaderStream::new(AsyncZstdDecoer::new(StreamReader::new(s)))
                .map_err(DataFusionError::from)
                .boxed(),
            #[cfg(not(feature = "compression"))]
            GZIP | BZIP2 | XZ | ZSTD => {
                return Err(DataFusionError::NotImplemented(
                    "Compression feature is not enabled".to_owned(),
                ));
            }
            UNCOMPRESSED => s.boxed(),
        })
    }

    /// Given a `Read`, create a `Read` which data are decompressed with `FileCompressionType`.
    pub fn convert_read<T: std::io::Read + Send + 'static>(
        &self,
        r: T,
    ) -> Result<Box<dyn std::io::Read + Send>> {
        Ok(match self.variant {
            #[cfg(feature = "compression")]
            GZIP => Box::new(MultiGzDecoder::new(r)),
            #[cfg(feature = "compression")]
            BZIP2 => Box::new(MultiBzDecoder::new(r)),
            #[cfg(feature = "compression")]
            XZ => Box::new(XzDecoder::new_multi_decoder(r)),
            #[cfg(feature = "compression")]
            ZSTD => match ZstdDecoder::new(r) {
                Ok(decoder) => Box::new(decoder),
                Err(e) => return Err(DataFusionError::External(Box::new(e))),
            },
            #[cfg(not(feature = "compression"))]
            GZIP | BZIP2 | XZ | ZSTD => {
                return Err(DataFusionError::NotImplemented(
                    "Compression feature is not enabled".to_owned(),
                ));
            }
            UNCOMPRESSED => Box::new(r),
        })
    }
}

/// Trait for extending the functionality of the `FileType` enum.
pub trait FileTypeExt {
    /// Given a `FileCompressionType`, return the `FileType`'s extension with compression suffix
    fn get_ext_with_compression(&self, c: FileCompressionType) -> Result<String>;
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::FileCompressionType;
    use datafusion_common::error::DataFusionError;

    use bytes::Bytes;
    use futures::StreamExt;

    #[test]
    fn from_str() {
        for (ext, compression_type) in [
            ("gz", FileCompressionType::GZIP),
            ("GZ", FileCompressionType::GZIP),
            ("gzip", FileCompressionType::GZIP),
            ("GZIP", FileCompressionType::GZIP),
            ("xz", FileCompressionType::XZ),
            ("XZ", FileCompressionType::XZ),
            ("bz2", FileCompressionType::BZIP2),
            ("BZ2", FileCompressionType::BZIP2),
            ("bzip2", FileCompressionType::BZIP2),
            ("BZIP2", FileCompressionType::BZIP2),
            ("zst", FileCompressionType::ZSTD),
            ("ZST", FileCompressionType::ZSTD),
            ("zstd", FileCompressionType::ZSTD),
            ("ZSTD", FileCompressionType::ZSTD),
            ("", FileCompressionType::UNCOMPRESSED),
        ] {
            assert_eq!(
                FileCompressionType::from_str(ext).unwrap(),
                compression_type
            );
        }

        assert!(matches!(
            FileCompressionType::from_str("Unknown"),
            Err(DataFusionError::NotImplemented(_))
        ));
    }

    #[tokio::test]
    async fn test_bgzip_stream_decoding() -> Result<(), DataFusionError> {
        // As described in https://samtools.github.io/hts-specs/SAMv1.pdf ("The BGZF compression format")

        // Ignore rust formatting so the byte array is easier to read
        #[rustfmt::skip]
        let data = [
            // Block header
            0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x06, 0x00, 0x42, 0x43,
            0x02, 0x00,
            // Block 0, literal: 42
            0x1e, 0x00, 0x33, 0x31, 0xe2, 0x02, 0x00, 0x31, 0x29, 0x86, 0xd1, 0x03, 0x00, 0x00, 0x00,
            // Block header
            0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x06, 0x00, 0x42, 0x43,
            0x02, 0x00,
            // Block 1, literal: 42
            0x1e, 0x00, 0x33, 0x31, 0xe2, 0x02, 0x00, 0x31, 0x29, 0x86, 0xd1, 0x03, 0x00, 0x00, 0x00,
            // EOF
            0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x06, 0x00, 0x42, 0x43,
            0x02, 0x00, 0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        // Create a byte stream
        let stream = futures::stream::iter(vec![Ok::<Bytes, DataFusionError>(
            Bytes::from(data.to_vec()),
        )]);
        let converted_stream =
            FileCompressionType::GZIP.convert_stream(stream.boxed())?;

        let vec = converted_stream
            .map(|r| r.unwrap())
            .collect::<Vec<Bytes>>()
            .await;

        let string_value = String::from_utf8_lossy(&vec[0]);

        assert_eq!(string_value, "42\n42\n");

        Ok(())
    }
}
