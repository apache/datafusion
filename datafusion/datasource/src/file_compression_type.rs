// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
        w: impl AsyncWrite + Send + Unpin + 'static,
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
        w: impl AsyncWrite + Send + Unpin + 'static,
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
    ///
    /// Input made of several concatenated members (streams or frames) is
    /// decoded in full, the same as by [`Self::convert_read`].
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
            BZIP2 => {
                let mut decoder = AsyncBzDecoder::new(StreamReader::new(s));
                decoder.multiple_members(true);

                ReaderStream::new(decoder)
                    .map_err(DataFusionError::from)
                    .boxed()
            }
            #[cfg(feature = "compression")]
            XZ => {
                let mut decoder = AsyncXzDecoder::new(StreamReader::new(s));
                decoder.multiple_members(true);

                ReaderStream::new(decoder)
                    .map_err(DataFusionError::from)
                    .boxed()
            }
            #[cfg(feature = "compression")]
            ZSTD => {
                let mut decoder = AsyncZstdDecoer::new(StreamReader::new(s));
                decoder.multiple_members(true);

                ReaderStream::new(decoder)
                    .map_err(DataFusionError::from)
                    .boxed()
            }
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

    #[cfg(feature = "compression")]
    const CODECS: [FileCompressionType; 4] = [
        FileCompressionType::GZIP,
        FileCompressionType::BZIP2,
        FileCompressionType::XZ,
        FileCompressionType::ZSTD,
    ];

    /// Compresses `data` as one member.
    #[cfg(feature = "compression")]
    async fn compress(
        codec: FileCompressionType,
        data: &[u8],
    ) -> Result<Vec<u8>, DataFusionError> {
        use futures::TryStreamExt;

        let input = futures::stream::iter([Ok(Bytes::copy_from_slice(data))]);
        let output: Vec<Bytes> = codec
            .convert_to_compress_stream(input.boxed())?
            .try_collect()
            .await?;
        Ok(output.concat())
    }

    #[cfg(feature = "compression")]
    fn decode_read(
        codec: FileCompressionType,
        file: &[u8],
    ) -> Result<Vec<u8>, DataFusionError> {
        use std::io::Read;

        let mut decoded = Vec::new();
        codec
            .convert_read(std::io::Cursor::new(file.to_vec()))?
            .read_to_end(&mut decoded)?;
        Ok(decoded)
    }

    #[cfg(feature = "compression")]
    async fn decode_stream(
        codec: FileCompressionType,
        chunks: Vec<Vec<u8>>,
    ) -> Result<Vec<u8>, DataFusionError> {
        use futures::TryStreamExt;

        let input = futures::stream::iter(chunks.into_iter().map(|c| Ok(Bytes::from(c))));
        let output: Vec<Bytes> =
            codec.convert_stream(input.boxed())?.try_collect().await?;
        Ok(output.concat())
    }

    /// A file of concatenated members, such as `cat a.zst b.zst` produces,
    /// decodes to every member through both `convert_read` and
    /// `convert_stream`, wherever the stream's chunks split the members.
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn concatenated_members_decode_in_full() -> Result<(), DataFusionError> {
        let lines = |range: std::ops::Range<u32>| -> Vec<u8> {
            range.flat_map(|i| format!("{i}\n").into_bytes()).collect()
        };
        let members = [lines(0..5_000), vec![], lines(5_000..7_000)];
        let expected = members.concat();

        let mut wrong = Vec::new();
        for codec in CODECS {
            let mut compressed = Vec::new();
            for member in &members {
                compressed.push(compress(codec, member).await?);
            }
            let file = compressed.concat();

            let read = decode_read(codec, &file)?;
            if read != expected {
                wrong.push(format!("{codec:?} convert_read: {} bytes", read.len()));
            }

            let mut chunkings = vec![
                ("one chunk", vec![file.clone()]),
                ("one chunk per member", compressed),
            ];
            for size in [1, 13] {
                let chunks = file.chunks(size).map(<[u8]>::to_vec).collect();
                chunkings.push(("fixed-size chunks", chunks));
            }
            for (chunking, chunks) in chunkings {
                let chunk_count = chunks.len();
                let streamed = decode_stream(codec, chunks).await?;
                if streamed != expected {
                    wrong.push(format!(
                        "{codec:?} convert_stream, {chunking} ({chunk_count}): {} bytes",
                        streamed.len()
                    ));
                }
            }
        }
        assert!(
            wrong.is_empty(),
            "expected {} bytes, decoded: {wrong:#?}",
            expected.len()
        );

        Ok(())
    }

    /// Bytes after the last member that do not start another member are an
    /// error through both `convert_read` and `convert_stream`.
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn bytes_after_the_last_member_are_an_error() -> Result<(), DataFusionError> {
        let mut wrong = Vec::new();
        for codec in CODECS {
            let file = [compress(codec, b"a\n").await?, b"garbage".to_vec()].concat();
            if let Ok(decoded) = decode_read(codec, &file) {
                wrong.push(format!("{codec:?} convert_read: {decoded:?}"));
            }
            if let Ok(decoded) = decode_stream(codec, vec![file]).await {
                wrong.push(format!("{codec:?} convert_stream: {decoded:?}"));
            }
        }
        assert!(wrong.is_empty(), "decoded without an error: {wrong:#?}");

        Ok(())
    }
}
