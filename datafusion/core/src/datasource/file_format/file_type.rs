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

//! File type abstraction

use crate::error::{DataFusionError, Result};

use crate::datasource::file_format::avro::DEFAULT_AVRO_EXTENSION;
use crate::datasource::file_format::csv::DEFAULT_CSV_EXTENSION;
use crate::datasource::file_format::json::DEFAULT_JSON_EXTENSION;
use crate::datasource::file_format::parquet::DEFAULT_PARQUET_EXTENSION;
#[cfg(feature = "compression")]
use async_compression::tokio::bufread::{
    BzDecoder as AsyncBzDecoder, GzipDecoder as AsyncGzDecoder,
    XzDecoder as AsyncXzDecoder,
};
use bytes::Bytes;
#[cfg(feature = "compression")]
use bzip2::read::BzDecoder;
use datafusion_common::parsers::CompressionTypeVariant;
#[cfg(feature = "compression")]
use flate2::read::GzDecoder;
use futures::Stream;
#[cfg(feature = "compression")]
use futures::TryStreamExt;
use std::str::FromStr;
#[cfg(feature = "compression")]
use tokio_util::io::{ReaderStream, StreamReader};
#[cfg(feature = "compression")]
use xz2::read::XzDecoder;
use CompressionTypeVariant::*;

/// Define each `FileType`/`FileCompressionType`'s extension
pub trait GetExt {
    /// File extension getter
    fn get_ext(&self) -> String;
}

/// Readable file compression type
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileCompressionType {
    variant: CompressionTypeVariant,
}

impl GetExt for FileCompressionType {
    fn get_ext(&self) -> String {
        match self.variant {
            GZIP => ".gz".to_owned(),
            BZIP2 => ".bz2".to_owned(),
            XZ => ".xz".to_owned(),
            UNCOMPRESSED => "".to_owned(),
        }
    }
}

impl From<CompressionTypeVariant> for FileCompressionType {
    fn from(t: CompressionTypeVariant) -> Self {
        Self { variant: t }
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

    /// Uncompressed file
    pub const UNCOMPRESSED: Self = Self {
        variant: UNCOMPRESSED,
    };

    /// The file is compressed or not
    pub const fn is_compressed(&self) -> bool {
        self.variant.is_compressed()
    }

    /// Given a `Stream`, create a `Stream` which data are decompressed with `FileCompressionType`.
    pub fn convert_stream<T: Stream<Item = Result<Bytes>> + Unpin + Send + 'static>(
        &self,
        s: T,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes>> + Send + Unpin>> {
        #[cfg(feature = "compression")]
        let err_converter = |e: std::io::Error| match e
            .get_ref()
            .and_then(|e| e.downcast_ref::<DataFusionError>())
        {
            Some(_) => {
                *(e.into_inner()
                    .unwrap()
                    .downcast::<DataFusionError>()
                    .unwrap())
            }
            None => Into::<DataFusionError>::into(e),
        };

        Ok(match self.variant {
            #[cfg(feature = "compression")]
            GZIP => Box::new(
                ReaderStream::new(AsyncGzDecoder::new(StreamReader::new(s)))
                    .map_err(err_converter),
            ),
            #[cfg(feature = "compression")]
            BZIP2 => Box::new(
                ReaderStream::new(AsyncBzDecoder::new(StreamReader::new(s)))
                    .map_err(err_converter),
            ),
            #[cfg(feature = "compression")]
            XZ => Box::new(
                ReaderStream::new(AsyncXzDecoder::new(StreamReader::new(s)))
                    .map_err(err_converter),
            ),
            #[cfg(not(feature = "compression"))]
            GZIP | BZIP2 | XZ => {
                return Err(DataFusionError::NotImplemented(
                    "Compression feature is not enabled".to_owned(),
                ))
            }
            UNCOMPRESSED => Box::new(s),
        })
    }

    /// Given a `Read`, create a `Read` which data are decompressed with `FileCompressionType`.
    pub fn convert_read<T: std::io::Read + Send + 'static>(
        &self,
        r: T,
    ) -> Result<Box<dyn std::io::Read + Send>> {
        Ok(match self.variant {
            #[cfg(feature = "compression")]
            GZIP => Box::new(GzDecoder::new(r)),
            #[cfg(feature = "compression")]
            BZIP2 => Box::new(BzDecoder::new(r)),
            #[cfg(feature = "compression")]
            XZ => Box::new(XzDecoder::new(r)),
            #[cfg(not(feature = "compression"))]
            GZIP | BZIP2 | XZ => {
                return Err(DataFusionError::NotImplemented(
                    "Compression feature is not enabled".to_owned(),
                ))
            }
            UNCOMPRESSED => Box::new(r),
        })
    }
}

/// Readable file type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileType {
    /// Apache Avro file
    AVRO,
    /// Apache Parquet file
    PARQUET,
    /// CSV file
    CSV,
    /// JSON file
    JSON,
}

impl GetExt for FileType {
    fn get_ext(&self) -> String {
        match self {
            FileType::AVRO => DEFAULT_AVRO_EXTENSION.to_owned(),
            FileType::PARQUET => DEFAULT_PARQUET_EXTENSION.to_owned(),
            FileType::CSV => DEFAULT_CSV_EXTENSION.to_owned(),
            FileType::JSON => DEFAULT_JSON_EXTENSION.to_owned(),
        }
    }
}

impl FromStr for FileType {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.to_uppercase();
        match s.as_str() {
            "AVRO" => Ok(FileType::AVRO),
            "PARQUET" => Ok(FileType::PARQUET),
            "CSV" => Ok(FileType::CSV),
            "JSON" | "NDJSON" => Ok(FileType::JSON),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unknown FileType: {s}"
            ))),
        }
    }
}

impl FileType {
    /// Given a `FileCompressionType`, return the `FileType`'s extension with compression suffix
    pub fn get_ext_with_compression(&self, c: FileCompressionType) -> Result<String> {
        let ext = self.get_ext();

        match self {
            FileType::JSON | FileType::CSV => Ok(format!("{}{}", ext, c.get_ext())),
            FileType::PARQUET | FileType::AVRO => match c.variant {
                UNCOMPRESSED => Ok(ext),
                _ => Err(DataFusionError::Internal(
                    "FileCompressionType can be specified for CSV/JSON FileType.".into(),
                )),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::datasource::file_format::file_type::{FileCompressionType, FileType};
    use crate::error::DataFusionError;
    use std::str::FromStr;

    #[test]
    fn get_ext_with_compression() {
        let file_type = FileType::CSV;
        assert_eq!(
            file_type
                .get_ext_with_compression(FileCompressionType::UNCOMPRESSED)
                .unwrap(),
            ".csv"
        );
        assert_eq!(
            file_type
                .get_ext_with_compression(FileCompressionType::GZIP)
                .unwrap(),
            ".csv.gz"
        );
        assert_eq!(
            file_type
                .get_ext_with_compression(FileCompressionType::XZ)
                .unwrap(),
            ".csv.xz"
        );
        assert_eq!(
            file_type
                .get_ext_with_compression(FileCompressionType::BZIP2)
                .unwrap(),
            ".csv.bz2"
        );

        let file_type = FileType::JSON;
        assert_eq!(
            file_type
                .get_ext_with_compression(FileCompressionType::UNCOMPRESSED)
                .unwrap(),
            ".json"
        );
        assert_eq!(
            file_type
                .get_ext_with_compression(FileCompressionType::GZIP)
                .unwrap(),
            ".json.gz"
        );
        assert_eq!(
            file_type
                .get_ext_with_compression(FileCompressionType::XZ)
                .unwrap(),
            ".json.xz"
        );
        assert_eq!(
            file_type
                .get_ext_with_compression(FileCompressionType::BZIP2)
                .unwrap(),
            ".json.bz2"
        );

        let file_type = FileType::AVRO;
        assert_eq!(
            file_type
                .get_ext_with_compression(FileCompressionType::UNCOMPRESSED)
                .unwrap(),
            ".avro"
        );
        assert!(matches!(
            file_type.get_ext_with_compression(FileCompressionType::GZIP),
            Err(DataFusionError::Internal(_))
        ));
        assert!(matches!(
            file_type.get_ext_with_compression(FileCompressionType::BZIP2),
            Err(DataFusionError::Internal(_))
        ));

        let file_type = FileType::PARQUET;
        assert_eq!(
            file_type
                .get_ext_with_compression(FileCompressionType::UNCOMPRESSED)
                .unwrap(),
            ".parquet"
        );
        assert!(matches!(
            file_type.get_ext_with_compression(FileCompressionType::GZIP),
            Err(DataFusionError::Internal(_))
        ));
        assert!(matches!(
            file_type.get_ext_with_compression(FileCompressionType::BZIP2),
            Err(DataFusionError::Internal(_))
        ));
    }

    #[test]
    fn from_str() {
        assert_eq!(FileType::from_str("csv").unwrap(), FileType::CSV);
        assert_eq!(FileType::from_str("CSV").unwrap(), FileType::CSV);

        assert_eq!(FileType::from_str("json").unwrap(), FileType::JSON);
        assert_eq!(FileType::from_str("JSON").unwrap(), FileType::JSON);

        assert_eq!(FileType::from_str("avro").unwrap(), FileType::AVRO);
        assert_eq!(FileType::from_str("AVRO").unwrap(), FileType::AVRO);

        assert_eq!(FileType::from_str("parquet").unwrap(), FileType::PARQUET);
        assert_eq!(FileType::from_str("PARQUET").unwrap(), FileType::PARQUET);

        assert!(matches!(
            FileType::from_str("Unknown"),
            Err(DataFusionError::NotImplemented(_))
        ));

        assert_eq!(
            FileCompressionType::from_str("gz").unwrap(),
            FileCompressionType::GZIP
        );
        assert_eq!(
            FileCompressionType::from_str("GZ").unwrap(),
            FileCompressionType::GZIP
        );
        assert_eq!(
            FileCompressionType::from_str("gzip").unwrap(),
            FileCompressionType::GZIP
        );
        assert_eq!(
            FileCompressionType::from_str("GZIP").unwrap(),
            FileCompressionType::GZIP
        );
        assert_eq!(
            FileCompressionType::from_str("xz").unwrap(),
            FileCompressionType::XZ
        );
        assert_eq!(
            FileCompressionType::from_str("XZ").unwrap(),
            FileCompressionType::XZ
        );
        assert_eq!(
            FileCompressionType::from_str("bz2").unwrap(),
            FileCompressionType::BZIP2
        );
        assert_eq!(
            FileCompressionType::from_str("BZ2").unwrap(),
            FileCompressionType::BZIP2
        );
        assert_eq!(
            FileCompressionType::from_str("bzip2").unwrap(),
            FileCompressionType::BZIP2
        );
        assert_eq!(
            FileCompressionType::from_str("BZIP2").unwrap(),
            FileCompressionType::BZIP2
        );

        assert_eq!(
            FileCompressionType::from_str("").unwrap(),
            FileCompressionType::UNCOMPRESSED
        );

        assert!(matches!(
            FileCompressionType::from_str("Unknown"),
            Err(DataFusionError::NotImplemented(_))
        ));
    }
}
