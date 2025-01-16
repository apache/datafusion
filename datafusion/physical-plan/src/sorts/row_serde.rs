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

use arrow::row::RowConverter;
use arrow::row::Rows;
use datafusion_common::error::DataFusionError;
use datafusion_common::Result;
use std::fs::File;
use std::future::Future;
use std::io::BufWriter;
use std::io::Write;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use std::sync::Arc;

use tokio::sync::mpsc::Sender;

use crate::stream::ReceiverStreamBuilder;
/// used for spill Rows

pub struct RowStreamBuilder {
    inner: ReceiverStreamBuilder<Rows>,
}

impl RowStreamBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: ReceiverStreamBuilder::new(capacity),
        }
    }

    pub fn tx(&self) -> Sender<Result<Rows, DataFusionError>> {
        self.inner.tx()
    }

    pub fn spawn<F>(&mut self, task: F)
    where
        F: Future<Output = Result<(), DataFusionError>>,
        F: Send + 'static,
    {
        self.inner.spawn(task)
    }

    pub fn spawn_blocking<F>(&mut self, f: F)
    where
        F: FnOnce() -> Result<(), DataFusionError>,
        F: Send + 'static,
    {
        self.inner.spawn_blocking(f)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum CompressionType {
    UNCOMPRESSED,
    #[cfg(feature = "compress")]
    GZIP,
}

/// +----------------+------------------+----------------+------------------+
/// | Block1 Data    | Block1 Metadata  | Block2 Data    | Block2 Metadata  | ...
/// +----------------+------------------+----------------+------------------+
/// | FileMetadata   | MetadataLength  |
/// +----------------+------------------+
#[derive(Debug)]
pub struct RowWriter {
    writer: BufWriter<File>,
    block_offsets: Vec<u64>,
    current_offset: u64,
    compression: CompressionType,
}

impl RowWriter {
    pub fn new(
        path: &Path,
        compression: Option<CompressionType>,
    ) -> Result<Self, DataFusionError> {
        let file = File::create(path).map_err(|e| {
            DataFusionError::Execution(format!("Failed to create file at {path:?}: {e}"))
        })?;

        Ok(Self {
            writer: BufWriter::new(file),
            block_offsets: Vec::new(),
            current_offset: 0,
            compression: compression.unwrap_or(CompressionType::UNCOMPRESSED),
        })
    }

    pub fn write_rows(&mut self, rows: &Rows) -> Result<(), DataFusionError> {
        self.block_offsets.push(self.current_offset);
        let (row_data, row_offsets) = self.prepare_row_data(rows)?;
        let compressed_data = self.compress(&row_data)?;

        self.writer.write_all(&compressed_data)?;

        self.write_block_metadata(&row_offsets)?;

        self.current_offset +=
            (compressed_data.len() + self.metadata_size(&row_offsets)) as u64;

        Ok(())
    }

    fn prepare_row_data(
        &self,
        rows: &Rows,
    ) -> Result<(Vec<u8>, Vec<u32>), DataFusionError> {
        let mut row_offsets = Vec::with_capacity(rows.num_rows());
        let mut current_offset = 0u32;
        let mut row_data = Vec::new();

        for i in 0..rows.num_rows() {
            row_offsets.push(current_offset);
            let row = rows.row(i).data();
            row_data.extend_from_slice(row);
            current_offset += row.len() as u32;
        }

        Ok((row_data, row_offsets))
    }

    fn write_block_metadata(
        &mut self,
        row_offsets: &[u32],
    ) -> Result<(), DataFusionError> {
        for &offset in row_offsets {
            self.writer.write_all(&offset.to_le_bytes())?;
        }
        self.writer
            .write_all(&(row_offsets.len() as u32).to_le_bytes())?;
        Ok(())
    }

    fn metadata_size(&self, row_offsets: &[u32]) -> usize {
        4 + // row count
        row_offsets.len() * 4 // row offsets
    }

    pub fn finish(mut self) -> Result<(), DataFusionError> {
        let metadata = self.prepare_file_metadata()?;
        let compressed_metadata = self.compress(&metadata)?;

        self.writer.write_all(&compressed_metadata)?;
        self.writer
            .write_all(&(compressed_metadata.len() as u32).to_le_bytes())?;
        self.writer.flush()?;

        Ok(())
    }

    fn prepare_file_metadata(&self) -> Result<Vec<u8>, DataFusionError> {
        let mut metadata = Vec::new();
        metadata.extend_from_slice(&(self.block_offsets.len() as u32).to_le_bytes());
        for &offset in &self.block_offsets {
            metadata.extend_from_slice(&(offset as u32).to_le_bytes());
        }
        Ok(metadata)
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, DataFusionError> {
        match self.compression {
            CompressionType::UNCOMPRESSED => Ok(data.to_vec()),
            #[cfg(feature = "compress")]
            CompressionType::GZIP => {
                let mut encoder = flate2::write::GzEncoder::new(
                    Vec::new(),
                    flate2::Compression::default(),
                );
                encoder.write_all(data)?;
                Ok(encoder.finish()?)
            }
        }
    }
}

pub struct RowReader {
    reader: BufReader<File>,
    block_offsets: Vec<u64>,
    current_block: usize,
    compression: CompressionType,
    converter: Arc<RowConverter>,
}

impl RowReader {
    pub fn new(
        path: &Path,
        compression: Option<CompressionType>,
        converter: Arc<RowConverter>,
    ) -> Result<Self, DataFusionError> {
        let mut reader = BufReader::new(File::open(path)?);
        let compression = compression.unwrap_or(CompressionType::UNCOMPRESSED);
        let block_offsets = Self::read_file_metadata(&mut reader, compression)?;

        Ok(Self {
            reader,
            block_offsets,
            current_block: 0,
            compression,
            converter,
        })
    }

    fn read_file_metadata(
        reader: &mut BufReader<File>,
        compression: CompressionType,
    ) -> Result<Vec<u64>, DataFusionError> {
        reader.seek(SeekFrom::End(-4))?;
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let metadata_len = u32::from_le_bytes(len_buf) as u64;

        reader.seek(SeekFrom::End(-(metadata_len as i64 + 4)))?;
        let mut metadata_buf = vec![0u8; metadata_len as usize];
        reader.read_exact(&mut metadata_buf)?;
        let metadata = Self::decompress(&metadata_buf, compression)?;

        let block_offsets = metadata[4..]
            .chunks_exact(4)
            .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()) as u64)
            .collect();

        Ok(block_offsets)
    }

    fn read_block(&mut self, block_idx: usize) -> Result<Rows, DataFusionError> {
        let start_offset = self.block_offsets[block_idx];
        let end_offset = if block_idx + 1 < self.block_offsets.len() {
            self.block_offsets[block_idx + 1]
        } else {
            let metadata_pos = self.reader.seek(SeekFrom::End(-4))?;
            let mut len_buf = [0u8; 4];
            self.reader.read_exact(&mut len_buf)?;
            let metadata_len = u32::from_le_bytes(len_buf);
            metadata_pos - metadata_len as u64
        };

        self.reader.seek(SeekFrom::Start(end_offset - 4))?;
        let mut len_buf = [0u8; 4];
        self.reader.read_exact(&mut len_buf)?;
        let row_count = u32::from_le_bytes(len_buf) as usize;

        let offsets_size = row_count * 4;
        self.reader
            .seek(SeekFrom::Start(end_offset - 4 - offsets_size as u64))?;
        let mut offsets = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let mut offset_buf = [0u8; 4];
            self.reader.read_exact(&mut offset_buf)?;
            offsets.push(u32::from_le_bytes(offset_buf) as usize);
        }

        let data_size = (end_offset - start_offset - 4 - offsets_size as u64) as usize;
        self.reader.seek(SeekFrom::Start(start_offset))?;
        let mut buffer = vec![0u8; data_size];
        self.reader.read_exact(&mut buffer)?;
        let data = Self::decompress(&buffer, self.compression)?;

        self.build_rows(&data, &offsets)
    }

    fn build_rows(
        &self,
        data: &[u8],
        row_offsets: &[usize],
    ) -> Result<Rows, DataFusionError> {
        let mut rows = self.converter.empty_rows(row_offsets.len(), data.len());

        for i in 0..row_offsets.len() {
            let start = row_offsets[i];
            let end = if i + 1 < row_offsets.len() {
                row_offsets[i + 1]
            } else {
                data.len()
            };

            rows.push(self.converter.parser().parse(&data[start..end]));
        }

        Ok(rows)
    }

    fn decompress(
        data: &[u8],
        compression: CompressionType,
    ) -> Result<Vec<u8>, DataFusionError> {
        match compression {
            CompressionType::UNCOMPRESSED => Ok(data.to_vec()),
            #[cfg(feature = "compress")]
            CompressionType::GZIP => {
                let mut decoder = flate2::read::GzDecoder::new(data);
                let mut decoded_data = Vec::new();
                decoder.read_to_end(&mut decoded_data)?;
                Ok(decoded_data)
            }
        }
    }
}

impl Iterator for RowReader {
    type Item = Result<Rows, DataFusionError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_block >= self.block_offsets.len() {
            return None;
        }

        let result = self.read_block(self.current_block);
        self.current_block += 1;
        Some(result)
    }
}
#[cfg(test)]
mod tests {
    use arrow::{
        compute::concat_batches,
        row::{RowConverter, SortField},
    };
    use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::DataFusionError;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    use crate::sorts::row_serde::{RowReader, RowWriter};
    #[test]
    fn test_recordbatch_to_row_and_back() -> Result<(), DataFusionError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let fields = schema
            .fields()
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect();

        let a = Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef;
        let b = Arc::new(Int32Array::from(vec![5, 6, 7, 8])) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::<Schema>::clone(&schema),
            vec![
                Arc::<dyn arrow_array::Array>::clone(&a),
                Arc::<dyn arrow_array::Array>::clone(&b),
            ],
        )?;

        let temp_file = NamedTempFile::new()?;
        let temp_path = temp_file.path();

        let converter = Arc::new(RowConverter::new(fields)?);
        let mut row_writer = RowWriter::new(temp_path, None)?;

        let rows = converter.convert_columns(batch.columns())?;
        row_writer.write_rows(&rows)?;
        row_writer.finish()?;

        let row_reader =
            RowReader::new(temp_path, None, Arc::<RowConverter>::clone(&converter))?;
        let mut read_batches = Vec::new();

        for rows in row_reader {
            let rows = rows?;
            let columns = converter.convert_rows(&rows)?;
            let record_batch =
                RecordBatch::try_new(Arc::<Schema>::clone(&schema), columns)?;
            read_batches.push(record_batch);
        }
        let read_batch = concat_batches(&schema, &read_batches)?;

        assert_eq!(read_batch.schema(), schema);
        assert_eq!(read_batch.num_rows(), batch.num_rows());
        assert_eq!(
            read_batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            a.as_any().downcast_ref::<Int32Array>().unwrap()
        );
        assert_eq!(
            read_batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            b.as_any().downcast_ref::<Int32Array>().unwrap()
        );

        Ok(())
    }
}
