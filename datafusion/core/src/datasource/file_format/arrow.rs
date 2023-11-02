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

//! Apache Arrow format abstractions
//!
//! Works with files following the [Arrow IPC format](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format)

use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

use crate::datasource::file_format::FileFormat;
use crate::datasource::physical_plan::{ArrowExec, FileScanConfig};
use crate::error::Result;
use crate::execution::context::SessionState;
use crate::physical_plan::ExecutionPlan;

use arrow::ipc::convert::fb_to_schema;
use arrow::ipc::reader::FileReader;
use arrow::ipc::root_as_message;
use arrow_schema::{ArrowError, Schema, SchemaRef};

use bytes::Bytes;
use datafusion_common::{FileType, Statistics};
use datafusion_physical_expr::PhysicalExpr;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::{GetResultPayload, ObjectMeta, ObjectStore};

/// Arrow `FileFormat` implementation.
#[derive(Default, Debug)]
pub struct ArrowFormat;

#[async_trait]
impl FileFormat for ArrowFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        _state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas = vec![];
        for object in objects {
            let r = store.as_ref().get(&object.location).await?;
            let schema = match r.payload {
                GetResultPayload::File(mut file, _) => {
                    let reader = FileReader::try_new(&mut file, None)?;
                    reader.schema()
                }
                GetResultPayload::Stream(stream) => {
                    infer_schema_from_file_stream(stream).await?
                }
            };
            schemas.push(schema.as_ref().clone());
        }
        let merged_schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(merged_schema))
    }

    async fn infer_stats(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &SessionState,
        conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = ArrowExec::new(conf);
        Ok(Arc::new(exec))
    }

    fn file_type(&self) -> FileType {
        FileType::ARROW
    }
}

const ARROW_MAGIC: [u8; 6] = [b'A', b'R', b'R', b'O', b'W', b'1'];
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

/// Custom implementation of inferring schema. Should eventually be moved upstream to arrow-rs. 
/// See https://github.com/apache/arrow-rs/issues/5021
async fn infer_schema_from_file_stream(
    mut stream: BoxStream<'static, object_store::Result<Bytes>>,
) -> Result<SchemaRef> {
    // Expected format:
    // <magic number "ARROW1"> - 6 bytes
    // <empty padding bytes [to 8 byte boundary]> - 2 bytes
    // <continutation: 0xFFFFFFFF> - 4 bytes, not present below v0.15.0
    // <metadata_size: int32> - 4 bytes
    // <metadata_flatbuffer: bytes>
    // <rest of file bytes>

    // So in first read we need at least all known sized sections,
    // which is 6 + 2 + 4 + 4 = 16 bytes.
    let bytes = collect_at_least_n_bytes(&mut stream, 16, None).await?;

    // Files should start with these magic bytes
    if bytes[0..6] != ARROW_MAGIC {
        return Err(ArrowError::ParseError(
            "Arrow file does not contian correct header".to_string(),
        ))?;
    }

    // Since continuation marker bytes added in later versions
    let (meta_len, rest_of_bytes_start_index) = if bytes[8..12] == CONTINUATION_MARKER {
        (&bytes[12..16], 16)
    } else {
        (&bytes[8..12], 12)
    };

    let meta_len = [meta_len[0], meta_len[1], meta_len[2], meta_len[3]];
    let meta_len = i32::from_le_bytes(meta_len);

    // Read bytes for Schema message
    let block_data = if bytes[rest_of_bytes_start_index..].len() < meta_len as usize {
        // Need to read more bytes to decode Message
        let mut block_data = Vec::with_capacity(meta_len as usize);
        // In case we had some spare bytes in our initial read chunk
        block_data.extend_from_slice(&bytes[rest_of_bytes_start_index..]);
        let size_to_read = meta_len as usize - block_data.len();
        let block_data =
            collect_at_least_n_bytes(&mut stream, size_to_read, Some(block_data)).await?;
        Cow::Owned(block_data)
    } else {
        // Already have the bytes we need
        let end_index = meta_len as usize + rest_of_bytes_start_index;
        let block_data = &bytes[rest_of_bytes_start_index..end_index];
        Cow::Borrowed(block_data)
    };

    // Decode Schema message
    let message = root_as_message(&block_data).map_err(|err| {
        ArrowError::ParseError(format!("Unable to read IPC message as metadata: {err:?}"))
    })?;
    let ipc_schema = message.header_as_schema().ok_or_else(|| {
        ArrowError::IpcError("Unable to read IPC message as schema".to_string())
    })?;
    let schema = fb_to_schema(ipc_schema);

    Ok(Arc::new(schema))
}

async fn collect_at_least_n_bytes(
    stream: &mut BoxStream<'static, object_store::Result<Bytes>>,
    n: usize,
    extend_from: Option<Vec<u8>>,
) -> Result<Vec<u8>> {
    let mut buf = extend_from.unwrap_or_else(|| Vec::with_capacity(n));
    // If extending existing buffer then ensure we read n additional bytes
    let n = n + buf.len();
    while let Some(bytes) = stream.next().await.transpose()? {
        buf.extend_from_slice(&bytes);
        if buf.len() >= n {
            break;
        }
    }
    if buf.len() < n {
        return Err(ArrowError::ParseError(
            "Unexpected end of byte stream for Arrow IPC file".to_string(),
        ))?;
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use object_store::{chunked::ChunkedStore, memory::InMemory, path::Path};

    use crate::execution::context::SessionContext;

    use super::*;

    #[tokio::test]
    async fn test_infer_schema_stream() -> Result<()> {
        let mut bytes = std::fs::read("tests/data/example.arrow")?;
        bytes.truncate(bytes.len() - 20); // mangle end to show we don't need to read whole file
        let location = Path::parse("example.arrow")?;
        let in_memory_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        in_memory_store.put(&location, bytes.into()).await?;

        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let object_meta = ObjectMeta {
            location,
            last_modified: DateTime::default(),
            size: usize::MAX,
            e_tag: None,
        };

        let arrow_format = ArrowFormat {};
        let expected = vec!["f0: Int64", "f1: Utf8", "f2: Boolean"];

        // Test chunk sizes where too small so we keep having to read more bytes
        // And when large enough that first read contains all we need
        for chunk_size in [7, 3000] {
            let store = Arc::new(ChunkedStore::new(in_memory_store.clone(), chunk_size));
            let inferred_schema = arrow_format
                .infer_schema(
                    &state,
                    &(store.clone() as Arc<dyn ObjectStore>),
                    &[object_meta.clone()],
                )
                .await?;
            let actual_fields = inferred_schema
                .fields()
                .iter()
                .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
                .collect::<Vec<_>>();
            assert_eq!(expected, actual_fields);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_infer_schema_short_stream() -> Result<()> {
        let mut bytes = std::fs::read("tests/data/example.arrow")?;
        bytes.truncate(20); // should cause error that file shorter than expected
        let location = Path::parse("example.arrow")?;
        let in_memory_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        in_memory_store.put(&location, bytes.into()).await?;

        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let object_meta = ObjectMeta {
            location,
            last_modified: DateTime::default(),
            size: usize::MAX,
            e_tag: None,
        };

        let arrow_format = ArrowFormat {};

        let store = Arc::new(ChunkedStore::new(in_memory_store.clone(), 7));
        let err = arrow_format
            .infer_schema(
                &state,
                &(store.clone() as Arc<dyn ObjectStore>),
                &[object_meta.clone()],
            )
            .await;

        assert!(err.is_err());
        assert_eq!(
            "Arrow error: Parser error: Unexpected end of byte stream for Arrow IPC file",
            err.unwrap_err().to_string()
        );

        Ok(())
    }
}
