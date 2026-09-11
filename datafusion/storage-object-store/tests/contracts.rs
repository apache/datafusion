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

#[path = "../../storage/tests/common/backend.rs"]
mod backend;
use datafusion_storage::{path::Path, *};
use datafusion_storage_object_store::ObjectStoreStorage;
use std::sync::Arc;

#[tokio::test]
async fn object_store_adapter_preserves_unconditional_reads() {
    use object_store::{ObjectStoreExt, memory::InMemory};
    let backend = Arc::new(InMemory::new());
    let storage = ObjectStoreStorage::new(backend.clone());
    backend::backend_contract(&storage).await;
    let path = Path::from("data.parquet");
    let metadata = storage
        .stat(&path, &FileAccessContext::default())
        .await
        .unwrap();
    assert!(metadata.e_tag.is_some());
    let reader = storage
        .open(&path, FileAccessContext::default())
        .await
        .unwrap();
    backend
        .put(
            &object_store::path::Path::from("data.parquet"),
            bytes::Bytes::from_static(b"replacement").into(),
        )
        .await
        .unwrap();
    assert_eq!(reader.read_range((0..3).into()).await.unwrap(), b"rep"[..]);
    assert_eq!(
        reader.read_ranges(vec![0..1, 2..3]).await.unwrap(),
        vec![
            bytes::Bytes::from_static(b"r"),
            bytes::Bytes::from_static(b"p")
        ]
    );
    assert_eq!(
        collect_bytes(reader.stream(None)).await.unwrap(),
        b"replacement"[..]
    );
}

#[test]
fn file_metadata_keeps_both_version_and_etag() {
    let metadata = object_store::ObjectMeta {
        location: object_store::path::Path::from("file"),
        last_modified: chrono::DateTime::UNIX_EPOCH,
        size: 42,
        e_tag: Some("etag".into()),
        version: Some("version".into()),
    };
    let info = datafusion_storage_object_store::file_info(metadata);
    assert_eq!(info.e_tag.as_deref(), Some("etag"));
    assert_eq!(info.version.as_deref(), Some("version"));
}
