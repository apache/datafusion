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

use async_trait::async_trait;
use bytes::Bytes;
use datafusion_storage::{path::Path, *};
use futures::{StreamExt, stream::BoxStream};
use std::{ops::Range, sync::Arc};

#[derive(Debug)]
struct Memory(Bytes);
#[async_trait]
impl Storage for Memory {
    async fn open(&self, _: &Path, _: FileAccessContext) -> Result<Arc<dyn FileReader>> {
        Ok(Arc::new(Self(self.0.clone())))
    }
}
#[async_trait]
impl FileReader for Memory {
    async fn read_range(&self, range: ReadRange) -> Result<Bytes> {
        let range = match range {
            ReadRange::Bounded(range) => range,
            ReadRange::Suffix(size) => {
                (self.0.len() as u64).saturating_sub(size)..self.0.len() as u64
            }
        };
        Ok(self.0.slice(range.start as usize..range.end as usize))
    }
    async fn read_ranges(&self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        Ok(ranges
            .into_iter()
            .map(|r| self.0.slice(r.start as usize..r.end as usize))
            .collect())
    }
    fn stream(
        self: Arc<Self>,
        range: Option<ReadRange>,
    ) -> BoxStream<'static, Result<Bytes>> {
        futures::stream::once(async move {
            match range {
                Some(range) => self.read_range(range).await,
                None => Ok(self.0.clone()),
            }
        })
        .boxed()
    }
}

#[tokio::test]
async fn replacement_and_deregistration_preserve_bound_readers() {
    let registry = StorageRegistry::default();
    let url = url::Url::parse("memory://bucket/prefix").unwrap();
    registry
        .register(&url, Arc::new(Memory(Bytes::from_static(b"old"))))
        .unwrap();
    let old = registry.get(&url).unwrap();
    let replaced = registry
        .register(&url, Arc::new(Memory(Bytes::from_static(b"new"))))
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&old, &replaced));
    let current = registry.get(&url).unwrap();
    assert_ne!(old.id(), current.id());
    registry.deregister(&url).unwrap();
    assert!(registry.get(&url).is_err());
    for (binding, expected) in [(old, b"old"), (current, b"new")] {
        let reader = binding
            .storage()
            .open(&Path::from("file"), FileAccessContext::default())
            .await
            .unwrap();
        let bytes = reader.read_range((0..3).into()).await.unwrap();
        drop(reader);
        drop(binding);
        assert_eq!(bytes, expected[..]);
    }
}

#[tokio::test]
async fn read_only_storage_does_not_require_metadata_or_output_operations() {
    let storage = Memory(Bytes::from_static(b"data"));
    let path = Path::from("file");
    let context = FileAccessContext::default();
    let reader = storage.open(&path, context.clone()).await.unwrap();
    assert_eq!(
        collect_bytes(reader.stream(None)).await.unwrap(),
        b"data"[..]
    );
    assert!(matches!(
        storage.stat(&path, &context).await,
        Err(Error::NotSupported(_))
    ));
    assert!(matches!(
        storage
            .writer(&path, WriterOptions::default(), context)
            .await,
        Err(Error::NotSupported(_))
    ));
}

#[test]
fn urls_are_namespaces_not_path_mounts() {
    assert_eq!(StorageUrl::local_filesystem().as_str(), "file:///");
    assert_eq!(
        StorageUrl::parse("s3://username:password@host:123/foo?bar=baz")
            .unwrap()
            .as_str(),
        "s3://host:123/",
    );
    assert!(StorageUrl::parse("s3://bucket:invalid").is_err());

    assert_eq!(
        StorageUrl::parse("s3://bucket/a").unwrap(),
        StorageUrl::parse("s3://bucket/b").unwrap()
    );
    assert_ne!(
        StorageUrl::parse("https://example.com:8443/a").unwrap(),
        StorageUrl::parse("https://example.com/a").unwrap()
    );
}
