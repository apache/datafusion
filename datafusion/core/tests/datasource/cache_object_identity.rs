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

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::prelude::{ParquetReadOptions, SessionContext, col, lit};
use datafusion_common::assert_batches_eq;
use datafusion_execution::cache::{StoreScopedPath, TableScopedPath};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_functions_aggregate::expr_fn::{count, max, min};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use url::Url;

fn parquet_bytes(values: Vec<i32>) -> Bytes {
    let values: ArrayRef = Arc::new(Int32Array::from(values));
    let batch = RecordBatch::try_from_iter([("value", values)]).unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let mut bytes = vec![];
    let mut writer =
        ArrowWriter::try_new(&mut bytes, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    bytes.into()
}

async fn aggregate_file(ctx: &SessionContext, url: &str) -> Vec<RecordBatch> {
    ctx.read_parquet(url, ParquetReadOptions::default())
        .await
        .unwrap()
        .aggregate(
            vec![],
            vec![
                min(col("value")).alias("min"),
                max(col("value")).alias("max"),
                count(col("value")).alias("count"),
            ],
        )
        .unwrap()
        .collect()
        .await
        .unwrap()
}

#[tokio::test]
async fn parquet_caches_separate_object_stores() {
    let first = Arc::new(FixedTimeStore::default());
    let second = Arc::new(FixedTimeStore::default());
    let path = Path::from("data/file.parquet");
    let first_bytes = parquet_bytes(vec![1, 2, 3]);
    let second_bytes = parquet_bytes(vec![7, 8, 9]);
    assert_eq!(first_bytes.len(), second_bytes.len());
    first.put(&path, first_bytes.into()).await.unwrap();
    second.put(&path, second_bytes.into()).await.unwrap();
    // Every ObjectMeta field, including ETag, is identical across the stores.
    assert_eq!(
        first.head(&path).await.unwrap(),
        second.head(&path).await.unwrap()
    );

    let ctx = SessionContext::new();
    ctx.runtime_env()
        .register_object_store(&Url::parse("mem://first").unwrap(), first);
    ctx.runtime_env()
        .register_object_store(&Url::parse("mem://second").unwrap(), second);
    let first_result = aggregate_file(&ctx, "mem://first/data/").await;
    assert_batches_eq!(
        [
            "+-----+-----+-------+",
            "| min | max | count |",
            "+-----+-----+-------+",
            "| 1   | 3   | 3     |",
            "+-----+-----+-------+",
        ],
        &first_result
    );
    let second_result = aggregate_file(&ctx, "mem://second/data/").await;
    assert_batches_eq!(
        [
            "+-----+-----+-------+",
            "| min | max | count |",
            "+-----+-----+-------+",
            "| 7   | 9   | 3     |",
            "+-----+-----+-------+",
        ],
        &second_result
    );

    // Execute scans as well as metadata-backed aggregates, including repeated
    // reads through the Parquet reader's metadata cache.
    for (url, value) in [("mem://first/data/", 2), ("mem://second/data/", 8)] {
        for _ in 0..2 {
            let results = ctx
                .read_parquet(url, ParquetReadOptions::default())
                .await
                .unwrap()
                .filter(col("value").eq(lit(value)))
                .unwrap()
                .collect()
                .await
                .unwrap();
            let values = results
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .iter()
                })
                .collect::<Vec<_>>();
            assert_eq!(values, vec![Some(value)], "{url}");
        }
    }

    let caches = &ctx.runtime_env().cache_manager;
    let metadata = caches.get_file_metadata_cache();
    let statistics = caches.get_file_statistic_cache().unwrap();
    let listings = caches.get_list_files_cache().unwrap();
    for url in ["mem://first", "mem://second"] {
        let object_store_url = ObjectStoreUrl::parse(url).unwrap();
        assert!(
            metadata
                .get(&StoreScopedPath::new(
                    object_store_url.clone(),
                    path.clone()
                ))
                .is_some()
        );
        assert!(
            statistics
                .get(&TableScopedPath {
                    table: None,
                    store_path: StoreScopedPath::new(
                        object_store_url.clone(),
                        path.clone()
                    ),
                })
                .is_some()
        );
        assert!(
            listings
                .get(&TableScopedPath {
                    table: None,
                    store_path: StoreScopedPath::new(
                        object_store_url,
                        Path::from("data")
                    ),
                })
                .is_some()
        );
    }
    assert_eq!(metadata.len(), 2);
    assert_eq!(statistics.len(), 2);
    assert_eq!(listings.len(), 2);
}

#[tokio::test]
async fn parquet_caches_refresh_changed_etag() {
    let store = Arc::new(FixedTimeStore::default());
    let path = Path::from("data.parquet");
    let original = parquet_bytes(vec![1, 2, 3]);
    let replacement = parquet_bytes(vec![7, 8, 9]);
    assert_eq!(original.len(), replacement.len());
    store.put(&path, original.into()).await.unwrap();
    let before = store.head(&path).await.unwrap();
    let ctx = SessionContext::new();
    ctx.runtime_env()
        .register_object_store(&Url::parse("mem://overwrite").unwrap(), store.clone());
    // A single-file URL obtains fresh metadata without a cached directory listing.
    let original_result = aggregate_file(&ctx, "mem://overwrite/data.parquet").await;
    assert_batches_eq!(
        [
            "+-----+-----+-------+",
            "| min | max | count |",
            "+-----+-----+-------+",
            "| 1   | 3   | 3     |",
            "+-----+-----+-------+",
        ],
        &original_result
    );

    store.put(&path, replacement.into()).await.unwrap();
    let after = store.head(&path).await.unwrap();
    assert_eq!(before.size, after.size);
    assert_eq!(before.last_modified, after.last_modified);
    assert_ne!(before.e_tag, after.e_tag);
    let replacement_result = aggregate_file(&ctx, "mem://overwrite/data.parquet").await;
    assert_batches_eq!(
        [
            "+-----+-----+-------+",
            "| min | max | count |",
            "+-----+-----+-------+",
            "| 7   | 9   | 3     |",
            "+-----+-----+-------+",
        ],
        &replacement_result
    );
    let object_store_url = ObjectStoreUrl::parse("mem://overwrite").unwrap();
    let caches = &ctx.runtime_env().cache_manager;
    let metadata = caches
        .get_file_metadata_cache()
        .get(&StoreScopedPath::new(
            object_store_url.clone(),
            path.clone(),
        ))
        .unwrap();
    let statistics = caches
        .get_file_statistic_cache()
        .unwrap()
        .get(&TableScopedPath {
            table: None,
            store_path: StoreScopedPath::new(object_store_url, path),
        })
        .unwrap();
    assert_eq!(metadata.meta.e_tag, after.e_tag);
    assert_eq!(statistics.meta.e_tag, after.e_tag);
}

/// Keeps modification times identical so the tests isolate store identity and ETag.
#[derive(Debug, Default)]
struct FixedTimeStore(InMemory);

impl Display for FixedTimeStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FixedTimeStore")
    }
}

#[async_trait]
impl ObjectStore for FixedTimeStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.0.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let mut result = self.0.get_opts(location, options).await?;
        result.meta.last_modified = chrono::DateTime::UNIX_EPOCH;
        Ok(result)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.0
            .list(prefix)
            .map_ok(|mut meta| {
                meta.last_modified = chrono::DateTime::UNIX_EPOCH;
                meta
            })
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        let mut result = self.0.list_with_delimiter(prefix).await?;
        for meta in &mut result.objects {
            meta.last_modified = chrono::DateTime::UNIX_EPOCH;
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.0.delete_stream(locations)
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.0.copy_opts(from, to, options).await
    }
}
