// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Benchmarks listing-cache misses through the public `ListingTableUrl` API.
//!
//! A lazy object store generates fresh metadata without retaining the listing.
//! There is no network or file-data I/O. Consumers count results without collecting
//! them, so large allocations come from listing/cache behavior, not the harness.
//! Cases vary whether a complete listing fits the 1 MiB cache, whether the consumer
//! stops after ten matches, and whether only one percent of extensions match.
//!
//! Run with `cargo bench -p datafusion-benchmarks --bench list_files_cache`.

use std::fmt::{Display, Formatter};

use async_trait::async_trait;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt, future, stream};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

const CACHE_LIMIT: usize = 1024 * 1024;

/// Generates metadata on demand; even a million-file listing has constant store
/// memory. Every `list` call generates its own paths rather than cloning a fixture.
#[derive(Debug)]
struct GeneratedObjectStore {
    files: usize,
    parquet_every: usize,
}

impl Display for GeneratedObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "GeneratedObjectStore")
    }
}

fn unsupported(operation: &str) -> object_store::Error {
    object_store::Error::NotImplemented {
        operation: operation.to_owned(),
        implementer: "GeneratedObjectStore".to_owned(),
    }
}

#[async_trait]
impl ObjectStore for GeneratedObjectStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(unsupported("put_opts"))
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(unsupported("put_multipart_opts"))
    }

    async fn get_opts(
        &self,
        _location: &Path,
        _opts: GetOptions,
    ) -> object_store::Result<GetResult> {
        Err(unsupported("get_opts"))
    }

    fn delete_stream(
        &self,
        _locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        stream::once(future::ready(Err(unsupported("delete_stream")))).boxed()
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        assert_eq!(prefix.map(Path::as_ref), Some("table"));
        let parquet_every = self.parquet_every;
        stream::iter(0..self.files)
            .map(move |index| {
                let extension = if index % parquet_every == 0 {
                    "parquet"
                } else {
                    "csv"
                };
                ObjectMeta {
                    location: Path::from(format!("table/part-{index:08}.{extension}")),
                    last_modified: Default::default(),
                    size: 1024,
                    e_tag: None,
                    version: None,
                }
            })
            .map(Ok)
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        Err(unsupported("list_with_delimiter"))
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        Err(unsupported("copy_opts"))
    }
}

fn criterion_benchmark(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let runtime_env = RuntimeEnvBuilder::new()
        .with_object_list_cache_limit(CACHE_LIMIT)
        .build_arc()
        .unwrap();
    let cache = runtime_env.cache_manager.get_list_files_cache().unwrap();
    let context = SessionContext::new_with_config_rt(SessionConfig::new(), runtime_env);
    let state = context.state();
    let url = ListingTableUrl::parse("test:///table/").unwrap();

    let mut group = criterion.benchmark_group("list_files_cache_miss");
    for (name, files, take, parquet_every) in [
        ("cacheable/full", 1_000, usize::MAX, 1),
        ("cacheable/first_10", 1_000, 10, 1),
        ("oversized/full", 100_000, usize::MAX, 1),
        ("oversized/first_10", 100_000, 10, 1),
        ("oversized/selective_full", 100_000, usize::MAX, 100),
        ("oversized/selective_first_10", 100_000, 10, 100),
    ] {
        let store = GeneratedObjectStore {
            files,
            parquet_every,
        };
        let expected_files = files.div_ceil(parquet_every).min(take);
        group.bench_function(name, |bencher| {
            bencher.to_async(&runtime).iter(|| async {
                // Include cache teardown, and ensure every iteration is a miss.
                cache.clear();
                let count = url
                    .list_all_files(&state, &store, ".parquet")
                    .await
                    .unwrap()
                    .take(take)
                    .try_fold(0usize, |count, _| future::ready(Ok(count + 1)))
                    .await
                    .unwrap();
                assert_eq!(count, expected_files);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
