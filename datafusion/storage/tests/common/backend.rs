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

use bytes::Bytes;
use datafusion_storage::{path::Path, *};
use futures::TryStreamExt;
use tokio::io::AsyncWriteExt;

pub async fn backend_contract(storage: &dyn Storage) {
    let context = FileAccessContext::new("backend-contract");
    let path = Path::from("data.parquet");
    let mut writer = storage
        .writer(
            &path,
            WriterOptions {
                buffer_size: Some(8),
            },
            context.clone(),
        )
        .await
        .unwrap();
    writer.write_all(b"01234").await.unwrap();
    writer.flush().await.unwrap();
    writer.write_all(b"56789").await.unwrap();
    writer.shutdown().await.unwrap();
    assert_eq!(storage.stat(&path, &context).await.unwrap().size, 10);
    let reader = storage.open(&path, context.clone()).await.unwrap();
    assert_eq!(reader.read_range((2..5).into()).await.unwrap(), b"234"[..]);
    assert_eq!(
        reader.read_range(ReadRange::Suffix(3)).await.unwrap(),
        b"789"[..]
    );
    assert_eq!(
        reader
            .read_ranges(vec![7..10, 1..5, 1..5, 4..8])
            .await
            .unwrap(),
        vec![
            Bytes::from_static(b"789"),
            Bytes::from_static(b"1234"),
            Bytes::from_static(b"1234"),
            Bytes::from_static(b"4567"),
        ]
    );
    assert_eq!(
        collect_bytes(reader.clone().stream(Some((2..8).into())))
            .await
            .unwrap(),
        b"234567"[..]
    );
    assert_eq!(
        collect_bytes(reader.stream(None)).await.unwrap(),
        b"0123456789"[..]
    );
    for name in [
        "events/part.parquet",
        "events/day/part.parquet",
        "events-other/part.parquet",
    ] {
        let mut writer = storage
            .writer(&Path::from(name), WriterOptions::default(), context.clone())
            .await
            .unwrap();
        writer.write_all(b"data").await.unwrap();
        writer.shutdown().await.unwrap();
    }
    let files = storage
        .list(&Path::from("events"), context.clone())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(files.len(), 2);
    let children = storage
        .list_with_delimiter(&Path::from("events"), &context)
        .await
        .unwrap();
    assert_eq!(children.files.len(), 1);
    assert_eq!(
        children.files[0].location,
        Path::from("events/part.parquet")
    );
    assert_eq!(children.directories, vec![Path::from("events/day")]);
}
