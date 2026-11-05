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

use std::sync::Arc;

use arrow_schema::DataType;
use futures::{FutureExt, StreamExt as _, TryStreamExt as _};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionConfig;
use datafusion_catalog_listing::helpers::{
    describe_partition, list_partitions, pruned_partition_list,
};
use datafusion_common::ScalarValue;
use datafusion_datasource::ListingTableUrl;
use datafusion_expr::{Expr, col, lit};
use datafusion_session::Session;

#[tokio::test]
async fn test_pruned_partition_list_empty() {
    let (store, state) = make_test_store_and_state(&[
        ("tablepath/mypartition=val1/notparquetfile", 100),
        ("tablepath/mypartition=val1/ignoresemptyfile.parquet", 0),
        ("tablepath/file.parquet", 100),
        ("tablepath/notapartition/file.parquet", 100),
        ("tablepath/notmypartition=val1/file.parquet", 100),
    ]);
    let filter = Expr::eq(col("mypartition"), lit("val1"));
    let pruned = pruned_partition_list(
        state.as_ref(),
        store.as_ref(),
        &ListingTableUrl::parse("file:///tablepath/").unwrap(),
        &[filter],
        ".parquet",
        &[(String::from("mypartition"), DataType::Utf8)],
    )
    .await
    .expect("partition pruning failed")
    .collect::<Vec<_>>()
    .await;

    assert_eq!(pruned.len(), 0);
}

#[tokio::test]
async fn test_pruned_partition_list() {
    let (store, state) = make_test_store_and_state(&[
        ("tablepath/mypartition=val1/file.parquet", 100),
        ("tablepath/mypartition=val2/file.parquet", 100),
        ("tablepath/mypartition=val1/ignoresemptyfile.parquet", 0),
        ("tablepath/mypartition=val1/other=val3/file.parquet", 100),
        ("tablepath/notapartition/file.parquet", 100),
        ("tablepath/notmypartition=val1/file.parquet", 100),
    ]);
    let filter = Expr::eq(col("mypartition"), lit("val1"));
    let pruned = pruned_partition_list(
        state.as_ref(),
        store.as_ref(),
        &ListingTableUrl::parse("file:///tablepath/").unwrap(),
        &[filter],
        ".parquet",
        &[(String::from("mypartition"), DataType::Utf8)],
    )
    .await
    .expect("partition pruning failed")
    .try_collect::<Vec<_>>()
    .await
    .unwrap();

    assert_eq!(pruned.len(), 2);
    let f1 = &pruned[0];
    assert_eq!(
        f1.object_meta.location.as_ref(),
        "tablepath/mypartition=val1/file.parquet"
    );
    assert_eq!(&f1.partition_values, &[ScalarValue::from("val1")]);
    let f2 = &pruned[1];
    assert_eq!(
        f2.object_meta.location.as_ref(),
        "tablepath/mypartition=val1/other=val3/file.parquet"
    );
    assert_eq!(f2.partition_values, &[ScalarValue::from("val1"),]);
}

#[tokio::test]
async fn test_pruned_partition_list_multi() {
    let (store, state) = make_test_store_and_state(&[
        ("tablepath/part1=p1v1/file.parquet", 100),
        ("tablepath/part1=p1v2/part2=p2v1/file1.parquet", 100),
        ("tablepath/part1=p1v2/part2=p2v1/file2.parquet", 100),
        ("tablepath/part1=p1v3/part2=p2v1/file2.parquet", 100),
        ("tablepath/part1=p1v2/part2=p2v2/file2.parquet", 100),
    ]);
    let filter1 = Expr::eq(col("part1"), lit("p1v2"));
    let filter2 = Expr::eq(col("part2"), lit("p2v1"));
    let pruned = pruned_partition_list(
        state.as_ref(),
        store.as_ref(),
        &ListingTableUrl::parse("file:///tablepath/").unwrap(),
        &[filter1, filter2],
        ".parquet",
        &[
            (String::from("part1"), DataType::Utf8),
            (String::from("part2"), DataType::Utf8),
        ],
    )
    .await
    .expect("partition pruning failed")
    .try_collect::<Vec<_>>()
    .await
    .unwrap();

    assert_eq!(pruned.len(), 2);
    let f1 = &pruned[0];
    assert_eq!(
        f1.object_meta.location.as_ref(),
        "tablepath/part1=p1v2/part2=p2v1/file1.parquet"
    );
    assert_eq!(
        &f1.partition_values,
        &[ScalarValue::from("p1v2"), ScalarValue::from("p2v1"),]
    );
    let f2 = &pruned[1];
    assert_eq!(
        f2.object_meta.location.as_ref(),
        "tablepath/part1=p1v2/part2=p2v1/file2.parquet"
    );
    assert_eq!(
        &f2.partition_values,
        &[ScalarValue::from("p1v2"), ScalarValue::from("p2v1")]
    );
}

#[tokio::test]
async fn test_list_partition() {
    let (store, _) = make_test_store_and_state(&[
        ("tablepath/part1=p1v1/file.parquet", 100),
        ("tablepath/part1=p1v2/part2=p2v1/file1.parquet", 100),
        ("tablepath/part1=p1v2/part2=p2v1/file2.parquet", 100),
        ("tablepath/part1=p1v3/part2=p2v1/file3.parquet", 100),
        ("tablepath/part1=p1v2/part2=p2v2/file4.parquet", 100),
        ("tablepath/part1=p1v2/part2=p2v2/empty.parquet", 0),
    ]);

    let partitions = list_partitions(
        store.as_ref(),
        &ListingTableUrl::parse("file:///tablepath/").unwrap(),
        0,
        None,
    )
    .await
    .expect("listing partitions failed");

    assert_eq!(
        &partitions
            .iter()
            .map(describe_partition)
            .collect::<Vec<_>>(),
        &vec![
            ("tablepath", 0, vec![]),
            ("tablepath/part1=p1v1", 1, vec![]),
            ("tablepath/part1=p1v2", 1, vec![]),
            ("tablepath/part1=p1v3", 1, vec![]),
        ]
    );

    let partitions = list_partitions(
        store.as_ref(),
        &ListingTableUrl::parse("file:///tablepath/").unwrap(),
        1,
        None,
    )
    .await
    .expect("listing partitions failed");

    assert_eq!(
        &partitions
            .iter()
            .map(describe_partition)
            .collect::<Vec<_>>(),
        &vec![
            ("tablepath", 0, vec![]),
            ("tablepath/part1=p1v1", 1, vec!["file.parquet"]),
            ("tablepath/part1=p1v2", 1, vec![]),
            ("tablepath/part1=p1v2/part2=p2v1", 2, vec![]),
            ("tablepath/part1=p1v2/part2=p2v2", 2, vec![]),
            ("tablepath/part1=p1v3", 1, vec![]),
            ("tablepath/part1=p1v3/part2=p2v1", 2, vec![]),
        ]
    );

    let partitions = list_partitions(
        store.as_ref(),
        &ListingTableUrl::parse("file:///tablepath/").unwrap(),
        2,
        None,
    )
    .await
    .expect("listing partitions failed");

    assert_eq!(
        &partitions
            .iter()
            .map(describe_partition)
            .collect::<Vec<_>>(),
        &vec![
            ("tablepath", 0, vec![]),
            ("tablepath/part1=p1v1", 1, vec!["file.parquet"]),
            ("tablepath/part1=p1v2", 1, vec![]),
            ("tablepath/part1=p1v3", 1, vec![]),
            (
                "tablepath/part1=p1v2/part2=p2v1",
                2,
                vec!["file1.parquet", "file2.parquet"]
            ),
            ("tablepath/part1=p1v2/part2=p2v2", 2, vec!["file4.parquet"]),
            ("tablepath/part1=p1v3/part2=p2v1", 2, vec!["file3.parquet"]),
        ]
    );
}

/// A partition directory named `__HIVE_DEFAULT_PARTITION__` holds the rows whose partition
/// value is null, so it is read as NULL rather than parsed as the column's type.
#[tokio::test]
async fn test_pruned_partition_list_null_partition() {
    let (store, state) = make_test_store_and_state(&[
        ("tablepath/mypartition=3/file.parquet", 100),
        (
            "tablepath/mypartition=__HIVE_DEFAULT_PARTITION__/file.parquet",
            100,
        ),
    ]);

    let pruned = pruned_partition_list(
        state.as_ref(),
        store.as_ref(),
        &ListingTableUrl::parse("file:///tablepath/").unwrap(),
        &[],
        ".parquet",
        &[(String::from("mypartition"), DataType::Int32)],
    )
    .await
    .expect("partition pruning failed")
    .try_collect::<Vec<_>>()
    .await
    .unwrap();

    assert_eq!(pruned.len(), 2);
    assert_eq!(&pruned[0].partition_values, &[ScalarValue::Int32(Some(3))]);
    assert_eq!(&pruned[1].partition_values, &[ScalarValue::Int32(None)]);
}

#[tokio::test]
async fn test_pruned_partition_list_prunes_null_partition() {
    let files: &[(&str, u64)] = &[
        ("tablepath/mypartition=3/file.parquet", 100),
        (
            "tablepath/mypartition=__HIVE_DEFAULT_PARTITION__/file.parquet",
            100,
        ),
    ];
    let partition_cols = &[(String::from("mypartition"), DataType::Int32)];

    let pruned_with = |filter: Expr| async move {
        let (store, state) = make_test_store_and_state(files);
        pruned_partition_list(
            state.as_ref(),
            store.as_ref(),
            &ListingTableUrl::parse("file:///tablepath/").unwrap(),
            &[filter],
            ".parquet",
            partition_cols,
        )
        .await
        .expect("partition pruning failed")
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
    };

    // The null partition does not match an equality filter: `mypartition = 3` is NULL there.
    let pruned = pruned_with(Expr::eq(col("mypartition"), lit(3i32))).await;
    assert_eq!(pruned.len(), 1);
    assert_eq!(
        pruned[0].object_meta.location.as_ref(),
        "tablepath/mypartition=3/file.parquet"
    );

    // ... and only the null partition matches `IS NULL`.
    let pruned = pruned_with(col("mypartition").is_null()).await;
    assert_eq!(pruned.len(), 1);
    assert_eq!(
        pruned[0].object_meta.location.as_ref(),
        "tablepath/mypartition=__HIVE_DEFAULT_PARTITION__/file.parquet"
    );
}

#[tokio::test]
async fn test_pruned_partition_list_null_partition_name_is_configurable() {
    let files: &[(&str, u64)] = &[
        ("tablepath/mypartition=MY_NULL/file.parquet", 100),
        (
            "tablepath/mypartition=__HIVE_DEFAULT_PARTITION__/file.parquet",
            100,
        ),
    ];

    let pruned_with_name = |name: &str| {
        let config = SessionConfig::new().set_str(
            "datafusion.execution.listing_table_hive_default_partition_name",
            name,
        );
        let (store, state) = make_test_store_and_state_with_config(files, config);
        async move {
            pruned_partition_list(
                state.as_ref(),
                store.as_ref(),
                &ListingTableUrl::parse("file:///tablepath/").unwrap(),
                &[],
                ".parquet",
                &[(String::from("mypartition"), DataType::Utf8)],
            )
            .await
            .expect("partition pruning failed")
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
        }
    };

    // A writer configured with a different placeholder, e.g. Hive's
    // `hive.exec.default.partition.name`: only that name is read as NULL.
    let pruned = pruned_with_name("MY_NULL").await;
    assert_eq!(
        &pruned[0].partition_values,
        &[ScalarValue::Utf8(None)],
        "MY_NULL should be read as NULL"
    );
    assert_eq!(
        &pruned[1].partition_values,
        &[ScalarValue::from("__HIVE_DEFAULT_PARTITION__")]
    );

    // The empty name turns the mapping off, leaving the directory name as the value.
    let pruned = pruned_with_name("").await;
    assert_eq!(&pruned[0].partition_values, &[ScalarValue::from("MY_NULL")]);
    assert_eq!(
        &pruned[1].partition_values,
        &[ScalarValue::from("__HIVE_DEFAULT_PARTITION__")]
    );
}

pub fn make_test_store_and_state(
    files: &[(&str, u64)],
) -> (Arc<InMemory>, Arc<dyn Session>) {
    make_test_store_and_state_with_config(files, SessionConfig::new())
}

pub fn make_test_store_and_state_with_config(
    files: &[(&str, u64)],
    config: SessionConfig,
) -> (Arc<InMemory>, Arc<dyn Session>) {
    let memory = InMemory::new();

    for (name, size) in files {
        memory
            .put(&Path::from(*name), vec![0; *size as usize].into())
            .now_or_never()
            .unwrap()
            .unwrap();
    }

    let state = SessionStateBuilder::new().with_config(config).build();
    (Arc::new(memory), Arc::new(state))
}
