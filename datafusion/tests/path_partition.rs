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

//! Test queries on partitioned datasets

use std::{fs, io, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    assert_batches_sorted_eq,
    datasource::{
        file_format::csv::CsvFormat,
        listing::{ListingOptions, ListingTable},
        object_store::{
            local::LocalFileSystem, FileMeta, FileMetaStream, ListEntryStream,
            ObjectReader, ObjectStore, SizedFile,
        },
    },
    error::{DataFusionError, Result},
    prelude::ExecutionContext,
    test_util::arrow_test_data,
};
use futures::{stream, StreamExt};

mod common;

#[tokio::test]
async fn csv_filter_with_file_col() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    register_partitioned_aggregate_csv(
        &mut ctx,
        &[
            "mytable/date=2021-10-27/file.csv",
            "mytable/date=2021-10-28/file.csv",
        ],
        &["date"],
        "mytable",
    );

    let result = ctx
        .sql("SELECT c1, c2 FROM t WHERE date='2021-10-27' and date!=c1 LIMIT 5")
        .await?
        .collect()
        .await?;

    let expected = vec![
        "+----+----+",
        "| c1 | c2 |",
        "+----+----+",
        "| a  | 1  |",
        "| b  | 1  |",
        "| b  | 5  |",
        "| c  | 2  |",
        "| d  | 5  |",
        "+----+----+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    Ok(())
}

#[tokio::test]
async fn csv_projection_on_partition() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    register_partitioned_aggregate_csv(
        &mut ctx,
        &[
            "mytable/date=2021-10-27/file.csv",
            "mytable/date=2021-10-28/file.csv",
        ],
        &["date"],
        "mytable",
    );

    let result = ctx
        .sql("SELECT c1, date FROM t WHERE date='2021-10-27' LIMIT 5")
        .await?
        .collect()
        .await?;

    let expected = vec![
        "+----+------------+",
        "| c1 | date       |",
        "+----+------------+",
        "| a  | 2021-10-27 |",
        "| b  | 2021-10-27 |",
        "| b  | 2021-10-27 |",
        "| c  | 2021-10-27 |",
        "| d  | 2021-10-27 |",
        "+----+------------+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    Ok(())
}

#[tokio::test]
async fn csv_grouping_by_partition() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    register_partitioned_aggregate_csv(
        &mut ctx,
        &[
            "mytable/date=2021-10-26/file.csv",
            "mytable/date=2021-10-27/file.csv",
            "mytable/date=2021-10-28/file.csv",
        ],
        &["date"],
        "mytable",
    );

    let result = ctx
        .sql("SELECT date, count(*), count(distinct(c1)) FROM t WHERE date<='2021-10-27' GROUP BY date")
        .await?
        .collect()
        .await?;

    let expected = vec![
        "+------------+-----------------+----------------------+",
        "| date       | COUNT(UInt8(1)) | COUNT(DISTINCT t.c1) |",
        "+------------+-----------------+----------------------+",
        "| 2021-10-26 | 100             | 5                    |",
        "| 2021-10-27 | 100             | 5                    |",
        "+------------+-----------------+----------------------+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    Ok(())
}

fn register_partitioned_aggregate_csv(
    ctx: &mut ExecutionContext,
    store_paths: &[&str],
    partition_cols: &[&str],
    table_path: &str,
) {
    let testdata = arrow_test_data();
    let csv_file_path = format!("{}/csv/aggregate_test_100.csv", testdata);
    let file_schema = common::aggr_test_schema();
    let object_store = MirroringObjectStore::new_arc(csv_file_path, store_paths);

    let mut options = ListingOptions::new(Arc::new(CsvFormat::default()));
    options.table_partition_cols = partition_cols.iter().map(|&s| s.to_owned()).collect();

    let table =
        ListingTable::new(object_store, table_path.to_owned(), file_schema, options);

    ctx.register_table("t", Arc::new(table))
        .expect("registering listing table failed");
}

#[derive(Debug)]
/// An object store implem that is mirrors a given file to multiple paths.
pub struct MirroringObjectStore {
    /// The `(path,size)` of the files that "exist" in the store
    files: Vec<String>,
    /// The file that will be read at all path
    mirrored_file: String,
    /// Size of the mirrored file
    file_size: u64,
}

impl MirroringObjectStore {
    pub fn new_arc(mirrored_file: String, paths: &[&str]) -> Arc<dyn ObjectStore> {
        let metadata = fs::metadata(&mirrored_file).expect("Local file metadata");
        Arc::new(Self {
            files: paths.iter().map(|&f| f.to_owned()).collect(),
            mirrored_file,
            file_size: metadata.len(),
        })
    }
}

#[async_trait]
impl ObjectStore for MirroringObjectStore {
    async fn list_file(&self, prefix: &str) -> Result<FileMetaStream> {
        let prefix = prefix.to_owned();
        let size = self.file_size;
        Ok(Box::pin(
            stream::iter(
                self.files
                    .clone()
                    .into_iter()
                    .filter(move |f| f.starts_with(&prefix)),
            )
            .map(move |f| {
                Ok(FileMeta {
                    sized_file: SizedFile { path: f, size },
                    last_modified: None,
                })
            }),
        ))
    }

    async fn list_dir(
        &self,
        _prefix: &str,
        _delimiter: Option<String>,
    ) -> Result<ListEntryStream> {
        unimplemented!()
    }

    fn file_reader(&self, file: SizedFile) -> Result<Arc<dyn ObjectReader>> {
        assert_eq!(
            self.file_size, file.size,
            "Requested files should have the same size as the mirrored file"
        );
        match self.files.iter().find(|&item| &file.path == item) {
            Some(_) => Ok(LocalFileSystem {}.file_reader(SizedFile {
                path: self.mirrored_file.clone(),
                size: self.file_size,
            })?),
            None => Err(DataFusionError::IoError(io::Error::new(
                io::ErrorKind::NotFound,
                "not in provided test list",
            ))),
        }
    }
}
