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
        file_format::{csv::CsvFormat, parquet::ParquetFormat},
        listing::{ListingOptions, ListingTable, ListingTableConfig},
        object_store::{
            local::LocalFileSystem, FileMeta, FileMetaStream, ListEntryStream,
            ObjectReader, ObjectStore, SizedFile,
        },
    },
    error::{DataFusionError, Result},
    physical_plan::ColumnStatistics,
    prelude::SessionContext,
    test_util::{self, arrow_test_data, parquet_test_data},
};
use futures::{stream, StreamExt};

#[tokio::test]
async fn csv_filter_with_file_col() -> Result<()> {
    let mut ctx = SessionContext::new();

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
    let mut ctx = SessionContext::new();

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
    let mut ctx = SessionContext::new();

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

#[tokio::test]
async fn parquet_multiple_partitions() -> Result<()> {
    let mut ctx = SessionContext::new();

    register_partitioned_alltypes_parquet(
        &mut ctx,
        &[
            "year=2021/month=09/day=09/file.parquet",
            "year=2021/month=10/day=09/file.parquet",
            "year=2021/month=10/day=28/file.parquet",
        ],
        &["year", "month", "day"],
        "",
        "alltypes_plain.parquet",
    )
    .await;

    let result = ctx
        .sql("SELECT id, day FROM t WHERE day=month ORDER BY id")
        .await?
        .collect()
        .await?;

    let expected = vec![
        "+----+-----+",
        "| id | day |",
        "+----+-----+",
        "| 0  | 09  |",
        "| 1  | 09  |",
        "| 2  | 09  |",
        "| 3  | 09  |",
        "| 4  | 09  |",
        "| 5  | 09  |",
        "| 6  | 09  |",
        "| 7  | 09  |",
        "+----+-----+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    Ok(())
}

#[tokio::test]
async fn parquet_statistics() -> Result<()> {
    let mut ctx = SessionContext::new();

    register_partitioned_alltypes_parquet(
        &mut ctx,
        &[
            "year=2021/month=09/day=09/file.parquet",
            "year=2021/month=10/day=09/file.parquet",
            "year=2021/month=10/day=28/file.parquet",
        ],
        &["year", "month", "day"],
        "",
        // This is the only file we found in the test set with
        // actual stats. It has 1 column / 1 row.
        "single_nan.parquet",
    )
    .await;

    //// NO PROJECTION ////
    let logical_plan = ctx.sql("SELECT * FROM t").await?.to_logical_plan();

    let physical_plan = ctx.create_physical_plan(&logical_plan).await?;
    assert_eq!(physical_plan.schema().fields().len(), 4);

    let stat_cols = physical_plan
        .statistics()
        .column_statistics
        .expect("col stats should be defined");
    assert_eq!(stat_cols.len(), 4);
    // stats for the first col are read from the parquet file
    assert_eq!(stat_cols[0].null_count, Some(3));
    // TODO assert partition column (1,2,3) stats once implemented (#1186)
    assert_eq!(stat_cols[1], ColumnStatistics::default());
    assert_eq!(stat_cols[2], ColumnStatistics::default());
    assert_eq!(stat_cols[3], ColumnStatistics::default());

    //// WITH PROJECTION ////
    let logical_plan = ctx
        .sql("SELECT mycol, day FROM t WHERE day='28'")
        .await?
        .to_logical_plan();

    let physical_plan = ctx.create_physical_plan(&logical_plan).await?;
    assert_eq!(physical_plan.schema().fields().len(), 2);

    let stat_cols = physical_plan
        .statistics()
        .column_statistics
        .expect("col stats should be defined");
    assert_eq!(stat_cols.len(), 2);
    // stats for the first col are read from the parquet file
    assert_eq!(stat_cols[0].null_count, Some(1));
    // TODO assert partition column stats once implemented (#1186)
    assert_eq!(stat_cols[1], ColumnStatistics::default());

    Ok(())
}

#[tokio::test]
async fn parquet_overlapping_columns() -> Result<()> {
    let mut ctx = SessionContext::new();

    // `id` is both a column of the file and a partitioning col
    register_partitioned_alltypes_parquet(
        &mut ctx,
        &[
            "id=1/file.parquet",
            "id=2/file.parquet",
            "id=3/file.parquet",
        ],
        &["id"],
        "",
        "alltypes_plain.parquet",
    )
    .await;

    let result = ctx.sql("SELECT id FROM t WHERE id=1 ORDER BY id").await;

    assert!(
        result.is_err(),
        "Dupplicate qualified name should raise error"
    );
    Ok(())
}

fn register_partitioned_aggregate_csv(
    ctx: &mut SessionContext,
    store_paths: &[&str],
    partition_cols: &[&str],
    table_path: &str,
) {
    let testdata = arrow_test_data();
    let csv_file_path = format!("{}/csv/aggregate_test_100.csv", testdata);
    let file_schema = test_util::aggr_test_schema();
    let object_store = MirroringObjectStore::new_arc(csv_file_path, store_paths);

    let mut options = ListingOptions::new(Arc::new(CsvFormat::default()));
    options.table_partition_cols = partition_cols.iter().map(|&s| s.to_owned()).collect();

    let config = ListingTableConfig::new(object_store, table_path)
        .with_listing_options(options)
        .with_schema(file_schema);
    let table = ListingTable::try_new(config).unwrap();

    ctx.register_table("t", Arc::new(table))
        .expect("registering listing table failed");
}

async fn register_partitioned_alltypes_parquet(
    ctx: &mut SessionContext,
    store_paths: &[&str],
    partition_cols: &[&str],
    table_path: &str,
    source_file: &str,
) {
    let testdata = parquet_test_data();
    let parquet_file_path = format!("{}/{}", testdata, source_file);
    let object_store =
        MirroringObjectStore::new_arc(parquet_file_path.clone(), store_paths);

    let mut options = ListingOptions::new(Arc::new(ParquetFormat::default()));
    options.table_partition_cols = partition_cols.iter().map(|&s| s.to_owned()).collect();
    options.collect_stat = true;

    let file_schema = options
        .infer_schema(Arc::clone(&object_store), store_paths[0])
        .await
        .expect("Parquet schema inference failed");

    let config = ListingTableConfig::new(object_store, table_path)
        .with_listing_options(options)
        .with_schema(file_schema);

    let table = ListingTable::try_new(config).unwrap();

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
