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

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::{
    datasource::{
        file_format::{csv::CsvFormat, parquet::ParquetFormat},
        listing::{ListingOptions, ListingTable, ListingTableConfig},
    },
    error::Result,
    physical_plan::ColumnStatistics,
    prelude::SessionContext,
    test_util::{self, arrow_test_data, parquet_test_data},
};
use datafusion_catalog::TableProvider;
use datafusion_common::stats::Precision;
use datafusion_common::test_util::batches_to_sort_string;
use datafusion_common::ScalarValue;
use datafusion_execution::config::SessionConfig;
use datafusion_expr::{col, lit, Expr, Operator};
use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::stream::{self, BoxStream};
use insta::assert_snapshot;
use object_store::{
    path::Path, GetOptions, GetResult, GetResultPayload, ListResult, ObjectMeta,
    ObjectStore, PutOptions, PutResult,
};
use object_store::{Attributes, MultipartUpload, PutMultipartOpts, PutPayload};
use url::Url;

#[tokio::test]
async fn parquet_partition_pruning_filter() -> Result<()> {
    let ctx = SessionContext::new();

    let table = create_partitioned_alltypes_parquet_table(
        &ctx,
        &[
            "year=2021/month=09/day=09/file.parquet",
            "year=2021/month=10/day=09/file.parquet",
            "year=2021/month=10/day=28/file.parquet",
        ],
        &[
            ("year", DataType::Int32),
            ("month", DataType::Int32),
            ("day", DataType::Int32),
        ],
        "mirror:///",
        "alltypes_plain.parquet",
    )
    .await;

    // The first three filters can be resolved using only the partition columns.
    let filters = [
        Expr::eq(col("year"), lit(2021)),
        Expr::eq(col("month"), lit(10)),
        Expr::eq(col("day"), lit(28)),
        Expr::gt(col("id"), lit(1)),
    ];
    let exec = table.scan(&ctx.state(), None, &filters, None).await?;
    let data_source_exec = exec.as_any().downcast_ref::<DataSourceExec>().unwrap();
    if let Some((_, parquet_config)) =
        data_source_exec.downcast_to_file_source::<ParquetSource>()
    {
        let pred = parquet_config.predicate().unwrap();
        // Only the last filter should be pushdown to TableScan
        let expected = Arc::new(BinaryExpr::new(
            Arc::new(Column::new_with_schema("id", &exec.schema()).unwrap()),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
        ));

        assert!(pred.as_any().is::<BinaryExpr>());
        let pred = pred.as_any().downcast_ref::<BinaryExpr>().unwrap();

        assert_eq!(pred, expected.as_ref());
    }
    Ok(())
}

#[tokio::test]
async fn parquet_distinct_partition_col() -> Result<()> {
    let ctx = SessionContext::new();

    register_partitioned_alltypes_parquet(
        &ctx,
        &[
            "year=2021/month=09/day=09/file.parquet",
            "year=2021/month=10/day=09/file.parquet",
            "year=2021/month=10/day=28/file.parquet",
        ],
        &[
            ("year", DataType::Int32),
            (
                "month",
                DataType::Dictionary(
                    Box::new(DataType::UInt16),
                    Box::new(DataType::Utf8),
                ),
            ),
            ("day", DataType::Utf8),
        ],
        "mirror:///",
        "alltypes_plain.parquet",
    )
    .await;
    //Test that only selecting partition columns is possible
    let result = ctx
        .sql("SELECT distinct year,month,day FROM t")
        .await?
        .collect()
        .await?;

    assert_snapshot!(batches_to_sort_string(&result), @r"
    +------+-------+-----+
    | year | month | day |
    +------+-------+-----+
    | 2021 | 09    | 09  |
    | 2021 | 10    | 09  |
    | 2021 | 10    | 28  |
    +------+-------+-----+
    ");
    //Test that the number of rows returned by partition column scan and actually reading the parquet file are the same
    let actual_row_count: usize = ctx
        .sql("SELECT id from t")
        .await?
        .collect()
        .await?
        .into_iter()
        .map(|batch| batch.num_rows())
        .sum();

    let partition_row_count: usize = ctx
        .sql("SELECT year from t")
        .await?
        .collect()
        .await?
        .into_iter()
        .map(|batch| batch.num_rows())
        .sum();
    assert_eq!(actual_row_count, partition_row_count);

    //Test limit logic. 3 test cases
    //1. limit is contained within a single partition with leftover rows
    //2. limit is contained within a single partition without leftover rows
    //3. limit is not contained within a single partition
    //The id column is included to ensure that the parquet file is actually scanned.
    let results  = ctx
        .sql("SELECT count(*) as num_rows_per_month, month, MAX(id) from t group by month order by num_rows_per_month desc")
        .await?
        .collect()
        .await?;

    let mut max_limit = match ScalarValue::try_from_array(results[0].column(0), 0)? {
        ScalarValue::Int64(Some(count)) => count,
        s => panic!("Expected count as Int64 found {}", s.data_type()),
    };

    max_limit += 1;
    let last_batch = results
        .last()
        .expect("There should be at least one record batch returned");
    let last_row_idx = last_batch.num_rows() - 1;
    let mut min_limit =
        match ScalarValue::try_from_array(last_batch.column(0), last_row_idx)? {
            ScalarValue::Int64(Some(count)) => count,
            s => panic!("Expected count as Int64 found {}", s.data_type()),
        };

    min_limit -= 1;

    let sql_cross_partition_boundary = format!("SELECT month FROM t limit {max_limit}");
    let resulting_limit: i64 = ctx
        .sql(sql_cross_partition_boundary.as_str())
        .await?
        .collect()
        .await?
        .into_iter()
        .map(|r| r.num_rows() as i64)
        .sum();

    assert_eq!(max_limit, resulting_limit);

    let sql_within_partition_boundary = format!("SELECT month from t limit {min_limit}");
    let resulting_limit: i64 = ctx
        .sql(sql_within_partition_boundary.as_str())
        .await?
        .collect()
        .await?
        .into_iter()
        .map(|r| r.num_rows() as i64)
        .sum();

    assert_eq!(min_limit, resulting_limit);

    let s = ScalarValue::try_from_array(results[0].column(1), 0)?;
    assert!(
        matches!(s.data_type(), DataType::Dictionary(_, v) if v.as_ref() == &DataType::Utf8),
        "Expected month as Dict(_, Utf8) found {s:?}"
    );
    let month = s.try_as_str().flatten().unwrap();

    let sql_on_partition_boundary = format!(
        "SELECT month from t where month = '{}' LIMIT {}",
        month,
        max_limit - 1
    );
    let resulting_limit: i64 = ctx
        .sql(sql_on_partition_boundary.as_str())
        .await?
        .collect()
        .await?
        .into_iter()
        .map(|r| r.num_rows() as i64)
        .sum();
    let partition_row_count = max_limit - 1;
    assert_eq!(partition_row_count, resulting_limit);
    Ok(())
}

#[tokio::test]
async fn csv_filter_with_file_col() -> Result<()> {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().set_str("datafusion.catalog.has_header", "true"),
    );

    register_partitioned_aggregate_csv(
        &ctx,
        &[
            "mytable/date=2021-10-27/file.csv",
            "mytable/date=2021-10-28/file.csv",
        ],
        &[("date", DataType::Utf8)],
        "mirror:///mytable/",
    );

    let result = ctx
        .sql("SELECT c1, c2 FROM t WHERE date='2021-10-27' and c1!='2021-10-27' LIMIT 5")
        .await?
        .collect()
        .await?;

    assert_snapshot!(batches_to_sort_string(&result), @r"
    +----+----+
    | c1 | c2 |
    +----+----+
    | a  | 1  |
    | b  | 1  |
    | b  | 5  |
    | c  | 2  |
    | d  | 5  |
    +----+----+
    ");

    Ok(())
}

#[tokio::test]
async fn csv_filter_with_file_nonstring_col() -> Result<()> {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().set_str("datafusion.catalog.has_header", "true"),
    );

    register_partitioned_aggregate_csv(
        &ctx,
        &[
            "mytable/date=2021-10-27/file.csv",
            "mytable/date=2021-10-28/file.csv",
        ],
        &[("date", DataType::Date32)],
        "mirror:///mytable/",
    );

    let result = ctx
        .sql("SELECT c1, c2, date FROM t WHERE date > '2021-10-27' LIMIT 5")
        .await?
        .collect()
        .await?;

    assert_snapshot!(batches_to_sort_string(&result), @r"
    +----+----+------------+
    | c1 | c2 | date       |
    +----+----+------------+
    | a  | 1  | 2021-10-28 |
    | b  | 1  | 2021-10-28 |
    | b  | 5  | 2021-10-28 |
    | c  | 2  | 2021-10-28 |
    | d  | 5  | 2021-10-28 |
    +----+----+------------+
    ");

    Ok(())
}

#[tokio::test]
async fn csv_projection_on_partition() -> Result<()> {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().set_str("datafusion.catalog.has_header", "true"),
    );

    register_partitioned_aggregate_csv(
        &ctx,
        &[
            "mytable/date=2021-10-27/file.csv",
            "mytable/date=2021-10-28/file.csv",
        ],
        &[("date", DataType::Date32)],
        "mirror:///mytable/",
    );

    let result = ctx
        .sql("SELECT c1, date FROM t WHERE date='2021-10-27' LIMIT 5")
        .await?
        .collect()
        .await?;

    assert_snapshot!(batches_to_sort_string(&result), @r"
    +----+------------+
    | c1 | date       |
    +----+------------+
    | a  | 2021-10-27 |
    | b  | 2021-10-27 |
    | b  | 2021-10-27 |
    | c  | 2021-10-27 |
    | d  | 2021-10-27 |
    +----+------------+
    ");

    Ok(())
}

#[tokio::test]
async fn csv_grouping_by_partition() -> Result<()> {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().set_str("datafusion.catalog.has_header", "true"),
    );

    register_partitioned_aggregate_csv(
        &ctx,
        &[
            "mytable/date=2021-10-26/file.csv",
            "mytable/date=2021-10-27/file.csv",
            "mytable/date=2021-10-28/file.csv",
        ],
        &[("date", DataType::Date32)],
        "mirror:///mytable/",
    );

    let result = ctx
        .sql("SELECT date, count(*), count(distinct(c1)) FROM t WHERE date<='2021-10-27' GROUP BY date")
        .await?
        .collect()
        .await?;

    assert_snapshot!(batches_to_sort_string(&result), @r"
    +------------+----------+----------------------+
    | date       | count(*) | count(DISTINCT t.c1) |
    +------------+----------+----------------------+
    | 2021-10-26 | 100      | 5                    |
    | 2021-10-27 | 100      | 5                    |
    +------------+----------+----------------------+
    ");

    Ok(())
}

#[tokio::test]
async fn parquet_multiple_partitions() -> Result<()> {
    let ctx = SessionContext::new();

    register_partitioned_alltypes_parquet(
        &ctx,
        &[
            "year=2021/month=09/day=09/file.parquet",
            "year=2021/month=10/day=09/file.parquet",
            "year=2021/month=10/day=28/file.parquet",
        ],
        &[
            ("year", DataType::Utf8),
            ("month", DataType::Utf8),
            ("day", DataType::Utf8),
        ],
        "mirror:///",
        "alltypes_plain.parquet",
    )
    .await;

    let result = ctx
        .sql("SELECT id, day FROM t WHERE day=month ORDER BY id")
        .await?
        .collect()
        .await?;

    assert_snapshot!(batches_to_sort_string(&result), @r"
    +----+-----+
    | id | day |
    +----+-----+
    | 0  | 09  |
    | 1  | 09  |
    | 2  | 09  |
    | 3  | 09  |
    | 4  | 09  |
    | 5  | 09  |
    | 6  | 09  |
    | 7  | 09  |
    +----+-----+
    ");

    Ok(())
}

#[tokio::test]
async fn parquet_multiple_nonstring_partitions() -> Result<()> {
    let ctx = SessionContext::new();

    register_partitioned_alltypes_parquet(
        &ctx,
        &[
            "year=2021/month=09/day=09/file.parquet",
            "year=2021/month=10/day=09/file.parquet",
            "year=2021/month=10/day=28/file.parquet",
        ],
        &[
            ("year", DataType::Int32),
            ("month", DataType::Int32),
            ("day", DataType::Int32),
        ],
        "mirror:///",
        "alltypes_plain.parquet",
    )
    .await;

    let result = ctx
        .sql("SELECT id, day FROM t WHERE day=month ORDER BY id")
        .await?
        .collect()
        .await?;

    assert_snapshot!(batches_to_sort_string(&result), @r"
    +----+-----+
    | id | day |
    +----+-----+
    | 0  | 9   |
    | 1  | 9   |
    | 2  | 9   |
    | 3  | 9   |
    | 4  | 9   |
    | 5  | 9   |
    | 6  | 9   |
    | 7  | 9   |
    +----+-----+
    ");

    Ok(())
}

#[tokio::test]
async fn parquet_statistics() -> Result<()> {
    let ctx = SessionContext::new();

    register_partitioned_alltypes_parquet(
        &ctx,
        &[
            "year=2021/month=09/day=09/file.parquet",
            "year=2021/month=10/day=09/file.parquet",
            "year=2021/month=10/day=28/file.parquet",
        ],
        &[
            ("year", DataType::Int32),
            ("month", DataType::Utf8),
            ("day", DataType::Utf8),
        ],
        "mirror:///",
        // This is the only file we found in the test set with
        // actual stats. It has 1 column / 1 row.
        "single_nan.parquet",
    )
    .await;

    //// NO PROJECTION ////
    let dataframe = ctx.sql("SELECT * FROM t").await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let schema = physical_plan.schema();
    assert_eq!(schema.fields().len(), 4);

    let stat_cols = physical_plan.statistics()?.column_statistics;
    assert_eq!(stat_cols.len(), 4);
    // stats for the first col are read from the parquet file
    assert_eq!(stat_cols[0].null_count, Precision::Exact(3));
    // TODO assert partition column (1,2,3) stats once implemented (#1186)
    assert_eq!(stat_cols[1], ColumnStatistics::new_unknown(),);
    assert_eq!(stat_cols[2], ColumnStatistics::new_unknown(),);
    assert_eq!(stat_cols[3], ColumnStatistics::new_unknown(),);

    //// WITH PROJECTION ////
    let dataframe = ctx.sql("SELECT mycol, day FROM t WHERE day='28'").await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let schema = physical_plan.schema();
    assert_eq!(schema.fields().len(), 2);

    let stat_cols = physical_plan.statistics()?.column_statistics;
    assert_eq!(stat_cols.len(), 2);
    // stats for the first col are read from the parquet file
    assert_eq!(stat_cols[0].null_count, Precision::Exact(1));
    // TODO assert partition column stats once implemented (#1186)
    assert_eq!(stat_cols[1], ColumnStatistics::new_unknown());

    Ok(())
}

#[tokio::test]
async fn parquet_overlapping_columns() -> Result<()> {
    let ctx = SessionContext::new();

    // `id` is both a column of the file and a partitioning col
    register_partitioned_alltypes_parquet(
        &ctx,
        &[
            "id=1/file.parquet",
            "id=2/file.parquet",
            "id=3/file.parquet",
        ],
        &[("id", DataType::Int64)],
        "mirror:///",
        "alltypes_plain.parquet",
    )
    .await;

    let result = ctx.sql("SELECT id FROM t WHERE id=1 ORDER BY id").await;

    assert!(
        result.is_err(),
        "Duplicate qualified name should raise error"
    );
    Ok(())
}

fn register_partitioned_aggregate_csv(
    ctx: &SessionContext,
    store_paths: &[&str],
    partition_cols: &[(&str, DataType)],
    table_path: &str,
) {
    let testdata = arrow_test_data();
    let csv_file_path = format!("{testdata}/csv/aggregate_test_100.csv");
    let file_schema = test_util::aggr_test_schema();
    let url = Url::parse("mirror://").unwrap();
    ctx.register_object_store(
        &url,
        MirroringObjectStore::new_arc(csv_file_path, store_paths),
    );

    let options = ListingOptions::new(Arc::new(CsvFormat::default()))
        .with_table_partition_cols(
            partition_cols
                .iter()
                .map(|x| (x.0.to_owned(), x.1.clone()))
                .collect::<Vec<_>>(),
        );

    let table_path = ListingTableUrl::parse(table_path).unwrap();
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(options)
        .with_schema(file_schema);
    let table = ListingTable::try_new(config).unwrap();

    ctx.register_table("t", Arc::new(table))
        .expect("registering listing table failed");
}

async fn register_partitioned_alltypes_parquet(
    ctx: &SessionContext,
    store_paths: &[&str],
    partition_cols: &[(&str, DataType)],
    table_path: &str,
    source_file: &str,
) {
    let table = create_partitioned_alltypes_parquet_table(
        ctx,
        store_paths,
        partition_cols,
        table_path,
        source_file,
    )
    .await;
    ctx.register_table("t", table)
        .expect("registering listing table failed");
}

async fn create_partitioned_alltypes_parquet_table(
    ctx: &SessionContext,
    store_paths: &[&str],
    partition_cols: &[(&str, DataType)],
    table_path: &str,
    source_file: &str,
) -> Arc<dyn TableProvider> {
    let testdata = parquet_test_data();
    let parquet_file_path = format!("{testdata}/{source_file}");
    let url = Url::parse("mirror://").unwrap();
    ctx.register_object_store(
        &url,
        MirroringObjectStore::new_arc(parquet_file_path.clone(), store_paths),
    );

    let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_table_partition_cols(
            partition_cols
                .iter()
                .map(|x| (x.0.to_owned(), x.1.clone()))
                .collect::<Vec<_>>(),
        );

    let table_path = ListingTableUrl::parse(table_path).unwrap();
    let store_path =
        ListingTableUrl::parse(format!("mirror:///{}", store_paths[0])).unwrap();

    let file_schema = options
        .infer_schema(&ctx.state(), &store_path)
        .await
        .expect("Parquet schema inference failed");

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(options)
        .with_schema(file_schema);
    Arc::new(ListingTable::try_new(config).unwrap())
}

#[derive(Debug)]
/// An object store implem that is mirrors a given file to multiple paths.
pub struct MirroringObjectStore {
    /// The `(path,size)` of the files that "exist" in the store
    files: Vec<Path>,
    /// The file that will be read at all path
    mirrored_file: String,
    /// Size of the mirrored file
    file_size: u64,
}

impl std::fmt::Display for MirroringObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl MirroringObjectStore {
    pub fn new_arc(mirrored_file: String, paths: &[&str]) -> Arc<dyn ObjectStore> {
        let metadata = std::fs::metadata(&mirrored_file).expect("Local file metadata");
        Arc::new(Self {
            files: paths.iter().map(|f| Path::parse(f).unwrap()).collect(),
            mirrored_file,
            file_size: metadata.len(),
        })
    }
}

#[async_trait]
impl ObjectStore for MirroringObjectStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _put_payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        unimplemented!()
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        unimplemented!()
    }

    async fn get_opts(
        &self,
        location: &Path,
        _options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.files.iter().find(|x| *x == location).unwrap();
        let path = std::path::PathBuf::from(&self.mirrored_file);
        let file = File::open(&path).unwrap();
        let metadata = file.metadata().unwrap();
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: metadata.modified().map(chrono::DateTime::from).unwrap(),
            size: metadata.len() as usize,
            e_tag: None,
            version: None,
        };

        Ok(GetResult {
            range: 0..meta.size,
            payload: GetResultPayload::File(file, path),
            meta,
            attributes: Attributes::default(),
        })
    }

    async fn get_range(
        &self,
        location: &Path,
        range: Range<usize>,
    ) -> object_store::Result<Bytes> {
        self.files.iter().find(|x| *x == location).unwrap();
        let path = std::path::PathBuf::from(&self.mirrored_file);
        let mut file = File::open(path).unwrap();
        file.seek(SeekFrom::Start(range.start as u64)).unwrap();

        let to_read = range.end - range.start;
        let mut data = Vec::with_capacity(to_read);
        let read = file.take(to_read as u64).read_to_end(&mut data).unwrap();
        assert_eq!(read, to_read);

        Ok(data.into())
    }

    async fn delete(&self, _location: &Path) -> object_store::Result<()> {
        unimplemented!()
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        let prefix = prefix.cloned().unwrap_or_default();
        Box::pin(stream::iter(self.files.iter().filter_map(
            move |location| {
                // Don't return for exact prefix match
                let filter = location
                    .prefix_match(&prefix)
                    .map(|mut x| x.next().is_some())
                    .unwrap_or(false);

                filter.then(|| {
                    Ok(ObjectMeta {
                        location: location.clone(),
                        last_modified: Utc.timestamp_nanos(0),
                        size: self.file_size as usize,
                        e_tag: None,
                        version: None,
                    })
                })
            },
        )))
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        let root = Path::default();
        let prefix = prefix.unwrap_or(&root);

        let mut common_prefixes = BTreeSet::new();
        let mut objects = vec![];

        for k in &self.files {
            let mut parts = match k.prefix_match(prefix) {
                Some(parts) => parts,
                None => continue,
            };

            // Pop first element
            let common_prefix = match parts.next() {
                Some(p) => p,
                // Should only return children of the prefix
                None => continue,
            };

            if parts.next().is_some() {
                common_prefixes.insert(prefix.child(common_prefix));
            } else {
                let object = ObjectMeta {
                    location: k.clone(),
                    last_modified: Utc.timestamp_nanos(0),
                    size: self.file_size as usize,
                    e_tag: None,
                    version: None,
                };
                objects.push(object);
            }
        }
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        unimplemented!()
    }

    async fn copy_if_not_exists(
        &self,
        _from: &Path,
        _to: &Path,
    ) -> object_store::Result<()> {
        unimplemented!()
    }
}
