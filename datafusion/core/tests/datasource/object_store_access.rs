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

//! Tests for object store access patterns with [`ListingTable`]\
//!
//! These tests setup a `ListingTable` backed by an in-memory object store
//! that counts the number of requests made against it and then do
//! various operations (table creation, queries with and without predicates)
//! to verify the expected object store access patterns.
//!
//! [`ListingTable`]: datafusion::datasource::listing::ListingTable

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use futures::stream::BoxStream;
use insta::assert_snapshot;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parking_lot::Mutex;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use url::Url;

#[tokio::test]
async fn create_single_csv_file() {
    assert_snapshot!(
        single_file_csv_test().await.requests(),
        @r"
    RequestCountingObjectStore()
    Total Requests: 2
    - HEAD path=csv_table.csv
    - GET  path=csv_table.csv
    "
    );
}

#[tokio::test]
async fn query_single_csv_file() {
    assert_snapshot!(
        single_file_csv_test().await.query("select * from csv_table").await,
        @r"
    ------- Query Output (2 rows) -------
    +---------+-------+-------+
    | c1      | c2    | c3    |
    +---------+-------+-------+
    | 0.00001 | 5e-12 | true  |
    | 0.00002 | 4e-12 | false |
    +---------+-------+-------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 2
    - HEAD path=csv_table.csv
    - GET  (opts) path=csv_table.csv
    "
    );
}

#[tokio::test]
async fn create_multi_file_csv_file() {
    assert_snapshot!(
        multi_file_csv_test().await.requests(),
        @r"
    RequestCountingObjectStore()
    Total Requests: 4
    - LIST prefix=data
    - GET  path=data/file_0.csv
    - GET  path=data/file_1.csv
    - GET  path=data/file_2.csv
    "
    );
}

#[tokio::test]
async fn query_multi_csv_file() {
    assert_snapshot!(
        multi_file_csv_test().await.query("select * from csv_table").await,
        @r"
    ------- Query Output (6 rows) -------
    +---------+-------+-------+
    | c1      | c2    | c3    |
    +---------+-------+-------+
    | 0.0     | 0.0   | true  |
    | 0.00003 | 5e-12 | false |
    | 0.00001 | 1e-12 | true  |
    | 0.00003 | 5e-12 | false |
    | 0.00002 | 2e-12 | true  |
    | 0.00003 | 5e-12 | false |
    +---------+-------+-------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 4
    - LIST prefix=data
    - GET  (opts) path=data/file_0.csv
    - GET  (opts) path=data/file_1.csv
    - GET  (opts) path=data/file_2.csv
    "
    );
}

#[tokio::test]
async fn create_single_parquet_file() {
    assert_snapshot!(
        single_file_parquet_test().await.requests(),
        @r"
    RequestCountingObjectStore()
    Total Requests: 4
    - HEAD path=parquet_table.parquet
    - GET  (range) range=2986-2994 path=parquet_table.parquet
    - GET  (range) range=2264-2986 path=parquet_table.parquet
    - GET  (range) range=2124-2264 path=parquet_table.parquet
    "
    );
}

#[tokio::test]
async fn query_single_parquet_file() {
    assert_snapshot!(
        single_file_parquet_test().await.query("select count(distinct a), count(b) from parquet_table").await,
        @r"
    ------- Query Output (1 rows) -------
    +---------------------------------+------------------------+
    | count(DISTINCT parquet_table.a) | count(parquet_table.b) |
    +---------------------------------+------------------------+
    | 200                             | 200                    |
    +---------------------------------+------------------------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 3
    - HEAD path=parquet_table.parquet
    - GET  (ranges) path=parquet_table.parquet ranges=4-534,534-1064
    - GET  (ranges) path=parquet_table.parquet ranges=1064-1594,1594-2124
    "
    );
}

#[tokio::test]
async fn query_single_parquet_file_with_single_predicate() {
    // Note that evaluating predicates requires additional object store requests
    // (to evaluate predicates)
    assert_snapshot!(
        single_file_parquet_test().await.query("select min(a), max(b) from parquet_table WHERE a > 150").await,
        @r"
    ------- Query Output (1 rows) -------
    +----------------------+----------------------+
    | min(parquet_table.a) | max(parquet_table.b) |
    +----------------------+----------------------+
    | 151                  | 1199                 |
    +----------------------+----------------------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 2
    - HEAD path=parquet_table.parquet
    - GET  (ranges) path=parquet_table.parquet ranges=1064-1481,1481-1594,1594-2011,2011-2124
    "
    );
}

#[tokio::test]
async fn query_single_parquet_file_multi_row_groups_multiple_predicates() {
    // Note that evaluating predicates requires additional object store requests
    // (to evaluate predicates)
    assert_snapshot!(
        single_file_parquet_test().await.query("select min(a), max(b) from parquet_table WHERE a > 50 AND b < 1150").await,
        @r"
    ------- Query Output (1 rows) -------
    +----------------------+----------------------+
    | min(parquet_table.a) | max(parquet_table.b) |
    +----------------------+----------------------+
    | 51                   | 1149                 |
    +----------------------+----------------------+
    ------- Object Store Request Summary -------
    RequestCountingObjectStore()
    Total Requests: 3
    - HEAD path=parquet_table.parquet
    - GET  (ranges) path=parquet_table.parquet ranges=4-421,421-534,534-951,951-1064
    - GET  (ranges) path=parquet_table.parquet ranges=1064-1481,1481-1594,1594-2011,2011-2124
    "
    );
}

/// Create a test with a single CSV file with three columns and two rows
async fn single_file_csv_test() -> Test {
    // upload CSV data to object store
    let csv_data = r#"c1,c2,c3
0.00001,5e-12,true
0.00002,4e-12,false
"#;

    Test::new()
        .with_bytes("/csv_table.csv", csv_data)
        .await
        .register_csv("csv_table", "/csv_table.csv")
        .await
}

/// Create a test with three CSV files in a directory
async fn multi_file_csv_test() -> Test {
    let mut test = Test::new();
    // upload CSV data to object store
    for i in 0..3 {
        let csv_data1 = format!(
            r#"c1,c2,c3
0.0000{i},{i}e-12,true
0.00003,5e-12,false
"#
        );
        test = test
            .with_bytes(&format!("/data/file_{i}.csv"), csv_data1)
            .await;
    }
    // register table
    test.register_csv("csv_table", "/data/").await
}

/// Create a test with a single parquet file that has two
/// columns and two row groups
///
/// Column "a": Int32 with values 0-100] in row group 1
/// and [101-200] in row group 2
///
/// Column "b": Int32 with values 1000-1100] in row group 1
/// and [1101-1200] in row group 2
async fn single_file_parquet_test() -> Test {
    // Create parquet bytes
    let a: ArrayRef = Arc::new(Int32Array::from_iter_values(0..200));
    let b: ArrayRef = Arc::new(Int32Array::from_iter_values(1000..1200));
    let batch = RecordBatch::try_from_iter([("a", a), ("b", b)]).unwrap();

    let mut buffer = vec![];
    let props = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_size(100)
        .build();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    Test::new()
        .with_bytes("/parquet_table.parquet", buffer)
        .await
        .register_parquet("parquet_table", "/parquet_table.parquet")
        .await
}

/// Runs tests with a request counting object store
struct Test {
    object_store: Arc<RequestCountingObjectStore>,
    session_context: SessionContext,
}

impl Test {
    fn new() -> Self {
        let object_store = Arc::new(RequestCountingObjectStore::new());
        let session_context = SessionContext::new();
        session_context
            .runtime_env()
            .register_object_store(&Url::parse("mem://").unwrap(), object_store.clone());
        Self {
            object_store,
            session_context,
        }
    }

    /// Returns a string representation of all recorded requests thus far
    fn requests(&self) -> String {
        format!("{}", self.object_store)
    }

    /// Store the specified bytes at the given path
    async fn with_bytes(self, path: &str, bytes: impl Into<Bytes>) -> Self {
        let path = Path::from(path);
        self.object_store
            .inner
            .put(&path, PutPayload::from(bytes.into()))
            .await
            .unwrap();
        self
    }

    /// Register a CSV file at the given path relative to the [`datafusion_test_data`] directory
    async fn register_csv(self, table_name: &str, path: &str) -> Self {
        let mut options = CsvReadOptions::new();
        options.has_header = true;
        let url = format!("mem://{path}");
        self.session_context
            .register_csv(table_name, url, options)
            .await
            .unwrap();
        self
    }

    /// Register a CSV file at the given path relative to the [`datafusion_test_data`] directory
    async fn register_parquet(self, table_name: &str, path: &str) -> Self {
        let path = format!("mem://{path}");
        self.session_context
            .register_parquet(table_name, path, Default::default())
            .await
            .unwrap();
        self
    }

    /// Runs the specified query and returns a string representation of the results
    /// suitable for comparison with insta snapshots
    ///
    /// Clears all recorded requests before running the query
    async fn query(&self, sql: &str) -> String {
        self.object_store.clear_requests();
        let results = self
            .session_context
            .sql(sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let num_rows = results.iter().map(|batch| batch.num_rows()).sum::<usize>();
        let formatted_result =
            arrow::util::pretty::pretty_format_batches(&results).unwrap();

        let object_store = &self.object_store;

        format!(
            r#"------- Query Output ({num_rows} rows) -------
{formatted_result}
------- Object Store Request Summary -------
{object_store}
"#
        )
    }
}

/// Details of individual requests made through the [`RequestCountingObjectStore`]
#[derive(Clone, Debug)]
enum RequestDetails {
    Get { path: Path },
    GetOpts { path: Path, get_options: GetOptions },
    GetRanges { path: Path, ranges: Vec<Range<u64>> },
    GetRange { path: Path, range: Range<u64> },
    Head { path: Path },
    List { prefix: Option<Path> },
    ListWithDelimiter { prefix: Option<Path> },
    ListWithOffset { prefix: Option<Path>, offset: Path },
}

fn display_range(range: &Range<u64>) -> impl Display + '_ {
    struct Wrapper<'a>(&'a Range<u64>);
    impl Display for Wrapper<'_> {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "{}-{}", self.0.start, self.0.end)
        }
    }
    Wrapper(range)
}
impl Display for RequestDetails {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            RequestDetails::Get { path } => {
                write!(f, "GET  path={path}")
            }
            RequestDetails::GetOpts { path, get_options } => {
                write!(f, "GET  (opts) path={path}")?;
                if let Some(range) = &get_options.range {
                    match range {
                        GetRange::Bounded(range) => {
                            let range = display_range(range);
                            write!(f, " range={range}")?;
                        }
                        GetRange::Offset(offset) => {
                            write!(f, " range=offset:{offset}")?;
                        }
                        GetRange::Suffix(suffix) => {
                            write!(f, " range=suffix:{suffix}")?;
                        }
                    }
                }
                if let Some(version) = &get_options.version {
                    write!(f, " version={version}")?;
                }
                if get_options.head {
                    write!(f, " head=true")?;
                }
                Ok(())
            }
            RequestDetails::GetRanges { path, ranges } => {
                write!(f, "GET  (ranges) path={path}")?;
                if !ranges.is_empty() {
                    write!(f, " ranges=")?;
                    for (i, range) in ranges.iter().enumerate() {
                        if i > 0 {
                            write!(f, ",")?;
                        }
                        write!(f, "{}", display_range(range))?;
                    }
                }
                Ok(())
            }
            RequestDetails::GetRange { path, range } => {
                let range = display_range(range);
                write!(f, "GET  (range) range={range} path={path}")
            }
            RequestDetails::Head { path } => {
                write!(f, "HEAD path={path}")
            }
            RequestDetails::List { prefix } => {
                write!(f, "LIST")?;
                if let Some(prefix) = prefix {
                    write!(f, " prefix={prefix}")?;
                }
                Ok(())
            }
            RequestDetails::ListWithDelimiter { prefix } => {
                write!(f, "LIST (with delimiter)")?;
                if let Some(prefix) = prefix {
                    write!(f, " prefix={prefix}")?;
                }
                Ok(())
            }
            RequestDetails::ListWithOffset { prefix, offset } => {
                write!(f, "LIST (with offset) offset={offset}")?;
                if let Some(prefix) = prefix {
                    write!(f, " prefix={prefix}")?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug)]
struct RequestCountingObjectStore {
    /// Inner (memory) store
    inner: Arc<dyn ObjectStore>,
    requests: Mutex<Vec<RequestDetails>>,
}

impl Display for RequestCountingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RequestCountingObjectStore()")?;
        let requests = self.recorded_requests();
        write!(f, "\nTotal Requests: {}", requests.len())?;
        for request in requests {
            write!(f, "\n- {request}")?;
        }
        Ok(())
    }
}

impl RequestCountingObjectStore {
    pub fn new() -> Self {
        let inner = Arc::new(InMemory::new());
        Self {
            inner,
            requests: Mutex::new(vec![]),
        }
    }

    pub fn clear_requests(&self) {
        self.requests.lock().clear();
    }

    /// Return a copy of the recorded requests normalized
    /// by removing the path prefix
    pub fn recorded_requests(&self) -> Vec<RequestDetails> {
        self.requests.lock().to_vec()
    }
}

#[async_trait]
impl ObjectStore for RequestCountingObjectStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(object_store::Error::NotImplemented)
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotImplemented)
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        let result = self.inner.get(location).await?;
        self.requests.lock().push(RequestDetails::Get {
            path: location.to_owned(),
        });
        Ok(result)
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let result = self.inner.get_opts(location, options.clone()).await?;
        self.requests.lock().push(RequestDetails::GetOpts {
            path: location.to_owned(),
            get_options: options,
        });
        Ok(result)
    }

    async fn get_range(
        &self,
        location: &Path,
        range: Range<u64>,
    ) -> object_store::Result<Bytes> {
        let result = self.inner.get_range(location, range.clone()).await?;
        self.requests.lock().push(RequestDetails::GetRange {
            path: location.to_owned(),
            range: range.clone(),
        });
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let result = self.inner.get_ranges(location, ranges).await?;
        self.requests.lock().push(RequestDetails::GetRanges {
            path: location.to_owned(),
            ranges: ranges.to_vec(),
        });
        Ok(result)
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let result = self.inner.head(location).await?;
        self.requests.lock().push(RequestDetails::Head {
            path: location.to_owned(),
        });
        Ok(result)
    }

    async fn delete(&self, _location: &Path) -> object_store::Result<()> {
        Err(object_store::Error::NotImplemented)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.requests.lock().push(RequestDetails::List {
            prefix: prefix.map(|p| p.to_owned()),
        });

        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.requests.lock().push(RequestDetails::ListWithOffset {
            prefix: prefix.map(|p| p.to_owned()),
            offset: offset.to_owned(),
        });
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        self.requests
            .lock()
            .push(RequestDetails::ListWithDelimiter {
                prefix: prefix.map(|p| p.to_owned()),
            });
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        Err(object_store::Error::NotImplemented)
    }

    async fn copy_if_not_exists(
        &self,
        _from: &Path,
        _to: &Path,
    ) -> object_store::Result<()> {
        Err(object_store::Error::NotImplemented)
    }
}
