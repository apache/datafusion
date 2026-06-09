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

//! Exposes the process's standard input as a `stdin://` object store so that
//! piped data (e.g. `cat data.csv | datafusion-cli`) can be queried via
//! `CREATE EXTERNAL TABLE ... LOCATION '/dev/stdin'`.

use std::io::Read;
use std::sync::Arc;

use datafusion::common::exec_datafusion_err;
use datafusion::config::ConfigFileType;
use datafusion::error::Result;

use object_store::memory::InMemory;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use url::Url;

/// Utilities for exposing the process's standard input as an object store.
///
/// stdin is surfaced as a `stdin://` object store and dispatched alongside the
/// other schemes (`s3`, `gs`, `http`, ...) so that reading piped data flows
/// through the normal object-store/listing code path, conceptually similar to
/// DuckDB's `PipeFileSystem`.
pub(crate) struct StdinUtils;

impl StdinUtils {
    /// The URL scheme used to expose stdin as an object store, mirroring how
    /// `s3`, `gs`, `http`, etc. are addressed.
    pub(crate) const SCHEME: &'static str = "stdin";

    /// Filesystem paths that refer to the process's standard input.
    ///
    /// These are intentionally limited to the well known pseudo-files exposed
    /// by the operating system so that ordinary files are never accidentally
    /// treated as stdin.
    const LOCATIONS: [&'static str; 3] = ["/dev/stdin", "/dev/fd/0", "/proc/self/fd/0"];

    /// Returns `true` if `location` refers to the process's standard input.
    fn is_stdin_location(location: &str) -> bool {
        Self::LOCATIONS.contains(&location)
    }

    /// Rewrites the well known stdin pseudo-paths (e.g. `/dev/stdin`) to a
    /// canonical `stdin://` URL so that reading from standard input flows
    /// through the same object-store/listing code path as any other scheme.
    /// Non-stdin locations are returned unchanged.
    ///
    /// The listing layer filters candidate files by extension, so the canonical
    /// object is named with the extension matching the declared `STORED AS`
    /// format.
    pub(crate) fn rewrite_location(
        location: &str,
        format: Option<&ConfigFileType>,
    ) -> String {
        if !Self::is_stdin_location(location) {
            return location.to_string();
        }

        let object_name = match format {
            Some(ConfigFileType::CSV) => "stdin.csv",
            Some(ConfigFileType::JSON) => "stdin.json",
            Some(ConfigFileType::PARQUET) => "stdin.parquet",
            _ => "stdin",
        };
        format!("{}:///{object_name}", Self::SCHEME)
    }

    /// Builds the object store backing the `stdin://` scheme by reading all of
    /// standard input into memory.
    ///
    /// A pipe (e.g. `cat data.csv | datafusion-cli`) is not seekable and reports
    /// a size of `0`, so it cannot be read directly by the file based formats
    /// (CSV requires seeking, Parquet needs the footer at the end of the file).
    /// Buffering the whole input up front sidesteps these limitations and lets
    /// the data be read like any other object, including being scanned more than
    /// once.
    pub(crate) async fn object_store(url: &Url) -> Result<Arc<dyn ObjectStore>> {
        let mut buffer = Vec::new();
        std::io::stdin()
            .lock()
            .read_to_end(&mut buffer)
            .map_err(|e| exec_datafusion_err!("Failed to read from stdin: {e}"))?;
        Self::in_memory_object_store(url, buffer).await
    }

    /// Stores `data` at the path referenced by `url` in a fresh [`InMemory`]
    /// store.
    async fn in_memory_object_store(
        url: &Url,
        data: Vec<u8>,
    ) -> Result<Arc<dyn ObjectStore>> {
        let store = InMemory::new();
        store
            .put(&ObjectStorePath::from_url_path(url.path())?, data.into())
            .await?;
        Ok(Arc::new(store))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::prelude::SessionContext;

    #[test]
    fn rewrites_stdin_locations() {
        // stdin pseudo-paths are rewritten to a `stdin://` URL carrying the
        // extension that matches the declared format.
        assert_eq!(
            StdinUtils::rewrite_location("/dev/stdin", Some(&ConfigFileType::CSV)),
            "stdin:///stdin.csv"
        );
        assert_eq!(
            StdinUtils::rewrite_location("/dev/fd/0", Some(&ConfigFileType::JSON)),
            "stdin:///stdin.json"
        );
        assert_eq!(
            StdinUtils::rewrite_location(
                "/proc/self/fd/0",
                Some(&ConfigFileType::PARQUET)
            ),
            "stdin:///stdin.parquet"
        );
        assert_eq!(
            StdinUtils::rewrite_location("/dev/stdin", None),
            "stdin:///stdin"
        );

        // Ordinary locations are left untouched.
        for location in ["/dev/stdout", "data/stdin.csv", "stdin", "s3://b/f.csv"] {
            assert_eq!(
                StdinUtils::rewrite_location(location, Some(&ConfigFileType::CSV)),
                location
            );
        }
    }

    /// Buffers `data` into the `stdin://` object store and reads it back through
    /// a `CREATE EXTERNAL TABLE`, returning the number of rows in the table.
    ///
    /// This exercises the full path used for `/dev/stdin` short of the actual
    /// stdin read, which cannot be driven from a unit test.
    async fn count_stdin_rows(
        data: Vec<u8>,
        stored_as: &str,
        format: Option<ConfigFileType>,
        options: &str,
    ) -> Result<usize> {
        let location = StdinUtils::rewrite_location("/dev/stdin", format.as_ref());
        let url = Url::parse(&location).unwrap();
        let store = StdinUtils::in_memory_object_store(&url, data).await?;

        let ctx = SessionContext::new();
        ctx.register_object_store(&url, store);
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE t STORED AS {stored_as} LOCATION '{location}' {options}"
        ))
        .await?
        .collect()
        .await?;

        ctx.sql("SELECT * FROM t").await?.count().await
    }

    #[tokio::test]
    async fn stdin_object_store_reads_csv() -> Result<()> {
        let data = b"a,b\n1,foo\n2,bar\n".to_vec();
        let rows = count_stdin_rows(
            data,
            "CSV",
            Some(ConfigFileType::CSV),
            "OPTIONS ('format.has_header' 'true')",
        )
        .await?;
        assert_eq!(rows, 2);
        Ok(())
    }

    #[tokio::test]
    async fn stdin_object_store_reads_json() -> Result<()> {
        let data = b"{\"a\": 1, \"b\": \"foo\"}\n{\"a\": 2, \"b\": \"bar\"}\n".to_vec();
        let rows = count_stdin_rows(data, "JSON", Some(ConfigFileType::JSON), "").await?;
        assert_eq!(rows, 2);
        Ok(())
    }

    #[tokio::test]
    async fn stdin_object_store_reads_parquet() -> Result<()> {
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;

        // Parquet requires random access to the footer, which a real pipe cannot
        // provide; the in-memory buffer makes this work.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let mut data = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut data, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let rows =
            count_stdin_rows(data, "PARQUET", Some(ConfigFileType::PARQUET), "").await?;
        assert_eq!(rows, 3);
        Ok(())
    }
}
