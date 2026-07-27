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

use std::io::{IsTerminal, Read};
use std::sync::Arc;

use datafusion::common::exec_datafusion_err;
use datafusion::config::ConfigFileType;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use futures::TryStreamExt;

use object_store::memory::InMemory;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use url::Url;

/// Marker [`SessionConfig`] extension recording that the session reads its SQL
/// commands from stdin (the interactive or piped REPL). stdin cannot then also
/// serve as a data source: reading it for table data would silently consume
/// the remaining SQL statements.
///
/// [`SessionConfig`]: datafusion::execution::context::SessionConfig
#[derive(Debug)]
pub struct StdinCarriesCommands;

/// Filesystem paths that refer to the process's standard input.
///
/// These are intentionally limited to the well known pseudo-files exposed by
/// the operating system so that ordinary files are never accidentally treated
/// as stdin.
const STDIN_LOCATIONS: [&str; 3] = ["/dev/stdin", "/dev/fd/0", "/proc/self/fd/0"];

/// Returns `true` if `path` refers to the process's standard input.
///
/// Re-exported as [`crate::object_storage::is_stdin_location`] so the CLI entry
/// point can detect when it reads its SQL from stdin via `-f /dev/stdin` and
/// avoid also offering stdin as a `LOCATION '/dev/stdin'` data source.
pub fn is_stdin_location(path: &str) -> bool {
    STDIN_LOCATIONS.contains(&path)
}

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

    /// Rewrites the well known stdin pseudo-paths (e.g. `/dev/stdin`) to a
    /// canonical `stdin://` URL so that reading from standard input flows
    /// through the same object-store/listing code path as any other scheme.
    /// Non-stdin locations are returned unchanged.
    ///
    /// The listing layer filters candidate files by extension, so the canonical
    /// object is named with the extension matching the declared `STORED AS`
    /// format. The name thereby also records which format stdin was consumed
    /// as: a later stdin-backed table declaring a different format resolves to
    /// a path the buffered store does not contain and is rejected by
    /// [`Self::get_or_create`].
    pub(crate) fn rewrite_location(
        location: &str,
        format: Option<&ConfigFileType>,
    ) -> String {
        if !is_stdin_location(location) {
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

    /// Returns the object store backing the `stdin://` scheme, buffering all of
    /// standard input when the store is first constructed and reusing that
    /// buffer for any subsequent `stdin://` table created in the same session.
    ///
    /// stdin is a one-shot stream: it can only be read once. The object store
    /// registry keys by scheme/authority, so every `stdin://` URL maps to the
    /// same store. Without this guard, a second `CREATE EXTERNAL TABLE ...
    /// LOCATION '/dev/stdin'` would re-read (now-EOF) stdin, build an empty
    /// store, and overwrite the populated one, silently emptying the earlier
    /// table. Reusing the already-registered store avoids that.
    ///
    /// A later stdin-backed table declaring a different `STORED AS` format
    /// resolves to an object the store does not contain (the object name
    /// records the format stdin was consumed as) and is rejected with a clear
    /// error — both reading the buffer as another format and re-reading stdin
    /// would be silently wrong.
    pub(crate) async fn get_or_create(
        state: &SessionState,
        url: &Url,
    ) -> Result<Arc<dyn ObjectStore>> {
        let Ok(existing) = state.runtime_env().object_store_registry.get_store(url)
        else {
            return Self::object_store(state, url).await;
        };

        let path = ObjectStorePath::from_url_path(url.path())?;
        if existing.head(&path).await.is_err() {
            let buffered = existing
                .list(None)
                .try_next()
                .await
                .ok()
                .flatten()
                .map(|meta| format!(" as '{}'", meta.location))
                .unwrap_or_default();
            return Err(exec_datafusion_err!(
                "stdin was already read{buffered} by an earlier statement; all \
                 tables backed by stdin in a session must declare the same \
                 STORED AS format"
            ));
        }
        Ok(existing)
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
    async fn object_store(
        state: &SessionState,
        url: &Url,
    ) -> Result<Arc<dyn ObjectStore>> {
        if state
            .config()
            .get_extension::<StdinCarriesCommands>()
            .is_some()
        {
            return Err(exec_datafusion_err!(
                "stdin is already being read for SQL commands, so it cannot \
                 also supply table data; pass the query with -c/--command or \
                 -f/--file so that stdin carries the data, e.g. \
                 `cat data.csv | datafusion-cli -f query.sql`"
            ));
        }
        if std::io::stdin().is_terminal() {
            return Err(exec_datafusion_err!(
                "stdin is connected to a terminal, not piped data; pipe the \
                 input in, e.g. `cat data.csv | datafusion-cli -f query.sql`"
            ));
        }

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

    use datafusion::prelude::{SessionConfig, SessionContext};

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
    async fn reuses_buffered_stdin_store() -> Result<()> {
        // stdin can only be read once, so a second `stdin://` table must reuse
        // the store buffered by the first instead of re-reading (now-empty)
        // stdin and overwriting it.
        //
        // The very first read happens inside `get_or_create` -> `object_store`,
        // which consumes the real process stdin and so cannot be driven from a
        // unit test. Seed the registry with the store that first read would have
        // produced (as the first `CREATE EXTERNAL TABLE` does), then drive the
        // lookup through `get_or_create` and assert it hands back that exact
        // store rather than rebuilding it.
        let url = Url::parse("stdin:///stdin.csv").unwrap();
        let path = ObjectStorePath::from_url_path(url.path())?;
        let buffered: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        buffered.put(&path, b"a\n1\n2\n".to_vec().into()).await?;

        let ctx = SessionContext::new();
        ctx.register_object_store(&url, Arc::clone(&buffered));

        let reused = StdinUtils::get_or_create(&ctx.state(), &url).await?;
        assert!(
            Arc::ptr_eq(&buffered, &reused),
            "get_or_create must reuse the registered stdin store, not rebuild it"
        );
        let bytes = reused.get(&path).await?.bytes().await?;
        assert_eq!(bytes.as_ref(), b"a\n1\n2\n");
        Ok(())
    }

    #[tokio::test]
    async fn rejects_second_stdin_table_with_different_format() -> Result<()> {
        // The buffered object's name records the format stdin was consumed
        // as; a later stdin table declaring a different format must fail with
        // a clear error rather than a downstream "not found" (or silently
        // misreading the bytes as another format).
        let csv_url = Url::parse("stdin:///stdin.csv").unwrap();
        let store =
            StdinUtils::in_memory_object_store(&csv_url, b"a\n1\n".to_vec()).await?;

        let ctx = SessionContext::new();
        ctx.register_object_store(&csv_url, store);

        let json_url = Url::parse("stdin:///stdin.json").unwrap();
        let err = StdinUtils::get_or_create(&ctx.state(), &json_url)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("must declare the same STORED AS format")
                && err.contains("stdin.csv"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn errors_when_stdin_carries_commands() {
        // Once the REPL owns stdin for SQL commands, building the stdin store
        // must fail with a clear error instead of swallowing the remaining
        // statements as table data.
        let config = SessionConfig::new().with_extension(Arc::new(StdinCarriesCommands));
        let ctx = SessionContext::new_with_config(config);

        let url = Url::parse("stdin:///stdin.csv").unwrap();
        let err = StdinUtils::get_or_create(&ctx.state(), &url)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("SQL commands"),
            "unexpected error: {err}"
        );
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
