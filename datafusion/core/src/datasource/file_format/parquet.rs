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

//! Re-exports the [`datafusion_datasource_parquet::file_format`] module, and contains tests for it.

pub use datafusion_datasource_parquet::file_format::*;

#[cfg(test)]
pub(crate) mod test_util {
    use arrow::array::RecordBatch;
    use datafusion_common::Result;
    use object_store::ObjectMeta;

    use crate::test::object_store::local_unpartitioned_file;

    /// Writes each `batch` to at least one temporary parquet file
    ///
    /// For example, if `batches` contains 2 batches, the function will create
    /// 2 temporary files, each containing the contents of one batch
    ///
    /// If multi_page is set to `true`, the parquet file(s) are written
    /// with 2 rows per data page (used to test page filtering and
    /// boundaries).
    pub async fn store_parquet(
        batches: Vec<RecordBatch>,
        multi_page: bool,
    ) -> Result<(Vec<ObjectMeta>, Vec<tempfile::NamedTempFile>)> {
        /// How many rows per page should be written
        const ROWS_PER_PAGE: usize = 2;
        /// write batches chunk_size rows at a time
        fn write_in_chunks<W: std::io::Write + Send>(
            writer: &mut parquet::arrow::ArrowWriter<W>,
            batch: &RecordBatch,
            chunk_size: usize,
        ) {
            let mut i = 0;
            while i < batch.num_rows() {
                let num = chunk_size.min(batch.num_rows() - i);
                writer.write(&batch.slice(i, num)).unwrap();
                i += num;
            }
        }

        // we need the tmp files to be sorted as some tests rely on the returned file ordering
        // https://github.com/apache/datafusion/pull/6629
        let tmp_files = {
            let mut tmp_files: Vec<_> = (0..batches.len())
                .map(|_| tempfile::NamedTempFile::new().expect("creating temp file"))
                .collect();
            tmp_files.sort_by(|a, b| a.path().cmp(b.path()));
            tmp_files
        };

        // Each batch writes to their own file
        let files: Vec<_> = batches
            .into_iter()
            .zip(tmp_files.into_iter())
            .map(|(batch, mut output)| {
                let mut builder = parquet::file::properties::WriterProperties::builder();
                if multi_page {
                    builder = builder.set_data_page_row_count_limit(ROWS_PER_PAGE)
                }
                builder = builder.set_bloom_filter_enabled(true);

                let props = builder.build();

                let mut writer = parquet::arrow::ArrowWriter::try_new(
                    &mut output,
                    batch.schema(),
                    Some(props),
                )
                .expect("creating writer");

                if multi_page {
                    // write in smaller batches as the parquet writer
                    // only checks datapage size limits on the boundaries of each batch
                    write_in_chunks(&mut writer, &batch, ROWS_PER_PAGE);
                } else {
                    writer.write(&batch).expect("Writing batch");
                };
                writer.close().unwrap();
                output
            })
            .collect();

        let meta: Vec<_> = files.iter().map(local_unpartitioned_file).collect();

        Ok((meta, files))
    }
}

#[cfg(test)]
mod tests {

    use std::fmt::{self, Display, Formatter};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use crate::datasource::file_format::parquet::test_util::store_parquet;
    use crate::datasource::file_format::test_util::scan_format;
    use crate::execution::SessionState;
    use crate::physical_plan::metrics::MetricValue;
    use crate::prelude::{ParquetReadOptions, SessionConfig, SessionContext};

    use arrow::array::RecordBatch;
    use arrow_schema::Schema;
    use datafusion_catalog::Session;
    use datafusion_common::ScalarValue::Utf8;
    use datafusion_common::cast::{
        as_binary_array, as_binary_view_array, as_boolean_array, as_float32_array,
        as_float64_array, as_int32_array, as_timestamp_nanosecond_array,
    };
    use datafusion_common::config::{ParquetOptions, TableParquetOptions};
    use datafusion_common::stats::Precision;
    use datafusion_common::test_util::batches_to_string;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_datasource::file_format::FileFormat;
    use datafusion_datasource::file_sink_config::{FileSink, FileSinkConfig};
    use datafusion_datasource::{ListingTableUrl, PartitionedFile};
    use datafusion_datasource_parquet::{
        ParquetFormat, ParquetFormatFactory, ParquetSink,
    };
    use datafusion_execution::TaskContext;
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_execution::runtime_env::RuntimeEnv;
    use datafusion_expr::dml::InsertOp;
    use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion_physical_plan::{ExecutionPlan, collect};

    use crate::test_util::bounded_stream;
    use arrow::array::{
        Array, ArrayRef, DictionaryArray, Int32Array, Int64Array, StringArray,
        types::Int32Type,
    };
    use arrow::datatypes::{DataType, Field};
    use async_trait::async_trait;
    use datafusion_datasource::file_groups::FileGroup;
    use datafusion_datasource_parquet::metadata::DFParquetMetadata;
    use futures::StreamExt;
    use futures::stream::BoxStream;
    use insta::assert_snapshot;
    use object_store::ObjectMeta;
    use object_store::local::LocalFileSystem;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
    };
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use parquet::arrow::arrow_reader::ArrowReaderOptions;
    use parquet::file::metadata::{
        KeyValue, ParquetColumnIndex, ParquetMetaData, ParquetOffsetIndex,
    };
    use parquet::file::page_index::column_index::ColumnIndexMetaData;
    use tokio::fs::File;

    enum ForceViews {
        Yes,
        No,
    }

    async fn _run_read_merged_batches(force_views: ForceViews) -> Result<()> {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let batch1 = RecordBatch::try_from_iter(vec![("c1", c1.clone())])?;
        let batch2 = RecordBatch::try_from_iter(vec![("c2", c2)])?;

        let store = Arc::new(LocalFileSystem::new()) as _;
        let (meta, _files) = store_parquet(vec![batch1, batch2], false).await?;

        let session = SessionContext::new();
        let ctx = session.state();
        let force_views = match force_views {
            ForceViews::Yes => true,
            ForceViews::No => false,
        };
        let format = ParquetFormat::default().with_force_view_types(force_views);
        let schema = format.infer_schema(&ctx, &store, &meta).await?;

        let file_metadata_cache =
            ctx.runtime_env().cache_manager.get_file_metadata_cache();
        let stats = DFParquetMetadata::new(&store, &meta[0])
            .with_file_metadata_cache(Some(Arc::clone(&file_metadata_cache)))
            .fetch_statistics(&schema)
            .await?;

        assert_eq!(stats.num_rows, Precision::Exact(3));
        let c1_stats = &stats.column_statistics[0];
        let c2_stats = &stats.column_statistics[1];
        assert_eq!(c1_stats.null_count, Precision::Exact(1));
        assert_eq!(c2_stats.null_count, Precision::Exact(3));

        let stats = DFParquetMetadata::new(&store, &meta[1])
            .with_file_metadata_cache(Some(Arc::clone(&file_metadata_cache)))
            .fetch_statistics(&schema)
            .await?;

        assert_eq!(stats.num_rows, Precision::Exact(3));
        let c1_stats = &stats.column_statistics[0];
        let c2_stats = &stats.column_statistics[1];
        assert_eq!(c1_stats.null_count, Precision::Exact(3));
        assert_eq!(c2_stats.null_count, Precision::Exact(1));
        assert_eq!(
            c2_stats.max_value,
            Precision::Exact(ScalarValue::Int64(Some(2)))
        );
        assert_eq!(
            c2_stats.min_value,
            Precision::Exact(ScalarValue::Int64(Some(1)))
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_merged_batches() -> Result<()> {
        _run_read_merged_batches(ForceViews::No).await?;
        _run_read_merged_batches(ForceViews::Yes).await?;

        Ok(())
    }

    #[tokio::test]
    async fn is_schema_stable() -> Result<()> {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let batch1 =
            RecordBatch::try_from_iter(vec![("a", c1.clone()), ("b", c1.clone())])?;
        let batch2 =
            RecordBatch::try_from_iter(vec![("c", c2.clone()), ("d", c2.clone())])?;

        let store = Arc::new(LocalFileSystem::new()) as _;
        let (meta, _files) = store_parquet(vec![batch1, batch2], false).await?;

        let session = SessionContext::new();
        let ctx = session.state();
        let format = ParquetFormat::default();
        let schema = format.infer_schema(&ctx, &store, &meta).await?;

        let order: Vec<_> = ["a", "b", "c", "d"]
            .into_iter()
            .map(|i| i.to_string())
            .collect();
        let coll: Vec<_> = schema
            .flattened_fields()
            .into_iter()
            .map(|i| i.name().to_string())
            .collect();
        assert_eq!(coll, order);

        Ok(())
    }

    #[derive(Debug)]
    struct RequestCountingObjectStore {
        inner: Arc<dyn ObjectStore>,
        request_count: AtomicUsize,
    }

    impl Display for RequestCountingObjectStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "RequestCounting({})", self.inner)
        }
    }

    impl RequestCountingObjectStore {
        pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                request_count: Default::default(),
            }
        }

        pub fn request_count(&self) -> usize {
            self.request_count.load(Ordering::SeqCst)
        }

        pub fn upcast(self: &Arc<Self>) -> Arc<dyn ObjectStore> {
            self.clone()
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

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.request_count.fetch_add(1, Ordering::SeqCst);
            self.inner.get_opts(location, options).await
        }

        async fn head(&self, _location: &Path) -> object_store::Result<ObjectMeta> {
            Err(object_store::Error::NotImplemented)
        }

        async fn delete(&self, _location: &Path) -> object_store::Result<()> {
            Err(object_store::Error::NotImplemented)
        }

        fn list(
            &self,
            _prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            Box::pin(futures::stream::once(async {
                Err(object_store::Error::NotImplemented)
            }))
        }

        async fn list_with_delimiter(
            &self,
            _prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            Err(object_store::Error::NotImplemented)
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

    async fn _run_fetch_metadata_with_size_hint(force_views: ForceViews) -> Result<()> {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let batch1 = RecordBatch::try_from_iter(vec![("c1", c1.clone())])?;
        let batch2 = RecordBatch::try_from_iter(vec![("c2", c2)])?;

        let store = Arc::new(RequestCountingObjectStore::new(Arc::new(
            LocalFileSystem::new(),
        )));
        let (meta, _files) = store_parquet(vec![batch1, batch2], false).await?;

        let session = SessionContext::new();
        let ctx = session.state();

        // Use a size hint larger than the parquet footer but smaller than the actual metadata, requiring a second fetch
        // for the remaining metadata
        let file_metadata_cache =
            ctx.runtime_env().cache_manager.get_file_metadata_cache();
        let df_meta = DFParquetMetadata::new(store.as_ref(), &meta[0])
            .with_metadata_size_hint(Some(9));
        df_meta.fetch_metadata().await?;
        assert_eq!(store.request_count(), 2);

        let df_meta =
            df_meta.with_file_metadata_cache(Some(Arc::clone(&file_metadata_cache)));

        // Increases by 3 because cache has no entries yet
        df_meta.fetch_metadata().await?;
        assert_eq!(store.request_count(), 5);

        // No increase because cache has an entry
        df_meta.fetch_metadata().await?;
        assert_eq!(store.request_count(), 5);

        // Increase by 2  because `get_file_metadata_cache()` is None
        let df_meta = df_meta.with_file_metadata_cache(None);
        df_meta.fetch_metadata().await?;
        assert_eq!(store.request_count(), 7);

        let force_views = match force_views {
            ForceViews::Yes => true,
            ForceViews::No => false,
        };
        let format = ParquetFormat::default()
            .with_metadata_size_hint(Some(9))
            .with_force_view_types(force_views);
        // Increase by 3, partial cache being used.
        let _schema = format.infer_schema(&ctx, &store.upcast(), &meta).await?;
        assert_eq!(store.request_count(), 10);
        // No increase, full cache being used.
        let schema = format.infer_schema(&ctx, &store.upcast(), &meta).await?;
        assert_eq!(store.request_count(), 10);

        // No increase, cache being used
        let df_meta =
            df_meta.with_file_metadata_cache(Some(Arc::clone(&file_metadata_cache)));
        let stats = df_meta.fetch_statistics(&schema).await?;
        assert_eq!(store.request_count(), 10);

        assert_eq!(stats.num_rows, Precision::Exact(3));
        let c1_stats = &stats.column_statistics[0];
        let c2_stats = &stats.column_statistics[1];
        assert_eq!(c1_stats.null_count, Precision::Exact(1));
        assert_eq!(c2_stats.null_count, Precision::Exact(3));

        let store = Arc::new(RequestCountingObjectStore::new(Arc::new(
            LocalFileSystem::new(),
        )));

        // Use the file size as the hint so we can get the full metadata from the first fetch
        let size_hint = meta[0].size as usize;
        let df_meta = DFParquetMetadata::new(store.as_ref(), &meta[0])
            .with_metadata_size_hint(Some(size_hint));

        df_meta.fetch_metadata().await?;
        // ensure the requests were coalesced into a single request
        assert_eq!(store.request_count(), 1);

        let session = SessionContext::new();
        let ctx = session.state();
        let file_metadata_cache =
            ctx.runtime_env().cache_manager.get_file_metadata_cache();
        let df_meta =
            df_meta.with_file_metadata_cache(Some(Arc::clone(&file_metadata_cache)));
        // Increases by 1 because cache has no entries yet and new session context
        df_meta.fetch_metadata().await?;
        assert_eq!(store.request_count(), 2);

        // No increase because cache has an entry
        df_meta.fetch_metadata().await?;
        assert_eq!(store.request_count(), 2);

        // Increase by 1  because `get_file_metadata_cache` is None
        let df_meta = df_meta.with_file_metadata_cache(None);
        df_meta.fetch_metadata().await?;
        assert_eq!(store.request_count(), 3);

        let format = ParquetFormat::default()
            .with_metadata_size_hint(Some(size_hint))
            .with_force_view_types(force_views);
        // Increase by 1, partial cache being used.
        let _schema = format.infer_schema(&ctx, &store.upcast(), &meta).await?;
        assert_eq!(store.request_count(), 4);
        // No increase, full cache being used.
        let schema = format.infer_schema(&ctx, &store.upcast(), &meta).await?;
        assert_eq!(store.request_count(), 4);
        // No increase, cache being used
        let df_meta =
            df_meta.with_file_metadata_cache(Some(Arc::clone(&file_metadata_cache)));
        let stats = df_meta.fetch_statistics(&schema).await?;
        assert_eq!(store.request_count(), 4);

        assert_eq!(stats.num_rows, Precision::Exact(3));
        let c1_stats = &stats.column_statistics[0];
        let c2_stats = &stats.column_statistics[1];
        assert_eq!(c1_stats.null_count, Precision::Exact(1));
        assert_eq!(c2_stats.null_count, Precision::Exact(3));

        let store = Arc::new(RequestCountingObjectStore::new(Arc::new(
            LocalFileSystem::new(),
        )));

        // Use a size hint larger than the file size to make sure we don't panic
        let size_hint = (meta[0].size + 100) as usize;
        let df_meta = DFParquetMetadata::new(store.as_ref(), &meta[0])
            .with_metadata_size_hint(Some(size_hint));

        df_meta.fetch_metadata().await?;
        assert_eq!(store.request_count(), 1);

        // No increase because cache has an entry
        let df_meta =
            df_meta.with_file_metadata_cache(Some(Arc::clone(&file_metadata_cache)));
        df_meta.fetch_metadata().await?;
        assert_eq!(store.request_count(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_metadata_with_size_hint() -> Result<()> {
        _run_fetch_metadata_with_size_hint(ForceViews::No).await?;
        _run_fetch_metadata_with_size_hint(ForceViews::Yes).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_from_parquet_metadata_dictionary() -> Result<()> {
        // Data for column c_dic: ["a", "b", "c", "d"]
        let values = StringArray::from_iter_values(["a", "b", "c", "d"]);
        let keys = Int32Array::from_iter_values([0, 1, 2, 3]);
        let dic_array = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values))?;
        let c_dic: ArrayRef = Arc::new(dic_array);

        // Data for column string_truncation: ["a".repeat(128), null, "b".repeat(128), null]
        let string_truncation: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a".repeat(128)),
            None,
            Some("b".repeat(128)),
            None,
        ]));

        let batch1 = RecordBatch::try_from_iter(vec![
            ("c_dic", c_dic),
            ("string_truncation", string_truncation),
        ])?;

        // Use store_parquet to write each batch to its own file
        // . batch1 written into first file and includes:
        //    - column c_dic that has 4 rows with no null. Stats min and max of dictionary column is available.
        //    - column string_truncation that has 4 rows with 2 nulls. Stats min and max of string column is available but not exact.
        let store = Arc::new(RequestCountingObjectStore::new(Arc::new(
            LocalFileSystem::new(),
        )));
        let (files, _file_names) = store_parquet(vec![batch1], false).await?;

        let state = SessionContext::new().state();
        // Make metadata size hint None to keep original behavior
        let format = ParquetFormat::default().with_metadata_size_hint(None);
        let _schema = format.infer_schema(&state, &store.upcast(), &files).await?;
        assert_eq!(store.request_count(), 3);
        // No increase, cache being used.
        let schema = format.infer_schema(&state, &store.upcast(), &files).await?;
        assert_eq!(store.request_count(), 3);

        // No increase in request count because cache is not empty
        let file_metadata_cache =
            state.runtime_env().cache_manager.get_file_metadata_cache();
        let stats = DFParquetMetadata::new(store.as_ref(), &files[0])
            .with_file_metadata_cache(Some(Arc::clone(&file_metadata_cache)))
            .fetch_statistics(&schema)
            .await?;
        assert_eq!(stats.num_rows, Precision::Exact(4));

        // column c_dic
        let c_dic_stats = &stats.column_statistics[0];

        assert_eq!(c_dic_stats.null_count, Precision::Exact(0));
        assert_eq!(
            c_dic_stats.max_value,
            Precision::Exact(Utf8(Some("d".into())))
        );
        assert_eq!(
            c_dic_stats.min_value,
            Precision::Exact(Utf8(Some("a".into())))
        );

        // column string_truncation
        let string_truncation_stats = &stats.column_statistics[1];

        assert_eq!(string_truncation_stats.null_count, Precision::Exact(2));
        assert_eq!(
            string_truncation_stats.max_value,
            Precision::Inexact(ScalarValue::Utf8View(Some("b".repeat(63) + "c")))
        );
        assert_eq!(
            string_truncation_stats.min_value,
            Precision::Inexact(ScalarValue::Utf8View(Some("a".repeat(64))))
        );

        Ok(())
    }

    async fn _run_test_statistics_from_parquet_metadata(
        force_views: ForceViews,
    ) -> Result<()> {
        // Data for column c1: ["Foo", null, "bar"]
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));
        let batch1 = RecordBatch::try_from_iter(vec![("c1", c1.clone())])?;

        // Data for column c2: [1, 2, null]
        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));
        let batch2 = RecordBatch::try_from_iter(vec![("c2", c2)])?;

        // Use store_parquet to write each batch to its own file
        // . batch1 written into first file and includes:
        //    - column c1 that has 3 rows with one null. Stats min and max of string column is missing for this test even the column has values
        // . batch2 written into second file and includes:
        //    - column c2 that has 3 rows with one null. Stats min and max of int are available and 1 and 2 respectively
        let store = Arc::new(RequestCountingObjectStore::new(Arc::new(
            LocalFileSystem::new(),
        )));
        let (files, _file_names) = store_parquet(vec![batch1, batch2], false).await?;

        let force_views = match force_views {
            ForceViews::Yes => true,
            ForceViews::No => false,
        };

        let mut state = SessionContext::new().state();
        state = set_view_state(state, force_views);
        let format = ParquetFormat::default()
            .with_force_view_types(force_views)
            .with_metadata_size_hint(None);
        let schema = format.infer_schema(&state, &store.upcast(), &files).await?;
        assert_eq!(store.request_count(), 6);

        let null_i64 = ScalarValue::Int64(None);
        let null_utf8 = if force_views {
            ScalarValue::Utf8View(None)
        } else {
            Utf8(None)
        };

        // No increase in request count because cache is not empty
        let file_metadata_cache =
            state.runtime_env().cache_manager.get_file_metadata_cache();
        let stats = DFParquetMetadata::new(store.as_ref(), &files[0])
            .with_file_metadata_cache(Some(Arc::clone(&file_metadata_cache)))
            .fetch_statistics(&schema)
            .await?;
        assert_eq!(store.request_count(), 6);
        assert_eq!(stats.num_rows, Precision::Exact(3));
        // column c1
        let c1_stats = &stats.column_statistics[0];
        assert_eq!(c1_stats.null_count, Precision::Exact(1));
        let expected_type = if force_views {
            ScalarValue::Utf8View
        } else {
            Utf8
        };
        assert_eq!(
            c1_stats.max_value,
            Precision::Exact(expected_type(Some("bar".to_string())))
        );
        assert_eq!(
            c1_stats.min_value,
            Precision::Exact(expected_type(Some("Foo".to_string())))
        );
        // column c2: missing from the file so the table treats all 3 rows as null
        let c2_stats = &stats.column_statistics[1];
        assert_eq!(c2_stats.null_count, Precision::Exact(3));
        assert_eq!(c2_stats.max_value, Precision::Exact(null_i64.clone()));
        assert_eq!(c2_stats.min_value, Precision::Exact(null_i64.clone()));

        // No increase in request count because cache is not empty
        let stats = DFParquetMetadata::new(store.as_ref(), &files[1])
            .with_file_metadata_cache(Some(Arc::clone(&file_metadata_cache)))
            .fetch_statistics(&schema)
            .await?;
        assert_eq!(store.request_count(), 6);
        assert_eq!(stats.num_rows, Precision::Exact(3));
        // column c1: missing from the file so the table treats all 3 rows as null
        let c1_stats = &stats.column_statistics[0];
        assert_eq!(c1_stats.null_count, Precision::Exact(3));
        assert_eq!(c1_stats.max_value, Precision::Exact(null_utf8.clone()));
        assert_eq!(c1_stats.min_value, Precision::Exact(null_utf8.clone()));
        // column c2
        let c2_stats = &stats.column_statistics[1];
        assert_eq!(c2_stats.null_count, Precision::Exact(1));
        assert_eq!(c2_stats.max_value, Precision::Exact(2i64.into()));
        assert_eq!(c2_stats.min_value, Precision::Exact(1i64.into()));

        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_from_parquet_metadata() -> Result<()> {
        _run_test_statistics_from_parquet_metadata(ForceViews::No).await?;

        _run_test_statistics_from_parquet_metadata(ForceViews::Yes).await?;

        Ok(())
    }

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let config = SessionConfig::new().with_batch_size(2);
        let session_ctx = SessionContext::new_with_config(config);
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;
        let stream = exec.execute(0, task_ctx)?;

        let tt_batches = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(11, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        assert_eq!(tt_batches, 4 /* 8/2 */);

        // test metadata
        assert_eq!(
            exec.partition_statistics(None)?.num_rows,
            Precision::Exact(8)
        );
        // TODO correct byte size: https://github.com/apache/datafusion/issues/14936
        assert_eq!(
            exec.partition_statistics(None)?.total_byte_size,
            Precision::Absent,
        );

        Ok(())
    }

    #[tokio::test]
    async fn capture_bytes_scanned_metric() -> Result<()> {
        let config = SessionConfig::new().with_batch_size(2);
        let session = SessionContext::new_with_config(config);
        let ctx = session.state();

        // Read the full file
        let projection = None;
        let exec = get_exec(&ctx, "alltypes_plain.parquet", projection, None).await?;

        // Read only one column. This should scan less data.
        let projection = Some(vec![0]);
        let exec_projected =
            get_exec(&ctx, "alltypes_plain.parquet", projection, None).await?;

        let task_ctx = ctx.task_ctx();

        let _ = collect(exec.clone(), task_ctx.clone()).await?;
        let _ = collect(exec_projected.clone(), task_ctx).await?;

        assert_bytes_scanned(exec, 671);
        assert_bytes_scanned(exec_projected, 73);

        Ok(())
    }

    #[tokio::test]
    async fn read_limit() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec =
            get_exec(&state, "alltypes_plain.parquet", projection, Some(1)).await?;

        // note: even if the limit is set, the executor rounds up to the batch size
        assert_eq!(
            exec.partition_statistics(None)?.num_rows,
            Precision::Exact(8)
        );
        assert_eq!(
            exec.partition_statistics(None)?.total_byte_size,
            Precision::Absent,
        );
        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(11, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        Ok(())
    }

    fn set_view_state(mut state: SessionState, use_views: bool) -> SessionState {
        let mut options = TableParquetOptions::default();
        options.global.schema_force_view_types = use_views;
        state
            .register_file_format(
                Arc::new(ParquetFormatFactory::new_with_options(options)),
                true,
            )
            .expect("ok");
        state
    }

    async fn _run_read_alltypes_plain_parquet(
        force_views: ForceViews,
        expected: &str,
    ) -> Result<()> {
        let force_views = match force_views {
            ForceViews::Yes => true,
            ForceViews::No => false,
        };

        let session_ctx = SessionContext::new();
        let mut state = session_ctx.state();
        state = set_view_state(state, force_views);

        let task_ctx = state.task_ctx();
        let projection = None;
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let x: Vec<String> = exec
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {}", f.name(), f.data_type()))
            .collect();
        let y = x.join("\n");
        assert_eq!(expected, y);

        let batches = collect(exec, task_ctx).await?;

        assert_eq!(1, batches.len());
        assert_eq!(11, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn read_alltypes_plain_parquet() -> Result<()> {
        let no_views = "id: Int32\n\
             bool_col: Boolean\n\
             tinyint_col: Int32\n\
             smallint_col: Int32\n\
             int_col: Int32\n\
             bigint_col: Int64\n\
             float_col: Float32\n\
             double_col: Float64\n\
             date_string_col: Binary\n\
             string_col: Binary\n\
             timestamp_col: Timestamp(ns)";
        _run_read_alltypes_plain_parquet(ForceViews::No, no_views).await?;

        let with_views = "id: Int32\n\
             bool_col: Boolean\n\
             tinyint_col: Int32\n\
             smallint_col: Int32\n\
             int_col: Int32\n\
             bigint_col: Int64\n\
             float_col: Float32\n\
             double_col: Float64\n\
             date_string_col: BinaryView\n\
             string_col: BinaryView\n\
             timestamp_col: Timestamp(ns)";
        _run_read_alltypes_plain_parquet(ForceViews::Yes, with_views).await?;

        Ok(())
    }

    #[tokio::test]
    async fn read_bool_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![1]);
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_boolean_array(batches[0].column(0))?;
        let mut values: Vec<bool> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[true, false, true, false, true, false, true, false]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_i32_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![0]);
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_int32_array(batches[0].column(0))?;
        let mut values: Vec<i32> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[4, 5, 6, 7, 2, 3, 0, 1]", format!("{values:?}"));

        Ok(())
    }

    #[tokio::test]
    async fn read_i96_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![10]);
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_timestamp_nanosecond_array(batches[0].column(0))?;
        let mut values: Vec<i64> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[1235865600000000000, 1235865660000000000, 1238544000000000000, 1238544060000000000, 1233446400000000000, 1233446460000000000, 1230768000000000000, 1230768060000000000]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_f32_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![6]);
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_float32_array(batches[0].column(0))?;
        let mut values: Vec<f32> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 1.1, 0.0, 1.1, 0.0, 1.1, 0.0, 1.1]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_f64_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![7]);
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_float64_array(batches[0].column(0))?;
        let mut values: Vec<f64> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 10.1, 0.0, 10.1, 0.0, 10.1, 0.0, 10.1]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_binary_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let mut state = session_ctx.state();
        state = set_view_state(state, false);

        let task_ctx = state.task_ctx();
        let projection = Some(vec![9]);
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_binary_array(batches[0].column(0))?;
        let mut values: Vec<&str> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(std::str::from_utf8(array.value(i)).unwrap());
        }

        assert_eq!(
            "[\"0\", \"1\", \"0\", \"1\", \"0\", \"1\", \"0\", \"1\"]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_binaryview_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let mut state = session_ctx.state();
        state = set_view_state(state, true);

        let task_ctx = state.task_ctx();
        let projection = Some(vec![9]);
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_binary_view_array(batches[0].column(0))?;
        let mut values: Vec<&str> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(std::str::from_utf8(array.value(i)).unwrap());
        }

        assert_eq!(
            "[\"0\", \"1\", \"0\", \"1\", \"0\", \"1\", \"0\", \"1\"]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_decimal_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();

        // parquet use the int32 as the physical type to store decimal
        let exec = get_exec(&state, "int32_decimal.parquet", None, None).await?;
        let batches = collect(exec, task_ctx.clone()).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        let column = batches[0].column(0);
        assert_eq!(&DataType::Decimal128(4, 2), column.data_type());

        // parquet use the int64 as the physical type to store decimal
        let exec = get_exec(&state, "int64_decimal.parquet", None, None).await?;
        let batches = collect(exec, task_ctx.clone()).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        let column = batches[0].column(0);
        assert_eq!(&DataType::Decimal128(10, 2), column.data_type());

        // parquet use the fixed length binary as the physical type to store decimal
        let exec = get_exec(&state, "fixed_length_decimal.parquet", None, None).await?;
        let batches = collect(exec, task_ctx.clone()).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        let column = batches[0].column(0);
        assert_eq!(&DataType::Decimal128(25, 2), column.data_type());

        let exec =
            get_exec(&state, "fixed_length_decimal_legacy.parquet", None, None).await?;
        let batches = collect(exec, task_ctx.clone()).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        let column = batches[0].column(0);
        assert_eq!(&DataType::Decimal128(13, 2), column.data_type());

        // parquet use the byte array as the physical type to store decimal
        let exec = get_exec(&state, "byte_array_decimal.parquet", None, None).await?;
        let batches = collect(exec, task_ctx.clone()).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        let column = batches[0].column(0);
        assert_eq!(&DataType::Decimal128(4, 2), column.data_type());

        Ok(())
    }
    #[tokio::test]
    async fn test_read_parquet_page_index() -> Result<()> {
        let testdata = datafusion_common::test_util::parquet_test_data();
        let path = format!("{testdata}/alltypes_tiny_pages.parquet");
        let file = File::open(path).await?;
        let options = ArrowReaderOptions::new().with_page_index(true);
        let builder =
            ParquetRecordBatchStreamBuilder::new_with_options(file, options.clone())
                .await?
                .metadata()
                .clone();
        check_page_index_validation(builder.column_index(), builder.offset_index());

        let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
        let file = File::open(path).await?;

        let builder = ParquetRecordBatchStreamBuilder::new_with_options(file, options)
            .await?
            .metadata()
            .clone();
        check_page_index_validation(builder.column_index(), builder.offset_index());

        Ok(())
    }

    fn check_page_index_validation(
        page_index: Option<&ParquetColumnIndex>,
        offset_index: Option<&ParquetOffsetIndex>,
    ) {
        assert!(page_index.is_some());
        assert!(offset_index.is_some());

        let page_index = page_index.unwrap();
        let offset_index = offset_index.unwrap();

        // there is only one row group in one file.
        assert_eq!(page_index.len(), 1);
        assert_eq!(offset_index.len(), 1);
        let page_index = page_index.first().unwrap();
        let offset_index = offset_index.first().unwrap();

        // 13 col in one row group
        assert_eq!(page_index.len(), 13);
        assert_eq!(offset_index.len(), 13);

        // test result in int_col
        let int_col_index = page_index.get(4).unwrap();
        let int_col_offset = offset_index.get(4).unwrap().page_locations();

        // 325 pages in int_col
        assert_eq!(int_col_offset.len(), 325);
        let ColumnIndexMetaData::INT32(index) = int_col_index else {
            panic!("fail to read page index.")
        };
        assert_eq!(index.min_values().len(), 325);
        assert_eq!(index.max_values().len(), 325);
        // all values are non null
        for idx in 0..325 {
            assert_eq!(index.null_count(idx), Some(0));
        }
    }

    fn assert_bytes_scanned(exec: Arc<dyn ExecutionPlan>, expected: usize) {
        let actual = exec
            .metrics()
            .expect("Metrics not recorded")
            .sum(|metric| matches!(metric.value(), MetricValue::Count { name, .. } if name == "bytes_scanned"))
            .map(|t| t.as_usize())
            .expect("bytes_scanned metric not recorded");

        assert_eq!(actual, expected);
    }

    async fn get_exec(
        state: &dyn Session,
        file_name: &str,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let testdata = datafusion_common::test_util::parquet_test_data();
        let state = state.as_any().downcast_ref::<SessionState>().unwrap();
        let format = state
            .get_file_format_factory("parquet")
            .map(|factory| factory.create(state, &Default::default()).unwrap())
            .unwrap_or_else(|| Arc::new(ParquetFormat::new()));

        scan_format(
            state, &*format, None, &testdata, file_name, projection, limit,
        )
        .await
    }

    /// Test that 0-byte files don't break while reading
    #[tokio::test]
    async fn test_read_empty_parquet() -> Result<()> {
        let tmp_dir = tempfile::TempDir::new()?;
        let path = format!("{}/empty.parquet", tmp_dir.path().to_string_lossy());
        File::create(&path).await?;

        let ctx = SessionContext::new();

        let df = ctx
            .read_parquet(&path, ParquetReadOptions::default())
            .await
            .expect("read_parquet should succeed");

        let result = df.collect().await?;

        assert_snapshot!(batches_to_string(&result), @r"
        ++
        ++
        ");

        Ok(())
    }

    /// Test that 0-byte files don't break while reading
    #[tokio::test]
    async fn test_read_partitioned_empty_parquet() -> Result<()> {
        let tmp_dir = tempfile::TempDir::new()?;
        let partition_dir = tmp_dir.path().join("col1=a");
        std::fs::create_dir(&partition_dir)?;
        File::create(partition_dir.join("empty.parquet")).await?;

        let ctx = SessionContext::new();

        let df = ctx
            .read_parquet(
                tmp_dir.path().to_str().unwrap(),
                ParquetReadOptions::new()
                    .table_partition_cols(vec![("col1".to_string(), DataType::Utf8)]),
            )
            .await
            .expect("read_parquet should succeed");

        let result = df.collect().await?;

        assert_snapshot!(batches_to_string(&result), @r"
        ++
        ++
        ");

        Ok(())
    }

    fn build_ctx(store_url: &url::Url) -> Arc<TaskContext> {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let local = Arc::new(
            LocalFileSystem::new_with_prefix(&tmp_dir)
                .expect("should create object store"),
        );

        let mut session = SessionConfig::default();
        let mut parquet_opts = ParquetOptions {
            allow_single_file_parallelism: true,
            ..Default::default()
        };
        parquet_opts.allow_single_file_parallelism = true;
        session.options_mut().execution.parquet = parquet_opts;

        let runtime = RuntimeEnv::default();
        runtime
            .object_store_registry
            .register_store(store_url, local);

        Arc::new(
            TaskContext::default()
                .with_session_config(session)
                .with_runtime(Arc::new(runtime)),
        )
    }

    #[tokio::test]
    async fn parquet_sink_write() -> Result<()> {
        let parquet_sink = create_written_parquet_sink("file:///").await?;

        // assert written to proper path
        let (path, file_metadata) = get_written(parquet_sink)?;
        let path_parts = path.parts().collect::<Vec<_>>();
        assert_eq!(path_parts.len(), 1, "should not have path prefix");

        // check the file metadata
        let expected_kv_meta = vec![
            // default is to include arrow schema
            KeyValue {
                key: "ARROW:schema".to_string(),
                value: Some(ENCODED_ARROW_SCHEMA.to_string()),
            },
            KeyValue {
                key: "my-data".to_string(),
                value: Some("stuff".to_string()),
            },
            KeyValue {
                key: "my-data-bool-key".to_string(),
                value: None,
            },
        ];
        assert_file_metadata(file_metadata, &expected_kv_meta);

        Ok(())
    }

    #[tokio::test]
    async fn parquet_sink_parallel_write() -> Result<()> {
        let opts = ParquetOptions {
            allow_single_file_parallelism: true,
            maximum_parallel_row_group_writers: 2,
            maximum_buffered_record_batches_per_stream: 2,
            ..Default::default()
        };

        let parquet_sink =
            create_written_parquet_sink_using_config("file:///", opts).await?;

        // assert written to proper path
        let (path, file_metadata) = get_written(parquet_sink)?;
        let path_parts = path.parts().collect::<Vec<_>>();
        assert_eq!(path_parts.len(), 1, "should not have path prefix");

        // check the file metadata
        let expected_kv_meta = vec![
            // default is to include arrow schema
            KeyValue {
                key: "ARROW:schema".to_string(),
                value: Some(ENCODED_ARROW_SCHEMA.to_string()),
            },
            KeyValue {
                key: "my-data".to_string(),
                value: Some("stuff".to_string()),
            },
            KeyValue {
                key: "my-data-bool-key".to_string(),
                value: None,
            },
        ];
        assert_file_metadata(file_metadata, &expected_kv_meta);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_empty_recordbatch_creates_file() -> Result<()> {
        let empty_record_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(Vec::<i32>::new()))],
        )
        .expect("Failed to create empty RecordBatch");

        let tmp_dir = tempfile::TempDir::new()?;
        let path = format!("{}/empty2.parquet", tmp_dir.path().to_string_lossy());

        let ctx = SessionContext::new();
        let df = ctx.read_batch(empty_record_batch.clone())?;
        df.write_parquet(&path, crate::dataframe::DataFrameWriteOptions::new(), None)
            .await?;
        assert!(std::path::Path::new(&path).exists());

        let stream = ctx
            .read_parquet(&path, ParquetReadOptions::new())
            .await?
            .execute_stream()
            .await?;
        assert_eq!(stream.schema(), empty_record_batch.schema());
        let results = stream.collect::<Vec<_>>().await;
        assert_eq!(results.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_write_empty_parquet_from_sql() -> Result<()> {
        let ctx = SessionContext::new();

        let tmp_dir = tempfile::TempDir::new()?;
        let path = format!("{}/empty_sql.parquet", tmp_dir.path().to_string_lossy());
        let df = ctx.sql("SELECT CAST(1 AS INT) AS id LIMIT 0").await?;
        df.write_parquet(&path, crate::dataframe::DataFrameWriteOptions::new(), None)
            .await?;
        // Expected the file to exist
        assert!(std::path::Path::new(&path).exists());
        let read_df = ctx.read_parquet(&path, ParquetReadOptions::new()).await?;
        let stream = read_df.execute_stream().await?;
        assert_eq!(stream.schema().fields().len(), 1);
        assert_eq!(stream.schema().field(0).name(), "id");

        let results: Vec<_> = stream.collect().await;
        assert_eq!(results.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn parquet_sink_write_insert_schema_into_metadata() -> Result<()> {
        // expected kv metadata without schema
        let expected_without = vec![
            KeyValue {
                key: "my-data".to_string(),
                value: Some("stuff".to_string()),
            },
            KeyValue {
                key: "my-data-bool-key".to_string(),
                value: None,
            },
        ];
        // expected kv metadata with schema
        let expected_with = [
            vec![KeyValue {
                key: "ARROW:schema".to_string(),
                value: Some(ENCODED_ARROW_SCHEMA.to_string()),
            }],
            expected_without.clone(),
        ]
        .concat();

        // single threaded write, skip insert
        let opts = ParquetOptions {
            allow_single_file_parallelism: false,
            skip_arrow_metadata: true,
            ..Default::default()
        };
        let parquet_sink =
            create_written_parquet_sink_using_config("file:///", opts).await?;
        let (_, file_metadata) = get_written(parquet_sink)?;
        assert_file_metadata(file_metadata, &expected_without);

        // single threaded write, do not skip insert
        let opts = ParquetOptions {
            allow_single_file_parallelism: false,
            skip_arrow_metadata: false,
            ..Default::default()
        };
        let parquet_sink =
            create_written_parquet_sink_using_config("file:///", opts).await?;
        let (_, file_metadata) = get_written(parquet_sink)?;
        assert_file_metadata(file_metadata, &expected_with);

        // multithreaded write, skip insert
        let opts = ParquetOptions {
            allow_single_file_parallelism: true,
            maximum_parallel_row_group_writers: 2,
            maximum_buffered_record_batches_per_stream: 2,
            skip_arrow_metadata: true,
            ..Default::default()
        };
        let parquet_sink =
            create_written_parquet_sink_using_config("file:///", opts).await?;
        let (_, file_metadata) = get_written(parquet_sink)?;
        assert_file_metadata(file_metadata, &expected_without);

        // multithreaded write, do not skip insert
        let opts = ParquetOptions {
            allow_single_file_parallelism: true,
            maximum_parallel_row_group_writers: 2,
            maximum_buffered_record_batches_per_stream: 2,
            skip_arrow_metadata: false,
            ..Default::default()
        };
        let parquet_sink =
            create_written_parquet_sink_using_config("file:///", opts).await?;
        let (_, file_metadata) = get_written(parquet_sink)?;
        assert_file_metadata(file_metadata, &expected_with);

        Ok(())
    }

    #[tokio::test]
    async fn parquet_sink_write_with_extension() -> Result<()> {
        let filename = "test_file.custom_ext";
        let file_path = format!("file:///path/to/{filename}");
        let parquet_sink = create_written_parquet_sink(file_path.as_str()).await?;

        // assert written to proper path
        let (path, _) = get_written(parquet_sink)?;
        let path_parts = path.parts().collect::<Vec<_>>();
        assert_eq!(
            path_parts.len(),
            3,
            "Expected 3 path parts, instead found {}",
            path_parts.len()
        );
        assert_eq!(path_parts.last().unwrap().as_ref(), filename);

        Ok(())
    }

    #[tokio::test]
    async fn parquet_sink_write_with_directory_name() -> Result<()> {
        let file_path = "file:///path/to";
        let parquet_sink = create_written_parquet_sink(file_path).await?;

        // assert written to proper path
        let (path, _) = get_written(parquet_sink)?;
        let path_parts = path.parts().collect::<Vec<_>>();
        assert_eq!(
            path_parts.len(),
            3,
            "Expected 3 path parts, instead found {}",
            path_parts.len()
        );
        assert!(path_parts.last().unwrap().as_ref().ends_with(".parquet"));

        Ok(())
    }

    #[tokio::test]
    async fn parquet_sink_write_with_folder_ending() -> Result<()> {
        let file_path = "file:///path/to/";
        let parquet_sink = create_written_parquet_sink(file_path).await?;

        // assert written to proper path
        let (path, _) = get_written(parquet_sink)?;
        let path_parts = path.parts().collect::<Vec<_>>();
        assert_eq!(
            path_parts.len(),
            3,
            "Expected 3 path parts, instead found {}",
            path_parts.len()
        );
        assert!(path_parts.last().unwrap().as_ref().ends_with(".parquet"));

        Ok(())
    }

    async fn create_written_parquet_sink(table_path: &str) -> Result<Arc<ParquetSink>> {
        create_written_parquet_sink_using_config(table_path, ParquetOptions::default())
            .await
    }

    static ENCODED_ARROW_SCHEMA: &str = "/////5QAAAAQAAAAAAAKAAwACgAJAAQACgAAABAAAAAAAQQACAAIAAAABAAIAAAABAAAAAIAAAA8AAAABAAAANz///8UAAAADAAAAAAAAAUMAAAAAAAAAMz///8BAAAAYgAAABAAFAAQAAAADwAEAAAACAAQAAAAGAAAAAwAAAAAAAAFEAAAAAAAAAAEAAQABAAAAAEAAABhAAAA";

    async fn create_written_parquet_sink_using_config(
        table_path: &str,
        global: ParquetOptions,
    ) -> Result<Arc<ParquetSink>> {
        // schema should match the ENCODED_ARROW_SCHEMA bove
        let field_a = Field::new("a", DataType::Utf8, false);
        let field_b = Field::new("b", DataType::Utf8, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));
        let object_store_url = ObjectStoreUrl::local_filesystem();

        let file_sink_config = FileSinkConfig {
            original_url: String::default(),
            object_store_url: object_store_url.clone(),
            file_group: FileGroup::new(vec![PartitionedFile::new("/tmp".to_string(), 1)]),
            table_paths: vec![ListingTableUrl::parse(table_path)?],
            output_schema: schema.clone(),
            table_partition_cols: vec![],
            insert_op: InsertOp::Overwrite,
            keep_partition_by_columns: false,
            file_extension: "parquet".into(),
        };
        let parquet_sink = Arc::new(ParquetSink::new(
            file_sink_config,
            TableParquetOptions {
                key_value_metadata: std::collections::HashMap::from([
                    ("my-data".to_string(), Some("stuff".to_string())),
                    ("my-data-bool-key".to_string(), None),
                ]),
                global,
                ..Default::default()
            },
        ));

        // create data
        let col_a: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar"]));
        let col_b: ArrayRef = Arc::new(StringArray::from(vec!["baz", "baz"]));
        let batch = RecordBatch::try_from_iter(vec![("a", col_a), ("b", col_b)])?;

        // write stream
        FileSink::write_all(
            parquet_sink.as_ref(),
            Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::iter(vec![Ok(batch)]),
            )),
            &build_ctx(object_store_url.as_ref()),
        )
        .await?;

        Ok(parquet_sink)
    }

    fn get_written(parquet_sink: Arc<ParquetSink>) -> Result<(Path, ParquetMetaData)> {
        let mut written = parquet_sink.written();
        let written = written.drain();
        assert_eq!(
            written.len(),
            1,
            "expected a single parquet files to be written, instead found {}",
            written.len()
        );

        let (path, parquet_meta_data) = written.take(1).next().unwrap();
        Ok((path, parquet_meta_data))
    }

    fn assert_file_metadata(
        parquet_meta_data: ParquetMetaData,
        expected_kv: &Vec<KeyValue>,
    ) {
        let file_metadata = parquet_meta_data.file_metadata();
        let schema_descr = file_metadata.schema_descr();
        assert_eq!(file_metadata.num_rows(), 2, "file metadata to have 2 rows");
        assert!(
            schema_descr
                .columns()
                .iter()
                .any(|col_schema| col_schema.name() == "a"),
            "output file metadata should contain col a"
        );
        assert!(
            schema_descr
                .columns()
                .iter()
                .any(|col_schema| col_schema.name() == "b"),
            "output file metadata should contain col b"
        );

        let mut key_value_metadata = file_metadata.key_value_metadata().unwrap().clone();
        key_value_metadata.sort_by(|a, b| a.key.cmp(&b.key));
        assert_eq!(&key_value_metadata, expected_kv);
    }

    #[tokio::test]
    async fn parquet_sink_write_partitions() -> Result<()> {
        let field_a = Field::new("a", DataType::Utf8, false);
        let field_b = Field::new("b", DataType::Utf8, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));
        let object_store_url = ObjectStoreUrl::local_filesystem();

        // set file config to include partitioning on field_a
        let file_sink_config = FileSinkConfig {
            original_url: String::default(),
            object_store_url: object_store_url.clone(),
            file_group: FileGroup::new(vec![PartitionedFile::new("/tmp".to_string(), 1)]),
            table_paths: vec![ListingTableUrl::parse("file:///")?],
            output_schema: schema.clone(),
            table_partition_cols: vec![("a".to_string(), DataType::Utf8)], // add partitioning
            insert_op: InsertOp::Overwrite,
            keep_partition_by_columns: false,
            file_extension: "parquet".into(),
        };
        let parquet_sink = Arc::new(ParquetSink::new(
            file_sink_config,
            TableParquetOptions::default(),
        ));

        // create data with 2 partitions
        let col_a: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar"]));
        let col_b: ArrayRef = Arc::new(StringArray::from(vec!["baz", "baz"]));
        let batch = RecordBatch::try_from_iter(vec![("a", col_a), ("b", col_b)])?;

        // write stream
        FileSink::write_all(
            parquet_sink.as_ref(),
            Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::iter(vec![Ok(batch)]),
            )),
            &build_ctx(object_store_url.as_ref()),
        )
        .await?;

        // assert written
        let mut written = parquet_sink.written();
        let written = written.drain();
        assert_eq!(
            written.len(),
            2,
            "expected two parquet files to be written, instead found {}",
            written.len()
        );

        // check the file metadata includes partitions
        let mut expected_partitions = std::collections::HashSet::from(["a=foo", "a=bar"]);
        for (path, parquet_metadata) in written.take(2) {
            let file_metadata = parquet_metadata.file_metadata();
            let schema = file_metadata.schema_descr();
            let num_rows = file_metadata.num_rows();

            let path_parts = path.parts().collect::<Vec<_>>();
            assert_eq!(path_parts.len(), 2, "should have path prefix");

            let prefix = path_parts[0].as_ref();
            assert!(
                expected_partitions.contains(prefix),
                "expected path prefix to match partition, instead found {prefix:?}"
            );
            expected_partitions.remove(prefix);

            assert_eq!(num_rows, 1, "file metadata to have 1 row");
            assert!(
                !schema
                    .columns()
                    .iter()
                    .any(|col_schema| col_schema.name() == "a"),
                "output file metadata will not contain partitioned col a"
            );
            assert!(
                schema
                    .columns()
                    .iter()
                    .any(|col_schema| col_schema.name() == "b"),
                "output file metadata should contain col b"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn parquet_sink_write_memory_reservation() -> Result<()> {
        async fn test_memory_reservation(global: ParquetOptions) -> Result<()> {
            let field_a = Field::new("a", DataType::Utf8, false);
            let field_b = Field::new("b", DataType::Utf8, false);
            let schema = Arc::new(Schema::new(vec![field_a, field_b]));
            let object_store_url = ObjectStoreUrl::local_filesystem();

            let file_sink_config = FileSinkConfig {
                original_url: String::default(),
                object_store_url: object_store_url.clone(),
                file_group: FileGroup::new(vec![PartitionedFile::new(
                    "/tmp".to_string(),
                    1,
                )]),
                table_paths: vec![ListingTableUrl::parse("file:///")?],
                output_schema: schema.clone(),
                table_partition_cols: vec![],
                insert_op: InsertOp::Overwrite,
                keep_partition_by_columns: false,
                file_extension: "parquet".into(),
            };
            let parquet_sink = Arc::new(ParquetSink::new(
                file_sink_config,
                TableParquetOptions {
                    key_value_metadata: std::collections::HashMap::from([
                        ("my-data".to_string(), Some("stuff".to_string())),
                        ("my-data-bool-key".to_string(), None),
                    ]),
                    global,
                    ..Default::default()
                },
            ));

            // create data
            let col_a: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar"]));
            let col_b: ArrayRef = Arc::new(StringArray::from(vec!["baz", "baz"]));
            let batch = RecordBatch::try_from_iter(vec![("a", col_a), ("b", col_b)])?;

            // create task context
            let task_context = build_ctx(object_store_url.as_ref());
            assert_eq!(
                task_context.memory_pool().reserved(),
                0,
                "no bytes are reserved yet"
            );

            let mut write_task = FileSink::write_all(
                parquet_sink.as_ref(),
                Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    bounded_stream(batch, 1000),
                )),
                &task_context,
            );

            // incrementally poll and check for memory reservation
            let mut reserved_bytes = 0;
            while futures::poll!(&mut write_task).is_pending() {
                reserved_bytes += task_context.memory_pool().reserved();
                tokio::time::sleep(Duration::from_micros(1)).await;
            }
            assert!(
                reserved_bytes > 0,
                "should have bytes reserved during write"
            );
            assert_eq!(
                task_context.memory_pool().reserved(),
                0,
                "no leaking byte reservation"
            );

            Ok(())
        }

        let write_opts = ParquetOptions {
            allow_single_file_parallelism: false,
            ..Default::default()
        };
        test_memory_reservation(write_opts)
            .await
            .expect("should track for non-parallel writes");

        let row_parallel_write_opts = ParquetOptions {
            allow_single_file_parallelism: true,
            maximum_parallel_row_group_writers: 10,
            maximum_buffered_record_batches_per_stream: 1,
            ..Default::default()
        };
        test_memory_reservation(row_parallel_write_opts)
            .await
            .expect("should track for row-parallel writes");

        let col_parallel_write_opts = ParquetOptions {
            allow_single_file_parallelism: true,
            maximum_parallel_row_group_writers: 1,
            maximum_buffered_record_batches_per_stream: 2,
            ..Default::default()
        };
        test_memory_reservation(col_parallel_write_opts)
            .await
            .expect("should track for column-parallel writes");

        Ok(())
    }
}
