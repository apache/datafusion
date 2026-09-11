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

use crate::{
    file::FileSource, file_scan_config::FileScanConfig, file_stream::FileOpener,
};

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion_common::{Result, tree_node::TreeNodeRecursion};
use datafusion_physical_expr::{PhysicalExpr, expressions::Column};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_storage::StorageBinding;

/// Minimal [`crate::file::FileSource`] implementation for use in tests.
#[derive(Clone)]
pub(crate) struct MockSource {
    metrics: ExecutionPlanMetricsSet,
    filter: Option<Arc<dyn PhysicalExpr>>,
    table_schema: crate::table_schema::TableSchema,
    projection: crate::projection::SplitProjection,
    file_opener: Option<Arc<dyn FileOpener>>,
}

impl Default for MockSource {
    fn default() -> Self {
        let table_schema =
            crate::table_schema::TableSchema::from(Arc::new(Schema::empty()));
        Self {
            metrics: ExecutionPlanMetricsSet::new(),
            filter: None,
            projection: crate::projection::SplitProjection::unprojected(&table_schema),
            table_schema,
            file_opener: None,
        }
    }
}

impl MockSource {
    pub fn new(table_schema: impl Into<crate::table_schema::TableSchema>) -> Self {
        let table_schema = table_schema.into();
        Self {
            metrics: ExecutionPlanMetricsSet::new(),
            filter: None,
            projection: crate::projection::SplitProjection::unprojected(&table_schema),
            table_schema,
            file_opener: None,
        }
    }

    pub fn with_filter(mut self, filter: Arc<dyn PhysicalExpr>) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn with_file_opener(mut self, file_opener: Arc<dyn FileOpener>) -> Self {
        self.file_opener = Some(file_opener);
        self
    }
}

impl FileSource for MockSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<StorageBinding>,
        _base_config: &FileScanConfig,
        _partition: usize,
        _access_context: datafusion_storage::FileAccessContext,
    ) -> Result<Arc<dyn FileOpener>> {
        self.file_opener.clone().ok_or_else(|| {
            datafusion_common::internal_datafusion_err!("MockSource missing FileOpener")
        })
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.filter.clone()
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "mock"
    }

    fn table_schema(&self) -> &crate::table_schema::TableSchema {
        &self.table_schema
    }

    fn try_pushdown_projection(
        &self,
        projection: &datafusion_physical_plan::projection::ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let new_projection = self.projection.source.try_merge(projection)?;
        let split_projection = crate::projection::SplitProjection::new(
            self.table_schema.file_schema(),
            &new_projection,
        );
        source.projection = split_projection;
        Ok(Some(Arc::new(source)))
    }

    fn projection(
        &self,
    ) -> Option<&datafusion_physical_plan::projection::ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
}

/// Create a column expression
pub(crate) fn col(name: &str, schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(Column::new_with_schema(name, schema)?))
}

/// Chunk sizes exercised by every parameterised test.
///
/// `usize::MAX` is intentionally included: `ChunkedStore` treats it as
/// "one chunk containing everything", giving the single-chunk fast path.
pub(crate) const CHUNK_SIZES: &[usize] = &[1, 2, 3, 4, 5, 7, 8, 11, 13, 16, usize::MAX];

/// A reader with controllable chunk boundaries for stream alignment tests.
pub(crate) fn make_chunked_reader(
    data: &[u8],
    chunk_size: usize,
) -> Arc<dyn datafusion_storage::FileReader> {
    #[derive(Debug)]
    struct Reader {
        data: bytes::Bytes,
        chunk_size: usize,
    }
    #[async_trait::async_trait]
    impl datafusion_storage::FileReader for Reader {
        async fn read_range(
            &self,
            range: datafusion_storage::ReadRange,
        ) -> datafusion_storage::Result<bytes::Bytes> {
            let range = match range {
                datafusion_storage::ReadRange::Bounded(r) => r,
                datafusion_storage::ReadRange::Suffix(n) => {
                    (self.data.len() as u64).saturating_sub(n)..self.data.len() as u64
                }
            };
            Ok(self.data.slice(range.start as usize..range.end as usize))
        }

        async fn read_ranges(
            &self,
            ranges: Vec<std::ops::Range<u64>>,
        ) -> datafusion_storage::Result<Vec<bytes::Bytes>> {
            Ok(ranges
                .into_iter()
                .map(|r| self.data.slice(r.start as usize..r.end as usize))
                .collect())
        }
        fn stream(
            self: Arc<Self>,
            range: Option<datafusion_storage::ReadRange>,
        ) -> futures::stream::BoxStream<'static, datafusion_storage::Result<bytes::Bytes>>
        {
            use futures::StreamExt;
            let size = self.data.len() as u64;
            let range = match range {
                Some(datafusion_storage::ReadRange::Bounded(r)) => r,
                Some(datafusion_storage::ReadRange::Suffix(n)) => {
                    size.saturating_sub(n)..size
                }
                None => 0..size,
            };
            futures::stream::unfold(
                (self, range.start as usize, range.end as usize),
                |(reader, start, end)| async move {
                    if start == end {
                        return None;
                    }
                    let next = start.saturating_add(reader.chunk_size).min(end);
                    let bytes = reader.data.slice(start..next);
                    Some((Ok(bytes), (reader, next, end)))
                },
            )
            .boxed()
        }
    }
    Arc::new(Reader {
        data: bytes::Bytes::copy_from_slice(data),
        chunk_size,
    })
}
