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

#[cfg(feature = "avro")]
pub mod avro_to_arrow;
#[cfg(feature = "avro")]
mod format;
#[cfg(feature = "avro")]
mod physical_plan;
// todo
// - deprecate old
// - split feature?

#[cfg(test)]
mod test_util {
    use datafusion::datasource::file_format::FileFormat;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::object_store::ObjectStoreUrl;
    use datafusion::datasource::physical_plan::FileScanConfig;
    use datafusion::test::object_store::local_unpartitioned_file;
    use datafusion_catalog::Session;
    use datafusion_common::Result;
    use datafusion_physical_plan::ExecutionPlan;
    use object_store::local::LocalFileSystem;
    use std::sync::Arc;

    pub async fn scan_format(
        state: &dyn Session,
        format: &dyn FileFormat,
        store_root: &str,
        file_name: &str,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let store = Arc::new(LocalFileSystem::new()) as _;
        let meta = local_unpartitioned_file(format!("{store_root}/{file_name}"));

        let file_schema = format
            .infer_schema(state, &store, std::slice::from_ref(&meta))
            .await?;

        let statistics = format
            .infer_stats(state, &store, file_schema.clone(), &meta)
            .await?;

        let file_groups = vec![vec![PartitionedFile {
            object_meta: meta,
            partition_values: vec![],
            range: None,
            statistics: None,
            extensions: None,
            metadata_size_hint: None,
        }]];

        let exec = format
            .create_physical_plan(
                state,
                FileScanConfig::new(
                    ObjectStoreUrl::local_filesystem(),
                    file_schema,
                    format.file_source(),
                )
                .with_file_groups(file_groups)
                .with_statistics(statistics)
                .with_projection(projection)
                .with_limit(limit),
                None,
            )
            .await?;
        Ok(exec)
    }
}
