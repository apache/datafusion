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
    PartitionedFile,
};

use std::sync::Arc;

use crate::file_groups::FileGroup;
use arrow::datatypes::SchemaRef;
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, Result, ScalarValue, Statistics};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};

/// Minimal [`crate::file::FileSource`] implementation for use in tests.
#[derive(Clone, Default)]
pub(crate) struct MockSource {
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
}

impl FileSource for MockSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn with_schema(&self, _schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.projected_statistics = Some(statistics);
        Arc::new(source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self
            .projected_statistics
            .as_ref()
            .expect("projected_statistics must be set")
            .clone())
    }

    fn file_type(&self) -> &str {
        "mock"
    }
}

/// Generates test files with min-max statistics in different overlap patterns
pub fn generate_test_files(num_files: usize, overlap_factor: f64) -> Vec<FileGroup> {
    let mut files = Vec::with_capacity(num_files);
    if num_files == 0 {
        return vec![];
    }
    let range_size = if overlap_factor == 0.0 {
        100 / num_files as i64
    } else {
        (100.0 / (overlap_factor * num_files as f64)).max(1.0) as i64
    };

    for i in 0..num_files {
        let base = (i as f64 * range_size as f64 * (1.0 - overlap_factor)) as i64;
        let min = base as f64;
        let max = (base + range_size) as f64;

        let file = PartitionedFile {
            object_meta: ObjectMeta {
                location: Path::from(format!("file_{}.parquet", i)),
                last_modified: chrono::Utc::now(),
                size: 1000,
                e_tag: None,
                version: None,
            },
            partition_values: vec![],
            range: None,
            statistics: Some(Statistics {
                num_rows: Precision::Exact(100),
                total_byte_size: Precision::Exact(1000),
                column_statistics: vec![ColumnStatistics {
                    null_count: Precision::Exact(0),
                    max_value: Precision::Exact(ScalarValue::Float64(Some(max))),
                    min_value: Precision::Exact(ScalarValue::Float64(Some(min))),
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                }],
            }),
            extensions: None,
            metadata_size_hint: None,
        };
        files.push(file);
    }

    vec![FileGroup::new(files)]
}

// Helper function to verify that files within each group maintain sort order
pub fn verify_sort_integrity(file_groups: &[FileGroup]) -> bool {
    for group in file_groups {
        let files = group.iter().collect::<Vec<_>>();
        for i in 1..files.len() {
            let prev_file = files[i - 1];
            let curr_file = files[i];

            // Check if the min value of current file is greater than max value of previous file
            if let (Some(prev_stats), Some(curr_stats)) =
                (&prev_file.statistics, &curr_file.statistics)
            {
                let prev_max = &prev_stats.column_statistics[0].max_value;
                let curr_min = &curr_stats.column_statistics[0].min_value;
                if curr_min.get_value().unwrap() <= prev_max.get_value().unwrap() {
                    return false;
                }
            }
        }
    }
    true
}
