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

//! Push-based Parquet decoder setup.
//!
//! This module is responsible for configuring [`ParquetPushDecoderBuilder`]
//! instances. The opener uses one [`DecoderBuilderConfig`] per file scan and
//! applies it to every decoder run (a scan may be split into multiple runs
//! when, for example, fully matched row groups can skip the row filter).

use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, RowSelectionPolicy};
use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;

use crate::access_plan::PreparedAccessPlan;
use crate::row_filter::ParquetReadPlan;

/// Shared options applied to every [`ParquetPushDecoderBuilder`] in a file scan.
///
/// A single scan may produce multiple decoders (for example, when fully matched
/// row groups split the scan into consecutive runs with different filter
/// requirements). All decoders in that scan share the same projection, batch
/// size, metrics sink, and selection policy.
pub(crate) struct DecoderBuilderConfig<'a> {
    pub(crate) read_plan: &'a ParquetReadPlan,
    pub(crate) batch_size: usize,
    pub(crate) arrow_reader_metrics: &'a ArrowReaderMetrics,
    pub(crate) force_filter_selections: bool,
    pub(crate) decoder_limit: Option<usize>,
}

impl DecoderBuilderConfig<'_> {
    /// Build a [`ParquetPushDecoderBuilder`] for a single decoder run.
    ///
    /// The caller is expected to attach the run-specific
    /// [`RowFilter`](parquet::arrow::arrow_reader::RowFilter) and predicate
    /// cache size on the returned builder.
    pub(crate) fn build(
        &self,
        prepared_access_plan: PreparedAccessPlan,
        metadata: ArrowReaderMetadata,
    ) -> ParquetPushDecoderBuilder {
        let mut builder = ParquetPushDecoderBuilder::new_with_metadata(metadata)
            .with_projection(self.read_plan.projection_mask.clone())
            .with_batch_size(self.batch_size)
            .with_metrics(self.arrow_reader_metrics.clone());
        if self.force_filter_selections {
            builder = builder.with_row_selection_policy(RowSelectionPolicy::Selectors);
        }
        if let Some(row_selection) = prepared_access_plan.row_selection {
            builder = builder.with_row_selection(row_selection);
        }
        builder = builder.with_row_groups(prepared_access_plan.row_group_indexes);
        if let Some(limit) = self.decoder_limit {
            builder = builder.with_limit(limit);
        }
        builder
    }
}
