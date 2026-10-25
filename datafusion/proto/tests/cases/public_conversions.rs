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

//! Compile-time guard for the `From` / `TryFrom` conversions between DataFusion
//! types and `datafusion_proto::protobuf` messages that downstream crates call.
//!
//! These impls were silently dropped once (see
//! <https://github.com/apache/datafusion/issues/24019>): they were replaced by
//! crate-local conversion traits as a stopgap during the `datafusion-proto-models`
//! extraction, and `cargo-semver-checks` has no lint for a removed hand-written
//! trait impl, so nothing caught the break. Coercing each conversion to a `fn`
//! pointer here does — moving an impl between crates is fine, removing one stops
//! compiling.
//!
//! Only the spelling is asserted. Behaviour is covered by the round-trip tests
//! next to each impl.

use datafusion_common::config::{
    CsvOptions, JsonOptions, ParquetCdcOptions, ParquetColumnOptions, ParquetOptions,
    TableParquetOptions,
};
use datafusion_common::display::StringifiedPlan;
use datafusion_common::{
    Constraint, Constraints, DataFusionError, JoinConstraint, JoinType, NullEquality,
    TableReference, UnnestOptions,
};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_sink_config::FileSinkConfig;
use datafusion_datasource::{FileRange, PartitionedFile};
use datafusion_datasource_csv::file_format::{CsvFormatFactory, CsvSink};
use datafusion_datasource_json::file_format::{JsonFormatFactory, JsonSink};
use datafusion_datasource_parquet::file_format::{ParquetFormatFactory, ParquetSink};
use datafusion_expr::dml::MergeIntoClauseKind;
use datafusion_expr::expr::NullTreatment;
use datafusion_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use datafusion_physical_expr::expressions::Column;
use datafusion_proto::protobuf;
use datafusion_proto_common::protobuf_common;

/// Asserts `T: From<F>` by naming the conversion.
fn assert_from<F, T: From<F>>() {
    let _: fn(F) -> T = From::from;
}

/// Asserts `T: TryFrom<F>` by naming the conversion.
fn assert_try_from<F, T: TryFrom<F>>() {
    let _: fn(F) -> Result<T, T::Error> = TryFrom::try_from;
}

/// Asserts `T: TryFrom<F, Error = DataFusionError>` rather than accepting the
/// blanket `TryFrom` implementation provided for an infallible `From`.
fn assert_datafusion_try_from<F, T: TryFrom<F, Error = DataFusionError>>() {
    let _: fn(F) -> Result<T, DataFusionError> = TryFrom::try_from;
}

#[test]
fn file_scan_conversions_are_std_traits() {
    assert_try_from::<&protobuf::PartitionedFile, PartitionedFile>();
    assert_try_from::<&PartitionedFile, protobuf::PartitionedFile>();
    assert_try_from::<&protobuf::FileRange, FileRange>();
    assert_try_from::<&FileRange, protobuf::FileRange>();
    assert_try_from::<&protobuf::FileGroup, FileGroup>();
    assert_try_from::<&FileGroup, protobuf::FileGroup>();
    assert_from::<&protobuf::PhysicalColumn, Column>();
    assert_from::<&Column, protobuf::PhysicalColumn>();
    assert_try_from::<&[PartitionedFile], protobuf::FileGroup>();
}

#[test]
fn file_sink_conversions_are_std_traits() {
    assert_try_from::<&protobuf::FileSinkConfig, FileSinkConfig>();
    assert_try_from::<&FileSinkConfig, protobuf::FileSinkConfig>();
    assert_try_from::<&protobuf::JsonSink, JsonSink>();
    assert_try_from::<&JsonSink, protobuf::JsonSink>();
    assert_try_from::<&protobuf::CsvSink, CsvSink>();
    assert_try_from::<&CsvSink, protobuf::CsvSink>();
    assert_try_from::<&protobuf::ParquetSink, ParquetSink>();
    assert_try_from::<&ParquetSink, protobuf::ParquetSink>();
}

#[test]
fn window_frame_conversions_are_std_traits() {
    assert_try_from::<protobuf::WindowFrame, WindowFrame>();
    assert_try_from::<&WindowFrame, protobuf::WindowFrame>();
    assert_try_from::<protobuf::WindowFrameBound, WindowFrameBound>();
    assert_try_from::<&WindowFrameBound, protobuf::WindowFrameBound>();
    assert_from::<protobuf::WindowFrameUnits, WindowFrameUnits>();
    assert_from::<WindowFrameUnits, protobuf::WindowFrameUnits>();
    assert_from::<protobuf::NullTreatment, NullTreatment>();
    assert_from::<NullTreatment, protobuf::NullTreatment>();
    assert_from::<protobuf::merge_into_clause_node::Kind, MergeIntoClauseKind>();
    assert_from::<MergeIntoClauseKind, protobuf::merge_into_clause_node::Kind>();
}

#[test]
fn common_type_conversions_are_std_traits() {
    assert_from::<&protobuf::UnnestOptions, UnnestOptions>();
    assert_from::<&UnnestOptions, protobuf::UnnestOptions>();
    assert_try_from::<protobuf::TableReference, TableReference>();
    assert_from::<TableReference, protobuf::TableReference>();
    assert_from::<&protobuf::StringifiedPlan, StringifiedPlan>();
    assert_from::<&StringifiedPlan, protobuf::StringifiedPlan>();
    assert_from::<protobuf::JoinType, JoinType>();
    assert_from::<JoinType, protobuf::JoinType>();
    assert_from::<protobuf::JoinConstraint, JoinConstraint>();
    assert_from::<JoinConstraint, protobuf::JoinConstraint>();
    assert_from::<protobuf::NullEquality, NullEquality>();
    assert_from::<NullEquality, protobuf::NullEquality>();
    assert_datafusion_try_from::<protobuf_common::Constraint, Constraint>();
    assert_datafusion_try_from::<&protobuf_common::Constraint, Constraint>();
    assert_datafusion_try_from::<protobuf_common::Constraints, Constraints>();
    assert_datafusion_try_from::<&protobuf_common::Constraints, Constraints>();
}

#[test]
fn file_format_option_conversions_are_std_traits() {
    assert_datafusion_try_from::<&protobuf::CsvOptions, CsvOptions>();
    assert_datafusion_try_from::<&protobuf::JsonOptions, JsonOptions>();
    assert_datafusion_try_from::<&protobuf::ParquetOptions, ParquetOptions>();
    assert_from::<protobuf::ParquetColumnOptions, ParquetColumnOptions>();
    assert_datafusion_try_from::<protobuf::ParquetCdcOptions, ParquetCdcOptions>();
    assert_datafusion_try_from::<&protobuf::TableParquetOptions, TableParquetOptions>();
    assert_from::<&CsvFormatFactory, protobuf::CsvOptions>();
    assert_from::<&JsonFormatFactory, protobuf::JsonOptions>();
    assert_from::<&ParquetFormatFactory, protobuf::TableParquetOptions>();
}
