// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Helpers for moving [`Statistics`] across the FFI boundary as prost-encoded
//! `datafusion_proto_common::Statistics` bytes.
//!
//! [`Statistics`] contains [`Precision<ScalarValue>`] for column min/max/sum,
//! and `ScalarValue` is a large enum that's impractical to mirror in
//! `#[repr(C)]`. The proto round-trip already exists in `datafusion-proto-common`
//! and is the same pattern used to ship filter expressions across the FFI
//! boundary, so we reuse it here.
//!
//! [`Precision<ScalarValue>`]: datafusion_common::stats::Precision

use datafusion_common::{DataFusionError, Result, Statistics};
use prost::Message;

/// Serialize [`Statistics`] to prost-encoded
/// `datafusion_proto_common::Statistics` bytes.
pub(crate) fn serialize_statistics(stats: &Statistics) -> Vec<u8> {
    datafusion_proto_common::Statistics::from(stats).encode_to_vec()
}

/// Decode prost-encoded `datafusion_proto_common::Statistics` bytes back into
/// [`Statistics`].
pub(crate) fn deserialize_statistics(bytes: &[u8]) -> Result<Statistics> {
    let proto = datafusion_proto_common::Statistics::decode(bytes).map_err(|e| {
        DataFusionError::Plan(format!("failed to decode Statistics: {e}"))
    })?;
    Statistics::try_from(&proto)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_common::stats::Precision;
    use datafusion_common::{ColumnStatistics, Statistics};

    use super::*;

    #[test]
    fn round_trip_unknown_statistics() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let original = Statistics::new_unknown(&Arc::new(schema));

        let bytes = serialize_statistics(&original);
        let observed = deserialize_statistics(&bytes).expect("decode");

        assert_eq!(observed, original);
    }

    #[test]
    fn round_trip_exact_statistics_with_scalar_values() {
        let original = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Exact(4096),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(2),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(50))),
                    min_value: Precision::Exact(ScalarValue::Int32(Some(-10))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(1234))),
                    distinct_count: Precision::Exact(40),
                    byte_size: Precision::Exact(800),
                },
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some(
                        "zebra".to_string(),
                    ))),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some(
                        "ant".to_string(),
                    ))),
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Inexact(95),
                    byte_size: Precision::Inexact(2048),
                },
            ],
        };

        let bytes = serialize_statistics(&original);
        let observed = deserialize_statistics(&bytes).expect("decode");

        assert_eq!(observed, original);
    }

    #[test]
    fn round_trip_mixed_precision() {
        let original = Statistics {
            num_rows: Precision::Inexact(42),
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Inexact(ScalarValue::Float64(Some(1.5))),
                min_value: Precision::Absent,
                sum_value: Precision::Inexact(ScalarValue::Float64(Some(63.0))),
                distinct_count: Precision::Absent,
                byte_size: Precision::Absent,
            }],
        };

        let bytes = serialize_statistics(&original);
        let observed = deserialize_statistics(&bytes).expect("decode");

        assert_eq!(observed, original);
    }
}
