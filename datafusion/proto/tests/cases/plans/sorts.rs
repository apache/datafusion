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

//! `SortExec` and `SortPreservingMergeExec`.

use super::{roundtrip_test, roundtrip_test_and_return};
use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{
    NormalizeFloatZeroExpr, PhysicalSortExpr, col,
};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_proto::physical_plan::{
    DefaultPhysicalExtensionCodec, DefaultPhysicalProtoConverter,
};
use std::sync::Arc;
use std::vec;

#[test]
fn roundtrip_sort() -> Result<()> {
    let field_a = Field::new("a", DataType::Boolean, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));
    let sort_exprs = [
        PhysicalSortExpr {
            expr: col("a", &schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: false,
            },
        },
        PhysicalSortExpr {
            expr: col("b", &schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        },
    ]
    .into();
    roundtrip_test(Arc::new(SortExec::new(
        sort_exprs,
        Arc::new(EmptyExec::new(schema)),
    )))
}

#[test]
fn roundtrip_sort_with_normalized_float_zero() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, false)]));
    let sort_exprs = [PhysicalSortExpr {
        expr: Arc::new(NormalizeFloatZeroExpr::new(col("a", &schema)?)),
        options: SortOptions::default(),
    }]
    .into();
    roundtrip_test(Arc::new(SortExec::new(
        sort_exprs,
        Arc::new(EmptyExec::new(schema)),
    )))
}

#[test]
fn roundtrip_sort_preserve_partitioning() -> Result<()> {
    let field_a = Field::new("a", DataType::Boolean, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));
    let sort_exprs: LexOrdering = [
        PhysicalSortExpr {
            expr: col("a", &schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: false,
            },
        },
        PhysicalSortExpr {
            expr: col("b", &schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        },
    ]
    .into();

    roundtrip_test(Arc::new(SortExec::new(
        sort_exprs.clone(),
        Arc::new(EmptyExec::new(schema.clone())),
    )))?;

    roundtrip_test(Arc::new(
        SortExec::new(sort_exprs, Arc::new(EmptyExec::new(schema)))
            .with_preserve_partitioning(true),
    ))
}

/// `SortExec::fetch` turns a sort into a top-k sort. Losing it during serde
/// would silently widen the result set, so exercise the `Some(..)` state
/// explicitly (`roundtrip_sort` only covers `None`).
///
/// `SortExec` currently derives `Debug`, so `roundtrip_test`'s
/// `format!("{plan:?}")` comparison does observe `fetch`. The assertions below
/// go through the accessor instead so that this coverage does not silently
/// disappear if `SortExec` ever grows a hand-written `Debug` impl.
#[test]
fn roundtrip_sort_with_fetch() -> Result<()> {
    let field_a = Field::new("a", DataType::Boolean, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));
    let sort_exprs: LexOrdering = [
        PhysicalSortExpr {
            expr: col("a", &schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: false,
            },
        },
        PhysicalSortExpr {
            expr: col("b", &schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        },
    ]
    .into();

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};

    let roundtripped = roundtrip_test_and_return(
        Arc::new(
            SortExec::new(
                sort_exprs.clone(),
                Arc::new(EmptyExec::new(Arc::clone(&schema))),
            )
            .with_fetch(Some(7)),
        ),
        &ctx,
        &codec,
        &proto_converter,
    )?;
    let roundtripped = roundtripped
        .downcast_ref::<SortExec>()
        .expect("should decode back into a SortExec");
    assert_eq!(roundtripped.fetch(), Some(7));
    assert_eq!(roundtripped.expr(), &sort_exprs);

    // `fetch` combined with `preserve_partitioning`, since both share the same
    // proto node.
    let roundtripped = roundtrip_test_and_return(
        Arc::new(
            SortExec::new(sort_exprs.clone(), Arc::new(EmptyExec::new(schema)))
                .with_fetch(Some(3))
                .with_preserve_partitioning(true),
        ),
        &ctx,
        &codec,
        &proto_converter,
    )?;
    let roundtripped = roundtripped
        .downcast_ref::<SortExec>()
        .expect("should decode back into a SortExec");
    assert_eq!(roundtripped.fetch(), Some(3));
    assert!(roundtripped.preserve_partitioning());
    Ok(())
}

/// Round trip a [`SortPreservingMergeExec`], which had no dedicated round trip
/// test at all.
///
/// Covers everything that is actually on the wire for this plan: the input, the
/// sort expressions and `fetch` in both its `None` and `Some(..)` states.
///
/// Note that `SortPreservingMergeExec::enable_round_robin_repartition` is
/// deliberately *not* asserted on here: it has no field in
/// `SortPreservingMergeExecNode`, so it is not serialized and decoding always
/// restores the `true` default from `SortPreservingMergeExec::new`. Asserting
/// round trip equality on it would give a false sense of coverage.
///
/// `SortPreservingMergeExec` derives `Debug`, so `roundtrip_test`'s
/// `format!("{plan:?}")` comparison does observe `expr` and `fetch`. The
/// assertions below use the accessors so the coverage survives a future
/// hand-written `Debug` impl.
#[test]
fn roundtrip_sort_preserving_merge() -> Result<()> {
    let field_a = Field::new("a", DataType::Boolean, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));
    let sort_exprs: LexOrdering = [
        PhysicalSortExpr {
            expr: col("a", &schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: false,
            },
        },
        PhysicalSortExpr {
            expr: col("b", &schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        },
    ]
    .into();

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};

    // No fetch: `fetch` is encoded as -1 and must decode back to `None`.
    let roundtripped = roundtrip_test_and_return(
        Arc::new(SortPreservingMergeExec::new(
            sort_exprs.clone(),
            Arc::new(EmptyExec::new(Arc::clone(&schema))),
        )),
        &ctx,
        &codec,
        &proto_converter,
    )?;
    let roundtripped = roundtripped
        .downcast_ref::<SortPreservingMergeExec>()
        .expect("should decode back into a SortPreservingMergeExec");
    assert_eq!(roundtripped.fetch(), None);
    assert_eq!(roundtripped.expr(), &sort_exprs);
    assert_eq!(roundtripped.input().schema(), schema);

    // With a fetch: dropping it would turn a bounded merge into an unbounded
    // one and change the query result.
    let roundtripped = roundtrip_test_and_return(
        Arc::new(
            SortPreservingMergeExec::new(
                sort_exprs.clone(),
                Arc::new(EmptyExec::new(Arc::clone(&schema))),
            )
            .with_fetch(Some(11)),
        ),
        &ctx,
        &codec,
        &proto_converter,
    )?;
    let roundtripped = roundtripped
        .downcast_ref::<SortPreservingMergeExec>()
        .expect("should decode back into a SortPreservingMergeExec");
    assert_eq!(roundtripped.fetch(), Some(11));
    assert_eq!(roundtripped.expr(), &sort_exprs);
    Ok(())
}
