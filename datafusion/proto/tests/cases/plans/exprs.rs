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

//! Physical expressions embedded in plans, including the binary
//! expression linearization.

use super::{roundtrip_test, roundtrip_test_and_return};
use arrow::datatypes::Fields;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, IntervalUnit, Schema};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{
    BinaryExpr, Column, PhysicalSortExpr, SqlSimilarToPattern, binary, col, like, lit,
};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::repartition::RangeExpr;
use datafusion::physical_plan::{
    ExecutionPlan, PhysicalExpr, RangePartitioning, SplitPoint,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion_common::Result;
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DefaultPhysicalExtensionCodec, DefaultPhysicalProtoConverter,
    PhysicalPlanDecodeContext, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::PhysicalPlanNode;
use std::sync::Arc;
use std::vec;

#[test]
fn roundtrip_date_time_interval() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("some_date", DataType::Date32, false),
        Field::new(
            "some_interval",
            DataType::Interval(IntervalUnit::DayTime),
            false,
        ),
    ]);
    let input = Arc::new(EmptyExec::new(Arc::new(schema.clone())));
    let date_expr = col("some_date", &schema)?;
    let literal_expr = col("some_interval", &schema)?;
    let date_time_interval_expr =
        binary(date_expr, Operator::Plus, literal_expr, &schema)?;
    let plan = Arc::new(ProjectionExec::try_new(
        vec![ProjectionExpr {
            expr: date_time_interval_expr,
            alias: "result".to_string(),
        }],
        input,
    )?);
    roundtrip_test(plan)
}

#[test]
fn roundtrip_like() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
    ]);
    let input = Arc::new(EmptyExec::new(Arc::new(schema.clone())));
    let like_expr = like(
        false,
        false,
        col("a", &schema)?,
        col("b", &schema)?,
        &schema,
    )?;
    let plan = Arc::new(ProjectionExec::try_new(
        vec![ProjectionExpr {
            expr: like_expr,
            alias: "result".to_string(),
        }],
        input,
    )?);
    roundtrip_test(plan)
}

/// Test that HashTableLookupExpr serializes to lit(true)
///
/// HashTableLookupExpr contains a runtime hash table that cannot be serialized.
/// The serialization code replaces it with lit(true) which is safe because
/// it's a performance optimization filter, not a correctness requirement.
#[test]
fn roundtrip_hash_table_lookup_expr_to_lit() -> Result<()> {
    use datafusion::physical_plan::joins::join_hash_map::JoinHashMapU32;
    use datafusion::physical_plan::joins::{HashTableLookupExpr, Map};

    // Create a simple schema and input plan
    let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int64, false)]));
    let input = Arc::new(EmptyExec::new(schema.clone()));

    // Create a HashTableLookupExpr - it will be replaced with lit(true) during serialization
    let hash_map = Arc::new(Map::HashMap(Box::new(JoinHashMapU32::with_capacity(0))));
    let on_columns = vec![col("col", &schema)?];
    let lookup_expr: Arc<dyn PhysicalExpr> = Arc::new(HashTableLookupExpr::new(
        on_columns,
        datafusion::physical_plan::joins::SeededRandomState::with_seed(0),
        hash_map,
        "test_lookup".to_string(),
    ));

    // Create a filter with the lookup expression
    let filter = Arc::new(FilterExec::try_new(lookup_expr, input)?);

    // Serialize
    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};

    let proto: PhysicalPlanNode =
        PhysicalPlanNode::try_from_physical_plan(filter.clone(), &codec)
            .expect("serialization should succeed");

    // Deserialize
    let result: Arc<dyn ExecutionPlan> = proto
        .try_into_physical_plan(&ctx.task_ctx(), &codec)
        .expect("deserialization should succeed");

    // The deserialized plan should have lit(true) instead of HashTableLookupExpr
    // Verify the filter predicate is a Literal(true)
    let result_filter = result.downcast_ref::<FilterExec>().unwrap();
    let predicate = result_filter.predicate();
    let literal = predicate.downcast_ref::<Literal>().unwrap();
    assert_eq!(*literal.value(), ScalarValue::Boolean(Some(true)));

    Ok(())
}

#[test]
fn roundtrip_hash_expr() -> Result<()> {
    use datafusion::physical_plan::joins::{HashExpr, SeededRandomState};

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, false),
    ]));

    // Create a HashExpr with test columns and seeds
    let on_columns = vec![col("a", &schema)?, col("b", &schema)?];
    let hash_expr: Arc<dyn PhysicalExpr> = Arc::new(HashExpr::new(
        on_columns,
        SeededRandomState::with_seed(0), // arbitrary random seed for testing
        "test_hash".to_string(),
    ));

    // Wrap in a filter by comparing hash value to a literal
    // hash_expr > 0 is always boolean
    let filter_expr = binary(hash_expr, Operator::Gt, lit(0u64), &schema)?;
    let filter = Arc::new(FilterExec::try_new(
        filter_expr,
        Arc::new(EmptyExec::new(schema)),
    )?);

    // Confirm that the debug string contains the random state seeds
    assert!(
        format!("{filter:?}").contains("test_hash(a@0, b@1, [0])"),
        "Debug string missing seeds: {filter:?}"
    );
    roundtrip_test(filter)
}

#[test]
fn roundtrip_range_expr() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Float64, false),
        Field::new("b", DataType::Float64, false),
    ]));
    let options = [SortOptions::new(true, true), SortOptions::new(false, false)];
    let range_partitioning = RangePartitioning::try_new(
        [
            PhysicalSortExpr::new(col("a", &schema)?, options[0]),
            PhysicalSortExpr::new(col("b", &schema)?, options[1]),
        ]
        .into(),
        vec![SplitPoint::new(vec![
            ScalarValue::Float64(Some(0.0)),
            ScalarValue::Float64(Some(1.0)),
        ])],
    )?;
    let range_expr: Arc<dyn PhysicalExpr> = Arc::new(RangeExpr::try_new(
        // Expression remapping may produce duplicate children. Preserve both
        // so their sort options stay aligned with the split-point values.
        vec![col("a", &schema)?, col("a", &schema)?],
        &range_partitioning,
        &schema,
    )?);
    let filter_expr = binary(range_expr, Operator::Eq, lit(0u64), &schema)?;
    let plan = Arc::new(FilterExec::try_new(
        filter_expr,
        Arc::new(EmptyExec::new(Arc::clone(&schema))),
    )?);

    let ctx = SessionContext::new();
    let result = roundtrip_test_and_return(
        plan,
        &ctx,
        &DefaultPhysicalExtensionCodec {},
        &DefaultPhysicalProtoConverter {},
    )?;
    let filter = result.downcast_ref::<FilterExec>().unwrap();
    let binary = filter.predicate().downcast_ref::<BinaryExpr>().unwrap();
    let range_expr = binary.left().downcast_ref::<RangeExpr>().unwrap();
    assert_eq!(range_expr.split_points(), range_partitioning.split_points());
    assert_eq!(range_expr.sort_options(), &options);
    let children = range_expr.on_columns();
    assert_eq!(children.len(), 2);
    for child in children {
        let column = child.downcast_ref::<Column>().unwrap();
        assert_eq!((column.name(), column.index()), ("a", 0));
    }

    Ok(())
}

#[test]
fn roundtrip_sql_similar_to_pattern() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
    let expr: Arc<dyn PhysicalExpr> =
        Arc::new(SqlSimilarToPattern::new(col("a", &schema)?));

    let codec = DefaultPhysicalExtensionCodec {};
    let converter = DefaultPhysicalProtoConverter {};
    let proto = converter.physical_expr_to_proto(&expr, &codec)?;
    let ctx = SessionContext::new();
    let task_ctx = ctx.task_ctx();
    let decode_ctx = PhysicalPlanDecodeContext::new(task_ctx.as_ref(), &codec);
    let decoded = converter.proto_to_physical_expr(&proto, &schema, &decode_ctx)?;

    assert_eq!(format!("{expr:?}"), format!("{decoded:?}"));
    Ok(())
}

#[test]
fn roundtrip_call_null_scalar_struct_dict() -> Result<()> {
    let data_type = DataType::Struct(Fields::from(vec![Field::new(
        "item",
        DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
        true,
    )]));

    let schema = Arc::new(Schema::new(vec![Field::new("a", data_type.clone(), true)]));
    let scan = Arc::new(EmptyExec::new(Arc::clone(&schema)));
    let scalar = lit(ScalarValue::try_from(data_type)?);
    let filter = Arc::new(FilterExec::try_new(
        Arc::new(BinaryExpr::new(scalar, Operator::Eq, col("a", &schema)?)),
        scan,
    )?);

    roundtrip_test(filter)
}

/// Test that a chain of the same operator (a AND b AND c) is linearized
/// and roundtrips correctly.
#[test]
fn roundtrip_binary_expr_chain_same_op() -> Result<()> {
    let field_a = Field::new("a", DataType::Boolean, false);
    let field_b = Field::new("b", DataType::Boolean, false);
    let field_c = Field::new("c", DataType::Boolean, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b, field_c]));
    let ab = binary(
        col("a", &schema)?,
        Operator::And,
        col("b", &schema)?,
        &schema,
    )?;
    let abc = binary(ab, Operator::And, col("c", &schema)?, &schema)?;
    roundtrip_test(Arc::new(FilterExec::try_new(
        abc,
        Arc::new(EmptyExec::new(schema)),
    )?))
}

/// Test that mixed operators (a AND b OR c) are NOT linearized together —
/// only chains of the same operator are flattened.
#[test]
fn roundtrip_binary_expr_mixed_ops() -> Result<()> {
    let field_a = Field::new("a", DataType::Boolean, false);
    let field_b = Field::new("b", DataType::Boolean, false);
    let field_c = Field::new("c", DataType::Boolean, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b, field_c]));
    // (a AND b) OR c — AND and OR are different operators, so linearization stops
    let a_and_b = binary(
        col("a", &schema)?,
        Operator::And,
        col("b", &schema)?,
        &schema,
    )?;
    let expr = binary(a_and_b, Operator::Or, col("c", &schema)?, &schema)?;
    roundtrip_test(Arc::new(FilterExec::try_new(
        expr,
        Arc::new(EmptyExec::new(schema)),
    )?))
}

/// Test that a deeply nested chain of AND expressions (like many WHERE conditions)
/// roundtrips correctly. This is the scenario from issue #18602.
#[test]
fn roundtrip_binary_expr_deeply_nested_and_chain() -> Result<()> {
    let field_a = Field::new("a", DataType::Boolean, false);
    let schema = Arc::new(Schema::new(vec![field_a]));

    // Build a chain: a AND a AND a AND ... (100 times)
    let col_a = col("a", &schema)?;
    let mut expr = Arc::clone(&col_a);
    for _ in 0..99 {
        expr = binary(expr, Operator::And, Arc::clone(&col_a), &schema)?;
    }

    roundtrip_test(Arc::new(FilterExec::try_new(
        expr,
        Arc::new(EmptyExec::new(schema)),
    )?))
}

/// Test that a deeply nested chain of OR expressions roundtrips correctly.
#[test]
fn roundtrip_binary_expr_deeply_nested_or_chain() -> Result<()> {
    let field_a = Field::new("a", DataType::Boolean, false);
    let schema = Arc::new(Schema::new(vec![field_a]));

    let col_a = col("a", &schema)?;
    let mut expr = Arc::clone(&col_a);
    for _ in 0..99 {
        expr = binary(expr, Operator::Or, Arc::clone(&col_a), &schema)?;
    }

    roundtrip_test(Arc::new(FilterExec::try_new(
        expr,
        Arc::new(EmptyExec::new(schema)),
    )?))
}

/// Test that alternating AND/OR operators produce correct results —
/// each sub-chain gets linearized independently.
#[test]
fn roundtrip_binary_expr_alternating_and_or() -> Result<()> {
    let field_a = Field::new("a", DataType::Boolean, false);
    let field_b = Field::new("b", DataType::Boolean, false);
    let field_c = Field::new("c", DataType::Boolean, false);
    let field_d = Field::new("d", DataType::Boolean, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b, field_c, field_d]));

    // (a AND b) OR (c AND d)
    let a_and_b = binary(
        col("a", &schema)?,
        Operator::And,
        col("b", &schema)?,
        &schema,
    )?;
    let c_and_d = binary(
        col("c", &schema)?,
        Operator::And,
        col("d", &schema)?,
        &schema,
    )?;
    let expr = binary(a_and_b, Operator::Or, c_and_d, &schema)?;

    roundtrip_test(Arc::new(FilterExec::try_new(
        expr,
        Arc::new(EmptyExec::new(schema)),
    )?))
}

/// Verify that the linearized proto format has a flat operands list
/// rather than deeply nested l/r fields.
#[test]
fn test_linearization_produces_flat_operands() -> Result<()> {
    // Build: a AND a AND a AND a (4 operands, 3 levels of nesting)
    let col_a: Arc<dyn PhysicalExpr> = Arc::new(Column::new("a", 0));
    let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::clone(&col_a),
                Operator::And,
                Arc::clone(&col_a),
            )),
            Operator::And,
            Arc::clone(&col_a),
        )),
        Operator::And,
        Arc::clone(&col_a),
    ));

    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let proto = proto_converter.physical_expr_to_proto(&expr, &codec)?;

    // The top-level should use the operands field with 4 entries
    match &proto.expr_type {
        Some(protobuf::physical_expr_node::ExprType::BinaryExpr(b)) => {
            assert!(
                b.l.is_none(),
                "l should be None when using linearized operands"
            );
            assert!(
                b.r.is_none(),
                "r should be None when using linearized operands"
            );
            assert_eq!(
                b.operands.len(),
                4,
                "Expected 4 linearized operands for a AND a AND a AND a"
            );
            assert_eq!(b.op, "And");
        }
        other => panic!("Expected BinaryExpr, got {other:?}"),
    }

    Ok(())
}

/// Test that linearization stops when encountering a different operator.
/// For (a AND b) OR c, only the top-level OR should be represented, and
/// the left-hand AND subtree should be a separate nested BinaryExpr.
#[test]
fn test_linearization_stops_at_different_op() -> Result<()> {
    // (a AND b) OR c
    let a_and_b: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("a", 0)),
        Operator::And,
        Arc::new(Column::new("b", 1)),
    ));
    let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        a_and_b,
        Operator::Or,
        Arc::new(Column::new("c", 2)),
    ));

    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let proto = proto_converter.physical_expr_to_proto(&expr, &codec)?;

    // The top-level OR should have only 2 operands (can't linearize through AND)
    match &proto.expr_type {
        Some(protobuf::physical_expr_node::ExprType::BinaryExpr(b)) => {
            assert_eq!(
                b.operands.len(),
                2,
                "Expected 2 operands for (a AND b) OR c"
            );
            assert_eq!(b.op, "Or");
            // The first operand should be a nested AND BinaryExpr
            match &b.operands[0].expr_type {
                Some(protobuf::physical_expr_node::ExprType::BinaryExpr(inner)) => {
                    assert_eq!(inner.op, "And");
                    assert_eq!(inner.operands.len(), 2);
                }
                other => panic!("Expected inner BinaryExpr(AND), got {other:?}"),
            }
        }
        other => panic!("Expected BinaryExpr, got {other:?}"),
    }

    Ok(())
}
