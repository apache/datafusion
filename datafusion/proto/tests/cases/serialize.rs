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

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use datafusion::execution::FunctionRegistry;
use datafusion::prelude::SessionContext;
use datafusion_expr::{col, create_udf, lit, ColumnarValue};
use datafusion_expr::{Expr, Volatility};
use datafusion_functions::string;
use datafusion_proto::bytes::Serializeable;
use datafusion_proto::logical_plan::to_proto::serialize_expr;
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;

#[test]
#[should_panic(
    expected = "Error decoding expr as protobuf: failed to decode Protobuf message"
)]
fn bad_decode() {
    Expr::from_bytes(b"Leet").unwrap();
}

#[test]
#[cfg(feature = "json")]
fn plan_to_json() {
    use datafusion_common::DFSchema;
    use datafusion_expr::{logical_plan::EmptyRelation, LogicalPlan};
    use datafusion_proto::bytes::logical_plan_to_json;

    let plan = LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::empty()),
    });
    let actual = logical_plan_to_json(&plan).unwrap();
    let expected = r#"{"emptyRelation":{}}"#.to_string();
    assert_eq!(actual, expected);
}

#[test]
#[cfg(feature = "json")]
fn json_to_plan() {
    use datafusion_expr::LogicalPlan;
    use datafusion_proto::bytes::logical_plan_from_json;

    let input = r#"{"emptyRelation":{}}"#.to_string();
    let ctx = SessionContext::new();
    let actual = logical_plan_from_json(&input, &ctx).unwrap();
    let result = matches!(actual, LogicalPlan::EmptyRelation(_));
    assert!(result, "Should parse empty relation");
}

#[test]
fn udf_roundtrip_with_registry() {
    let ctx = context_with_udf();

    let expr = ctx
        .udf("dummy")
        .expect("could not find udf")
        .call(vec![lit("")]);

    let bytes = expr.to_bytes().unwrap();
    let deserialized_expr = Expr::from_bytes_with_registry(&bytes, &ctx).unwrap();

    assert_eq!(expr, deserialized_expr);
}

#[test]
#[should_panic(
    expected = "No function registry provided to deserialize, so can not deserialize User Defined Function 'dummy'"
)]
fn udf_roundtrip_without_registry() {
    let ctx = context_with_udf();

    let expr = ctx
        .udf("dummy")
        .expect("could not find udf")
        .call(vec![lit("")]);

    let bytes = expr.to_bytes().unwrap();
    // should explode
    Expr::from_bytes(&bytes).unwrap();
}

fn roundtrip_expr(expr: &Expr) -> Expr {
    let bytes = expr.to_bytes().unwrap();
    Expr::from_bytes(&bytes).unwrap()
}

#[test]
fn exact_roundtrip_linearized_binary_expr() {
    // (((A AND B) AND C) AND D)
    let expr_ordered = col("A").and(col("B")).and(col("C")).and(col("D"));
    assert_eq!(expr_ordered, roundtrip_expr(&expr_ordered));

    // Ensure that no other variation becomes equal
    let other_variants = vec![
        // (((B AND A) AND C) AND D)
        col("B").and(col("A")).and(col("C")).and(col("D")),
        // (((A AND C) AND B) AND D)
        col("A").and(col("C")).and(col("B")).and(col("D")),
        // (((A AND B) AND D) AND C)
        col("A").and(col("B")).and(col("D")).and(col("C")),
        // A AND (B AND (C AND D)))
        col("A").and(col("B").and(col("C").and(col("D")))),
    ];
    for case in other_variants {
        // Each variant is still equal to itself
        assert_eq!(case, roundtrip_expr(&case));

        // But non of them is equal to the original
        assert_ne!(expr_ordered, roundtrip_expr(&case));
        assert_ne!(roundtrip_expr(&expr_ordered), roundtrip_expr(&case));
    }
}

#[test]
fn roundtrip_qualified_alias() {
    let qual_alias = col("c1").alias_qualified(Some("my_table"), "my_column");
    assert_eq!(qual_alias, roundtrip_expr(&qual_alias));
}

#[test]
fn roundtrip_deeply_nested_binary_expr() {
    // We need more stack space so this doesn't overflow in dev builds
    std::thread::Builder::new()
        .stack_size(10_000_000)
        .spawn(|| {
            let n = 100;
            // a < 5
            let basic_expr = col("a").lt(lit(5i32));
            // (a < 5) OR (a < 5) OR (a < 5) OR ...
            let or_chain =
                (0..n).fold(basic_expr.clone(), |expr, _| expr.or(basic_expr.clone()));
            // (a < 5) OR (a < 5) AND (a < 5) OR (a < 5) AND (a < 5) AND (a < 5) OR ...
            let expr =
                (0..n).fold(or_chain.clone(), |expr, _| expr.and(or_chain.clone()));

            // Should work fine.
            let bytes = expr.to_bytes().unwrap();

            let decoded_expr = Expr::from_bytes(&bytes)
                .expect("serialization worked, so deserialization should work as well");
            assert_eq!(decoded_expr, expr);
        })
        .expect("spawning thread")
        .join()
        .expect("joining thread");
}

#[test]
fn roundtrip_deeply_nested_binary_expr_reverse_order() {
    // We need more stack space so this doesn't overflow in dev builds
    std::thread::Builder::new()
        .stack_size(10_000_000)
        .spawn(|| {
            let n = 100;

            // a < 5
            let expr_base = col("a").lt(lit(5i32));

            // ((a < 5 AND a < 5) AND a < 5) AND ...
            let and_chain =
                (0..n).fold(expr_base.clone(), |expr, _| expr.and(expr_base.clone()));

            // a < 5 AND (a < 5 AND (a < 5 AND ...))
            let expr = expr_base.and(and_chain);

            // Should work fine.
            let bytes = expr.to_bytes().unwrap();

            let decoded_expr = Expr::from_bytes(&bytes)
                .expect("serialization worked, so deserialization should work as well");
            assert_eq!(decoded_expr, expr);
        })
        .expect("spawning thread")
        .join()
        .expect("joining thread");
}

#[test]
fn roundtrip_deeply_nested() {
    // we need more stack space so this doesn't overflow in dev builds
    std::thread::Builder::new().stack_size(20_000_000).spawn(|| {
            // don't know what "too much" is, so let's slowly try to increase complexity
            let n_max = 100;

            for n in 1..n_max {
                println!("testing: {n}");

                let expr_base = col("a").lt(lit(5i32));
                // Generate a tree of AND and OR expressions (no subsequent ANDs or ORs).
                let expr = (0..n).fold(expr_base.clone(), |expr, n| if n % 2 == 0 { expr.and(expr_base.clone()) } else { expr.or(expr_base.clone()) });

                // Convert it to an opaque form
                let bytes = match expr.to_bytes() {
                    Ok(bytes) => bytes,
                    Err(_) => {
                        // found expression that is too deeply nested
                        return;
                    }
                };

                // Decode bytes from somewhere (over network, etc.
                let decoded_expr = Expr::from_bytes(&bytes).expect("serialization worked, so deserialization should work as well");
                assert_eq!(expr, decoded_expr);
            }

            panic!("did not find a 'too deeply nested' expression, tested up to a depth of {n_max}")
        }).expect("spawning thread").join().expect("joining thread");
}

/// return a `SessionContext` with a `dummy` function registered as a UDF
fn context_with_udf() -> SessionContext {
    let scalar_fn = Arc::new(|args: &[ColumnarValue]| {
        let ColumnarValue::Array(array) = &args[0] else {
            panic!("should be array")
        };
        Ok(ColumnarValue::from(Arc::new(array.clone()) as ArrayRef))
    });

    let udf = create_udf(
        "dummy",
        vec![DataType::Utf8],
        Arc::new(DataType::Utf8),
        Volatility::Immutable,
        scalar_fn,
    );

    let ctx = SessionContext::new();
    ctx.register_udf(udf);

    ctx
}

#[test]
fn test_expression_serialization_roundtrip() {
    use datafusion_common::ScalarValue;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_proto::logical_plan::from_proto::parse_expr;

    let ctx = SessionContext::new();
    let lit = Expr::Literal(ScalarValue::Utf8(None));
    for function in string::functions() {
        // default to 4 args (though some exprs like substr have error checking)
        let num_args = 4;
        let args: Vec<_> = std::iter::repeat(&lit).take(num_args).cloned().collect();
        let expr = Expr::ScalarFunction(ScalarFunction::new_udf(function, args));

        let extension_codec = DefaultLogicalExtensionCodec {};
        let proto = serialize_expr(&expr, &extension_codec).unwrap();
        let deserialize = parse_expr(&proto, &ctx, &extension_codec).unwrap();

        let serialize_name = extract_function_name(&expr);
        let deserialize_name = extract_function_name(&deserialize);

        assert_eq!(serialize_name, deserialize_name);
    }

    /// Extracts the first part of a function name
    /// 'foo(bar)' -> 'foo'
    fn extract_function_name(expr: &Expr) -> String {
        let name = expr.schema_name().to_string();
        name.split('(').next().unwrap().to_string()
    }
}
