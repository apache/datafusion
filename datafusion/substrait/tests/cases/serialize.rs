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

#[cfg(test)]
mod tests {
    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use datafusion_substrait::serializer;

    use datafusion::error::Result;
    use datafusion::prelude::*;

    use insta::assert_snapshot;
    use std::collections::HashMap;
    use std::fs;
    use substrait::proto::expression::field_reference::{ReferenceType, RootType};
    use substrait::proto::expression::reference_segment::ReferenceType as SegmentType;
    use substrait::proto::expression::{Lambda, RexType, ScalarFunction};
    use substrait::proto::extensions::simple_extension_declaration::MappingType;
    use substrait::proto::function_argument::ArgType;
    use substrait::proto::plan_rel::RelType;
    use substrait::proto::rel_common::{Emit, EmitKind};
    use substrait::proto::r#type::{I64, Kind as TypeKind, List, Nullability};
    use substrait::proto::{Expression, FunctionArgument, RelCommon, Type, rel};

    use crate::cases::roundtrip_logical_plan::higher_order_function_ctx;

    #[tokio::test]
    async fn serialize_to_file() -> Result<()> {
        let ctx = create_context().await?;
        let path = "tests/serialize_to_file.bin";
        let sql = "SELECT a, b FROM data";

        // Test case 1: serializing to a non-existing file should succeed.
        serializer::serialize(sql, &ctx, path).await?;
        serializer::deserialize(path).await?;

        // Test case 2: serializing to an existing file should fail.
        let got = serializer::serialize(sql, &ctx, path)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            [
                "File exists", // unix
                "os error 80"  // windows
            ]
            .iter()
            .any(|s| got.contains(s))
        );

        fs::remove_file(path)?;

        Ok(())
    }

    #[tokio::test]
    async fn serialize_simple_select() -> Result<()> {
        let ctx = create_context().await?;
        let path = "tests/simple_select.bin";
        let sql = "SELECT a, b FROM data";
        // Test reference
        let df_ref = ctx.sql(sql).await?;
        let plan_ref = df_ref.into_optimized_plan()?;
        // Test
        // Write substrait plan to file
        serializer::serialize(sql, &ctx, path).await?;
        // Read substrait plan from file
        let proto = serializer::deserialize(path).await?;
        // Check plan equality
        let plan = from_substrait_plan(&ctx.state(), &proto).await?;
        let plan_str_ref = format!("{plan_ref}");
        let plan_str = format!("{plan}");
        assert_eq!(plan_str_ref, plan_str);
        // Delete test binary file
        fs::remove_file(path)?;

        Ok(())
    }

    #[tokio::test]
    async fn table_scan_without_projection() -> Result<()> {
        let ctx = create_context().await?;
        let table = provider_as_source(ctx.table_provider("data").await?);
        let table_scan = LogicalPlanBuilder::scan("data", table, None)?.build()?;
        let convert_result = to_substrait_plan(&table_scan, &ctx.state());
        assert!(convert_result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn include_remaps_for_projects() -> Result<()> {
        let ctx = create_context().await?;
        let df = ctx.sql("SELECT b, a + a, a FROM data").await?;
        let datafusion_plan = df.into_optimized_plan()?;

        assert_snapshot!(
                    format!("{}", datafusion_plan),
                    @r"
        Projection: data.b, data.a + data.a, data.a
          TableScan: data projection=[a, b]
        "
        ,
                );

        let plan = to_substrait_plan(&datafusion_plan, &ctx.state())?
            .as_ref()
            .clone();

        let relation = plan.relations.first().unwrap().rel_type.as_ref();
        let root_rel = match relation {
            Some(RelType::Root(root)) => root.input.as_ref().unwrap(),
            _ => panic!("expected Root"),
        };
        if let Some(rel::RelType::Project(p)) = root_rel.rel_type.as_ref() {
            // The input has 2 columns [a, b], the Projection has 3 expressions [b, a + a, a]
            // The required output mapping is [2,3,4], which skips the 2 input columns.
            assert_emit(p.common.as_ref(), vec![2, 3, 4]);

            if let Some(rel::RelType::Read(r)) =
                p.input.as_ref().unwrap().rel_type.as_ref()
            {
                let mask_expression = r.projection.as_ref().unwrap();
                let select = mask_expression.select.as_ref().unwrap();
                assert_eq!(
                    2,
                    select.struct_items.len(),
                    "Read outputs two columns: a, b"
                );
                return Ok(());
            }
        }
        panic!("plan did not match expected structure")
    }

    #[tokio::test]
    async fn include_remaps_for_windows() -> Result<()> {
        let ctx = create_context().await?;
        // let df = ctx.sql("SELECT a, b, lead(b) OVER (PARTITION BY a) FROM data").await?;
        let df = ctx
            .sql("SELECT b, RANK() OVER (PARTITION BY a), c FROM data;")
            .await?;
        let datafusion_plan = df.into_optimized_plan()?;
        assert_snapshot!(
                    datafusion_plan,
                    @r"
        Projection: data.b, rank() PARTITION BY [data.a] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, data.c
          WindowAggr: windowExpr=[[rank() PARTITION BY [data.a] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
            TableScan: data projection=[a, b, c]
        "
        ,
                );

        let plan = to_substrait_plan(&datafusion_plan, &ctx.state())?
            .as_ref()
            .clone();

        let relation = plan.relations.first().unwrap().rel_type.as_ref();
        let root_rel = match relation {
            Some(RelType::Root(root)) => root.input.as_ref().unwrap(),
            _ => panic!("expected Root"),
        };

        if let Some(rel::RelType::Project(p1)) = root_rel.rel_type.as_ref() {
            // The WindowAggr outputs 4 columns, the Projection has 4 columns
            assert_emit(p1.common.as_ref(), vec![4, 5, 6]);

            if let Some(rel::RelType::Project(p2)) =
                p1.input.as_ref().unwrap().rel_type.as_ref()
            {
                // The input has 3 columns, the WindowAggr has 4 expression
                assert_emit(p2.common.as_ref(), vec![3, 4, 5, 6]);

                if let Some(rel::RelType::Read(r)) =
                    p2.input.as_ref().unwrap().rel_type.as_ref()
                {
                    let mask_expression = r.projection.as_ref().unwrap();
                    let select = mask_expression.select.as_ref().unwrap();
                    assert_eq!(
                        3,
                        select.struct_items.len(),
                        "Read outputs three columns: a, b, c"
                    );
                    return Ok(());
                }
            }
        }
        panic!("plan did not match expected structure")
    }

    #[tokio::test]
    async fn higher_order_function42() -> Result<()> {
        let ctx = higher_order_function_ctx().await?;
        let df = ctx
            .sql(
                "SELECT array_transform2(
                [[data3.p1]],
                (v, i) -> array_concat(
                    -- when entering this expression, inner v is pushed into the producer and shadows outer v, but after exiting this,
                    -- it should be removed and unshadow the outer v, so that it can be used in the next expression
                    array_transform2(v, (v, j) -> v * i * j),
                    array_transform2(v, (v, j) -> v * i * j)
                )
            ) from data3"
            )
            .await?;
        let datafusion_plan = df.into_optimized_plan()?;
        assert_snapshot!(
            datafusion_plan,
            @"
        Projection: array_transform2(make_array(make_array(data3.p1)), (v, i) -> array_concat(array_transform2(v, (v, j) -> v * i * j), array_transform2(v, (v, j) -> v * i * j)))
          TableScan: data3 projection=[p1]
        "
        );

        let plan = to_substrait_plan(&datafusion_plan, &ctx.state())?
            .as_ref()
            .clone();

        let functions: HashMap<u32, String> = plan
            .extensions
            .iter()
            .filter_map(|e| match e.mapping_type.as_ref()? {
                MappingType::ExtensionFunction(f) => {
                    Some((f.function_anchor, f.name.clone()))
                }
                _ => None,
            })
            .collect();

        let relation = plan.relations.first().unwrap().rel_type.as_ref();
        let root_rel = match relation {
            Some(RelType::Root(root)) => root.input.as_ref().unwrap(),
            _ => panic!("expected Root"),
        };

        let Some(rel::RelType::Project(p)) = root_rel.rel_type.as_ref() else {
            panic!("expected Project at top of plan")
        };

        //note: checking only lambda arguments
        let nullable_i64 = TypeKind::I64(I64 {
            type_variation_reference: 0,
            nullability: Nullability::Nullable as i32,
        });

        // Outer call: array_transform2(make_array(make_array(p1)), lambda)
        let outer = expect_scalar_function(&p.expressions[0]);
        assert_eq!(functions[&outer.function_reference], "array_transform2");
        assert_eq!(outer.arguments.len(), 2);

        // arg 1: outer lambda with two parameters (list<i64>, i64) for (v, i)
        let outer_lambda = expect_lambda_arg(&outer.arguments[1]);
        let outer_params = &outer_lambda.parameters.as_ref().unwrap();
        assert_eq!(outer_params.nullability, Nullability::Required as i32);
        assert_eq!(outer_params.types.len(), 2);
        assert_eq!(
            outer_params.types[0].kind.as_ref().unwrap(),
            &TypeKind::List(Box::new(List {
                r#type: Some(Box::new(Type {
                    kind: Some(nullable_i64.clone())
                })),
                type_variation_reference: 0,
                nullability: Nullability::Nullable as i32
            }))
        );
        assert_eq!(outer_params.types[1].kind.as_ref().unwrap(), &nullable_i64);

        // outer lambda body: array_concat(array_transform2(...), array_transform2(...))
        let concat = expect_scalar_function(outer_lambda.body.as_ref().unwrap());
        assert_eq!(functions[&concat.function_reference], "array_concat");
        assert_eq!(concat.arguments.len(), 2);

        for arg in &concat.arguments {
            // each arg: array_transform2(v, inner_lambda)
            let inner_call = expect_scalar_function(expect_value(arg));
            assert_eq!(
                functions[&inner_call.function_reference],
                "array_transform2"
            );
            assert_eq!(inner_call.arguments.len(), 2);

            // inner_call arg 0: outer v (steps_out=0, field=0 from outer lambda's body)
            assert_lambda_param_ref(&inner_call.arguments[0], 0, 0);

            // inner_call arg 1: inner lambda with two i64 parameters for (v, j)
            let inner_lambda = expect_lambda_arg(&inner_call.arguments[1]);
            let inner_params = &inner_lambda.parameters.as_ref().unwrap();
            assert_eq!(inner_params.nullability, Nullability::Required as i32);
            assert_eq!(inner_params.types.len(), 2);
            assert_eq!(inner_params.types[0].kind.as_ref().unwrap(), &nullable_i64);
            assert_eq!(inner_params.types[1].kind.as_ref().unwrap(), &nullable_i64);

            // inner lambda body: multiply(multiply(v, i), j) -- v * i * j
            let mul = expect_scalar_function(inner_lambda.body.as_ref().unwrap());
            assert_eq!(functions[&mul.function_reference], "multiply");
            assert_eq!(mul.arguments.len(), 2);

            // mul arg 0: multiply(v, i)
            let inner_mul = expect_scalar_function(expect_value(&mul.arguments[0]));
            assert_eq!(functions[&inner_mul.function_reference], "multiply");
            assert_eq!(inner_mul.arguments.len(), 2);
            // inner v: current (inner) lambda, field 0
            assert_lambda_param_ref(&inner_mul.arguments[0], 0, 0);
            // outer i: one lambda out, field 1
            assert_lambda_param_ref(&inner_mul.arguments[1], 1, 1);

            // mul arg 1: inner j (current lambda, field 1)
            assert_lambda_param_ref(&mul.arguments[1], 0, 1);
        }

        Ok(())
    }

    fn assert_lambda_param_ref(arg: &FunctionArgument, steps_out: u32, field: i32) {
        let expr = expect_value(arg);
        let field_ref = match expr.rex_type.as_ref().unwrap() {
            RexType::Selection(s) => s.as_ref(),
            _ => panic!("expected lambda-parameter selection"),
        };
        let lambda_param_ref = match field_ref.root_type.as_ref() {
            Some(RootType::LambdaParameterReference(lpr)) => lpr,
            _ => panic!("expected lambda-parameter reference"),
        };
        let direct_ref = match field_ref.reference_type.as_ref().unwrap() {
            ReferenceType::DirectReference(d) => d,
            _ => panic!("expected direct reference"),
        };
        let struct_field = match direct_ref.reference_type.as_ref().unwrap() {
            SegmentType::StructField(sf) => sf,
            _ => panic!("expected struct field segment"),
        };
        assert_eq!(
            lambda_param_ref.steps_out, steps_out,
            "unexpected steps_out"
        );
        assert_eq!(struct_field.field, field, "unexpected struct field");
    }

    fn expect_value(arg: &FunctionArgument) -> &Expression {
        match arg.arg_type.as_ref().unwrap() {
            ArgType::Value(v) => v,
            _ => panic!("expected value FunctionArgument"),
        }
    }

    fn expect_scalar_function(expr: &Expression) -> &ScalarFunction {
        match expr.rex_type.as_ref().unwrap() {
            RexType::ScalarFunction(s) => s,
            _ => panic!("expected ScalarFunction expression"),
        }
    }

    fn expect_lambda_arg(arg: &FunctionArgument) -> &Lambda {
        match expect_value(arg).rex_type.as_ref().unwrap() {
            RexType::Lambda(l) => l.as_ref(),
            _ => panic!("expected Lambda expression"),
        }
    }

    fn assert_emit(rel_common: Option<&RelCommon>, output_mapping: Vec<i32>) {
        assert_eq!(
            rel_common.unwrap().emit_kind.clone(),
            Some(EmitKind::Emit(Emit { output_mapping }))
        );
    }

    async fn create_context() -> Result<SessionContext> {
        let ctx = SessionContext::new();
        ctx.register_csv("data", "tests/testdata/data.csv", CsvReadOptions::new())
            .await?;
        ctx.register_csv("data2", "tests/testdata/data.csv", CsvReadOptions::new())
            .await?;
        Ok(ctx)
    }
}
