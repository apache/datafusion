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
    use std::fs;
    use substrait::proto::expression::field_reference::{ReferenceType, RootType};
    use substrait::proto::expression::reference_segment;
    use substrait::proto::expression::{ReferenceSegment, RexType};
    use substrait::proto::function_argument::ArgType;
    use substrait::proto::plan_rel::RelType;
    use substrait::proto::rel_common::{Emit, EmitKind};
    use substrait::proto::r#type::{I64, Kind as TypeKind, List, Nullability, Struct};
    use substrait::proto::{Expression, RelCommon, Type, rel};

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
    async fn higher_order_function() -> Result<()> {
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
        let plan = to_substrait_plan(&datafusion_plan, &ctx.state())?
            .as_ref()
            .clone();

        let relation = plan.relations.first().unwrap().rel_type.as_ref();
        let root_rel = match relation {
            Some(RelType::Root(root)) => root.input.as_ref().unwrap(),
            _ => panic!("expected Root"),
        };

        let Some(rel::RelType::Project(p)) = root_rel.rel_type.as_ref() else {
            panic!("expected Project at top of plan")
        };

        let mut params = vec![];
        let mut lambda_refs = vec![];

        collect_lambda_ref(&p.expressions[0], &mut params, &mut lambda_refs);

        let nullable_i64 = Type {
            kind: Some(TypeKind::I64(I64 {
                type_variation_reference: 0,
                nullability: Nullability::Nullable as i32,
            })),
        };

        let inner_lambda_struct = Struct {
            // v, j
            types: vec![nullable_i64.clone(); 2],
            type_variation_reference: 0,
            nullability: Nullability::Required as i32,
        };

        assert_eq!(
            params,
            vec![
                Struct {
                    types: vec![
                        // v
                        Type {
                            kind: Some(TypeKind::List(Box::new(List {
                                r#type: Some(Box::new(nullable_i64.clone())),
                                type_variation_reference: 0,
                                nullability: Nullability::Nullable as i32
                            })))
                        },
                        // i
                        nullable_i64,
                    ],
                    type_variation_reference: 0,
                    nullability: Nullability::Required as i32,
                },
                inner_lambda_struct.clone(),
                inner_lambda_struct,
            ]
        );

        assert_eq!(
            lambda_refs,
            vec![
                // first inner array_transform2 argument: outer v
                (0, 0),
                // first inner lambda body: v * i * j
                (0, 0),
                (1, 1),
                (0, 1),
                // second inner array_transform2 argument: outer v
                (0, 0),
                // second inner lambda body: v * i * j
                (0, 0),
                (1, 1),
                (0, 1),
            ]
        );

        Ok(())
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

    // Recursively walks a expression tree depth-first, collecting in visit order:
    // - `params`: the parameter struct of each Lambda encountered
    // - `lambda_refs`: every field reference whose root is a LambdaParameterReference,
    //   recorded as (steps_out, field_index) so tests can assert which enclosing
    //   lambda each reference resolves to and which parameter within it.
    fn collect_lambda_ref(
        expr: &Expression,
        params: &mut Vec<Struct>,
        lambda_refs: &mut Vec<(u32, i32)>,
    ) {
        if let Some(rex_type) = &expr.rex_type {
            match rex_type {
                RexType::Selection(field_reference) => {
                    if let (
                        Some(ReferenceType::DirectReference(ReferenceSegment {
                            reference_type:
                                Some(reference_segment::ReferenceType::StructField(
                                    struct_field,
                                )),
                        })),
                        Some(RootType::LambdaParameterReference(lambda_param_ref)),
                    ) = (&field_reference.reference_type, &field_reference.root_type)
                    {
                        lambda_refs.push((lambda_param_ref.steps_out, struct_field.field))
                    }
                }
                RexType::ScalarFunction(scalar_function) => {
                    for arg in &scalar_function.arguments {
                        match &arg.arg_type {
                            Some(ArgType::Value(value)) => {
                                collect_lambda_ref(value, params, lambda_refs)
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                RexType::Lambda(lambda) => {
                    if let Some(parameters) = &lambda.parameters {
                        params.push(parameters.clone());
                    }
                    if let Some(body) = &lambda.body {
                        collect_lambda_ref(body, params, lambda_refs);
                    }
                }
                RexType::Literal(_literal) => {}
                _ => unreachable!(),
            }
        }
    }
}
