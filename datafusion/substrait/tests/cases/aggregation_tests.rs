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

//! Tests to verify aggregation relation handling in Substrait

#[cfg(test)]
mod tests {
    use crate::utils::test::{add_plan_schemas_to_ctx, read_json};
    use datafusion::common::Result;
    use datafusion::dataframe::DataFrame;
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use insta::assert_snapshot;
    use substrait::proto::aggregate_rel::Measure;
    use substrait::proto::expression::field_reference::ReferenceType;
    use substrait::proto::expression::reference_segment::ReferenceType as SegRefType;
    use substrait::proto::expression::{FieldReference, ReferenceSegment, RexType};
    use substrait::proto::extensions::simple_extension_declaration::MappingType;
    use substrait::proto::extensions::SimpleExtensionDeclaration;
    use substrait::proto::function_argument::ArgType;
    use substrait::proto::read_rel::{ReadType, VirtualTable};
    use substrait::proto::rel::RelType;
    use substrait::proto::{
        AggregateFunction, Expression, FunctionArgument, NamedStruct, Plan, PlanRel,
        ReadRel, Rel, RelRoot, Type, r#type,
    };

    #[tokio::test]
    async fn no_grouping_set() -> Result<()> {
        let proto_plan =
            read_json("tests/testdata/test_plans/aggregate_groupings/no_groupings.json");
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
            plan,
            @r"
        Aggregate: groupBy=[[]], aggr=[[sum(c0) AS summation]]
          EmptyRelation: rows=0
        "
        );

        // Trigger execution to ensure plan validity
        DataFrame::new(ctx.state(), plan).show().await?;

        Ok(())
    }

    #[tokio::test]
    async fn one_grouping_set() -> Result<()> {
        let proto_plan = read_json(
            "tests/testdata/test_plans/aggregate_groupings/single_grouping.json",
        );
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
            plan,
            @r"
        Aggregate: groupBy=[[c0]], aggr=[[sum(c0) AS summation]]
          EmptyRelation: rows=0
        "
        );

        // Trigger execution to ensure plan validity
        DataFrame::new(ctx.state(), plan).show().await?;

        Ok(())
    }

    /// When root names rename struct fields inside an aggregate measure's
    /// return type (e.g. `List<Struct{c0,c1}>` → `List<Struct{one,two}>`),
    /// `rename_expressions` injects `Expr::Cast` around the aggregate
    /// function. The physical planner rejects Cast-wrapped aggregates.
    /// This test verifies that the consumer wraps the Aggregate in a
    /// Projection instead.
    #[tokio::test]
    #[expect(deprecated)]
    async fn aggregate_with_struct_field_rename() -> Result<()> {
        // Build a Substrait plan:
        //   ReadRel(VirtualTable) with one column: List<Struct{c0: Utf8, c1: Utf8}>
        //   AggregateRel with array_agg on that column
        //   Root names rename struct fields: c0 → one, c1 → two

        let utf8_nullable = Type {
            kind: Some(r#type::Kind::String(r#type::String {
                type_variation_reference: 0,
                nullability: r#type::Nullability::Nullable as i32,
            })),
        };

        let struct_type = r#type::Struct {
            types: vec![utf8_nullable.clone(), utf8_nullable.clone()],
            type_variation_reference: 0,
            nullability: r#type::Nullability::Nullable as i32,
        };

        let list_of_struct = Type {
            kind: Some(r#type::Kind::List(Box::new(r#type::List {
                r#type: Some(Box::new(Type {
                    kind: Some(r#type::Kind::Struct(struct_type.clone())),
                })),
                type_variation_reference: 0,
                nullability: r#type::Nullability::Nullable as i32,
            }))),
        };

        // ReadRel with VirtualTable (empty) and base_schema
        let read_rel = Rel {
            rel_type: Some(RelType::Read(Box::new(ReadRel {
                common: None,
                base_schema: Some(NamedStruct {
                    names: vec![
                        "col0".to_string(),
                        "c0".to_string(),
                        "c1".to_string(),
                    ],
                    r#struct: Some(r#type::Struct {
                        types: vec![list_of_struct.clone()],
                        type_variation_reference: 0,
                        nullability: r#type::Nullability::Required as i32,
                    }),
                }),
                filter: None,
                best_effort_filter: None,
                projection: None,
                advanced_extension: None,
                read_type: Some(ReadType::VirtualTable(VirtualTable {
                    values: vec![],
                    expressions: vec![],
                })),
            }))),
        };

        // AggregateRel with array_agg(col0)
        let field_ref = Expression {
            rex_type: Some(RexType::Selection(Box::new(FieldReference {
                reference_type: Some(ReferenceType::DirectReference(
                    ReferenceSegment {
                        reference_type: Some(SegRefType::StructField(Box::new(
                            substrait::proto::expression::reference_segment::StructField {
                                field: 0,
                                child: None,
                            },
                        ))),
                    },
                )),
                root_type: Some(
                    substrait::proto::expression::field_reference::RootType::RootReference(
                        substrait::proto::expression::field_reference::RootReference {},
                    ),
                ),
            }))),
        };

        let aggregate_rel = Rel {
            rel_type: Some(RelType::Aggregate(Box::new(
                substrait::proto::AggregateRel {
                    common: None,
                    input: Some(Box::new(read_rel)),
                    grouping_expressions: vec![],
                    groupings: vec![],
                    measures: vec![Measure {
                        measure: Some(AggregateFunction {
                            function_reference: 1,
                            arguments: vec![FunctionArgument {
                                arg_type: Some(ArgType::Value(field_ref)),
                            }],
                            sorts: vec![],
                            output_type: Some(list_of_struct),
                            invocation: 1, // AGGREGATION_INVOCATION_ALL
                            phase: 3,      // AGGREGATION_PHASE_INITIAL_TO_RESULT
                            args: vec![],
                            options: vec![],
                        }),
                        filter: None,
                    }],
                    advanced_extension: None,
                },
            ))),
        };

        // Root names: rename struct fields c0 → one, c1 → two
        let proto_plan = Plan {
            version: None,
            extension_uris: vec![substrait::proto::extensions::SimpleExtensionUri {
                extension_uri_anchor: 1,
                uri: "/functions_aggregate.yaml".to_string(),
            }],
            extension_urns: vec![],
            extensions: vec![SimpleExtensionDeclaration {
                mapping_type: Some(MappingType::ExtensionFunction(
                    substrait::proto::extensions::simple_extension_declaration::ExtensionFunction {
                        extension_uri_reference: 1,
                        extension_urn_reference: 0,
                        function_anchor: 1,
                        name: "array_agg:list".to_string(),
                    },
                )),
            }],
            relations: vec![PlanRel {
                rel_type: Some(substrait::proto::plan_rel::RelType::Root(RelRoot {
                    input: Some(aggregate_rel),
                    names: vec![
                        "result".to_string(),
                        "one".to_string(),
                        "two".to_string(),
                    ],
                })),
            }],
            advanced_extensions: None,
            expected_type_urls: vec![],
            parameter_bindings: vec![],
            type_aliases: vec![],
        };

        let ctx = SessionContext::new();
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        // The plan should contain a Projection wrapping the Aggregate,
        // because rename_expressions injects Cast for the struct field rename.
        let plan_str = format!("{plan}");
        assert!(
            plan_str.contains("Projection:"),
            "Expected Projection wrapper but got:\n{plan_str}"
        );
        assert!(
            plan_str.contains("Aggregate:"),
            "Expected Aggregate in plan but got:\n{plan_str}"
        );

        // Execute to confirm the physical planner accepts this plan
        DataFrame::new(ctx.state(), plan).show().await?;

        Ok(())
    }
}
