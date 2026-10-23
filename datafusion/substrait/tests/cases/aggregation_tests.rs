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
    use datafusion::arrow::array::record_batch;
    use datafusion::arrow::datatypes as arrow_schema;
    use datafusion::common::{Result, ScalarValue, TableReference};
    use datafusion::dataframe::DataFrame;
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use insta::assert_snapshot;
    use prost::Message;
    use serde_json::json;
    use substrait::proto::{AggregationPhase, Plan, expression, plan_rel, rel};

    fn aggregate_phase_plan(phase: i32, rooted: bool) -> Plan {
        let i64_type = json!({"i64": {"nullability": "NULLABILITY_REQUIRED"}});
        let output_type = if phase == AggregationPhase::InitialToIntermediate as i32 {
            json!({"struct": {"types": [i64_type.clone(), i64_type.clone()], "nullability": "NULLABILITY_REQUIRED"}})
        } else {
            json!({"fp64": {"nullability": "NULLABILITY_NULLABLE"}})
        };
        let rel = json!({"aggregate": {
            "input": {"read": {
                "baseSchema": {"names": ["c0"], "struct": {
                    "types": [i64_type], "nullability": "NULLABILITY_REQUIRED"
                }},
                "namedTable": {"names": ["t_avg"]}
            }},
            "measures": [{"measure": {
                "functionReference": 1,
                "outputType": output_type,
                "arguments": [{"value": {"selection": {
                    "directReference": {"structField": {}}, "rootReference": {}
                }}}]
            }}]
        }});
        let relation = if rooted {
            let names = if phase == AggregationPhase::InitialToIntermediate as i32 {
                vec!["average", "sum", "count"]
            } else {
                vec!["average"]
            };
            json!({"root": {"input": rel, "names": names}})
        } else {
            json!({"rel": rel})
        };
        let mut plan: Plan = serde_json::from_value(json!({
            "extensions": [{"extensionFunction": {"functionAnchor": 1, "name": "avg:i64"}}],
            "relations": [relation]
        }))
        .unwrap();
        let relation = match plan.relations[0].rel_type.as_mut().unwrap() {
            plan_rel::RelType::Rel(rel) => rel,
            plan_rel::RelType::Root(root) => root.input.as_mut().unwrap(),
        };
        let Some(rel::RelType::Aggregate(aggregate)) = relation.rel_type.as_mut() else {
            panic!("expected aggregate");
        };
        aggregate.measures[0].measure.as_mut().unwrap().phase = phase;
        Plan::decode(plan.encode_to_vec().as_slice()).unwrap()
    }

    async fn aggregate_phase_context() -> Result<SessionContext> {
        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE t_avg AS SELECT column1 AS c0 FROM (VALUES (1::BIGINT), (2::BIGINT))")
            .await?
            .collect()
            .await?;
        Ok(ctx)
    }

    #[tokio::test]
    async fn aggregate_supported_phases() -> Result<()> {
        let ctx = aggregate_phase_context().await?;
        for phase in [
            AggregationPhase::Unspecified,
            AggregationPhase::InitialToResult,
        ] {
            for rooted in [false, true] {
                let proto = aggregate_phase_plan(phase as i32, rooted);
                let plan = from_substrait_plan(&ctx.state(), &proto).await?;
                let batches = DataFrame::new(ctx.state(), plan).collect().await?;
                assert_eq!(
                    ScalarValue::try_from_array(batches[0].column(0), 0)?,
                    ScalarValue::Float64(Some(1.5))
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn aggregate_unsupported_phases() -> Result<()> {
        let ctx = aggregate_phase_context().await?;
        for phase in [
            AggregationPhase::InitialToIntermediate,
            AggregationPhase::IntermediateToIntermediate,
            AggregationPhase::IntermediateToResult,
        ] {
            for rooted in [false, true] {
                let proto = aggregate_phase_plan(phase as i32, rooted);
                let err = from_substrait_plan(&ctx.state(), &proto).await.unwrap_err();
                assert!(
                    err.to_string().contains(&format!(
                        "Unsupported aggregation phase: {}",
                        phase.as_str_name()
                    )),
                    "{err}"
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn aggregate_invalid_phase() -> Result<()> {
        let ctx = aggregate_phase_context().await?;
        for phase in [-1, 12345] {
            let proto = aggregate_phase_plan(phase, false);
            let err = from_substrait_plan(&ctx.state(), &proto).await.unwrap_err();
            assert!(
                err.to_string()
                    .contains(&format!("Invalid aggregation phase {phase}")),
                "{err}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn window_aggregation_phases() -> Result<()> {
        let original =
            read_json("tests/testdata/test_plans/select_window_count.substrait.json");
        let ctx = SessionContext::new();
        ctx.register_batch(
            TableReference::bare("DATA"),
            record_batch!(
                ("D", Int32, [1, 2, 3]),
                ("PART", Int32, [1, 1, 1]),
                ("ORD", Int32, [1, 2, 3])
            )?,
        )?;
        for phase in [
            AggregationPhase::Unspecified as i32,
            AggregationPhase::InitialToResult as i32,
            AggregationPhase::InitialToIntermediate as i32,
            AggregationPhase::IntermediateToIntermediate as i32,
            AggregationPhase::IntermediateToResult as i32,
            12345,
        ] {
            let mut proto = original.clone();
            let Some(plan_rel::RelType::Root(root)) =
                proto.relations[0].rel_type.as_mut()
            else {
                panic!("expected root");
            };
            let Some(rel::RelType::Project(project)) =
                root.input.as_mut().unwrap().rel_type.as_mut()
            else {
                panic!("expected projection");
            };
            let Some(expression::RexType::WindowFunction(window)) =
                project.expressions[0].rex_type.as_mut()
            else {
                panic!("expected window function");
            };
            window.phase = phase;
            let proto = Plan::decode(proto.encode_to_vec().as_slice()).unwrap();
            let result = from_substrait_plan(&ctx.state(), &proto).await;
            if matches!(
                AggregationPhase::try_from(phase),
                Ok(AggregationPhase::Unspecified | AggregationPhase::InitialToResult)
            ) {
                let batches = DataFrame::new(ctx.state(), result?).collect().await?;
                datafusion::assert_batches_sorted_eq!(
                    [
                        "+-----------+",
                        "| LEAD_EXPR |",
                        "+-----------+",
                        "| 2         |",
                        "| 3         |",
                        "| 3         |",
                        "+-----------+"
                    ],
                    &batches
                );
            } else {
                let err = result.unwrap_err();
                let expected = match AggregationPhase::try_from(phase) {
                    Ok(phase) => {
                        format!("Unsupported aggregation phase: {}", phase.as_str_name())
                    }
                    Err(_) => format!("Invalid aggregation phase {phase}"),
                };
                assert!(err.to_string().contains(&expected), "{err}");
            }
        }
        Ok(())
    }

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

    #[tokio::test]
    async fn multiple_grouping_sets_follow_substrait_output_order() -> Result<()> {
        let proto_plan = read_json(
            "tests/testdata/test_plans/aggregate_groupings/multiple_groupings.json",
        );
        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto_plan)?;
        let plan = from_substrait_plan(&ctx.state(), &proto_plan).await?;

        assert_snapshot!(
            plan,
            @r"
        Projection: c0, c1, sum(c0) AS summation
          Aggregate: groupBy=[[GROUPING SETS ((c0), (c1), (c0, c1))]], aggr=[[sum(c0)]]
            Values: (Int64(1), Int64(10)), (Int64(1), Int64(20)), (Int64(2), Int64(10))
        "
        );

        let results = DataFrame::new(ctx.state(), plan).collect().await?;
        datafusion::assert_batches_sorted_eq!(
            [
                "+----+----+-----------+",
                "| c0 | c1 | summation |",
                "+----+----+-----------+",
                "|    | 10 | 3         |",
                "|    | 20 | 1         |",
                "| 1  |    | 2         |",
                "| 1  | 10 | 1         |",
                "| 1  | 20 | 1         |",
                "| 2  |    | 2         |",
                "| 2  | 10 | 2         |",
                "+----+----+-----------+",
            ],
            &results
        );

        Ok(())
    }
}
