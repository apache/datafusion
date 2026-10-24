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
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::{Result, ScalarValue, assert_batches_sorted_eq};
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::extensions::Extensions;
    use datafusion_substrait::logical_plan::consumer::{
        DefaultSubstraitConsumer, SubstraitConsumer, from_substrait_plan,
        from_substrait_plan_with_consumer,
    };
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};
    use substrait::proto::{
        CrossRel, Plan, PlanRel, ReferenceRel, Rel, RelRoot, plan_rel, rel,
    };

    #[tokio::test]
    async fn repeated_references_are_inlined() -> Result<()> {
        let ctx = SessionContext::new();
        let calls = Arc::new(AtomicI64::new(0));
        let counter = Arc::clone(&calls);
        ctx.register_udf(create_udf(
            "next_value",
            vec![],
            DataType::Int64,
            Volatility::Volatile,
            Arc::new(move |_| {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(
                    counter.fetch_add(1, Ordering::SeqCst),
                ))))
            }),
        ));
        let plan = shared_body(&ctx, "SELECT next_value()").await?;
        let logical = from_substrait_plan(&ctx.state(), &plan).await?;
        let batches = ctx.execute_logical_plan(logical).await?.collect().await?;
        // Each branch must execute the volatile body independently.
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(batches[0].num_rows(), 1);
        assert_ne!(batches[0].column(0), batches[0].column(1));
        Ok(())
    }

    #[tokio::test]
    async fn forward_and_nested_references() -> Result<()> {
        let ctx = SessionContext::new();
        let mut plan = shared_body(&ctx, "SELECT 42").await?;
        let body = plan.relations[0].clone();
        // The output root is in the middle; the shared tree references a later tree.
        plan.relations[0] = tree(reference(2));
        plan.relations.push(body);
        let extensions = Extensions::try_from(&plan.extensions)?;
        let state = ctx.state();
        let consumer =
            DefaultSubstraitConsumer::new_with_plan(&extensions, &state, &plan);
        let logical = from_substrait_plan_with_consumer(&consumer, &plan).await?;
        let batches = ctx.execute_logical_plan(logical).await?.collect().await?;
        assert_batches_sorted_eq!(
            [
                "+----+----+",
                "| a  | b  |",
                "+----+----+",
                "| 42 | 42 |",
                "+----+----+"
            ],
            &batches
        );
        Ok(())
    }

    #[tokio::test]
    async fn invalid_references() -> Result<()> {
        let ctx = SessionContext::new();
        for (ordinal, message) in [
            (-1, "Invalid subtree ordinal -1"),
            (99, "Invalid subtree ordinal 99"),
            (0, "Cyclic Substrait reference at subtree 0"),
            (1, "Cyclic Substrait reference"),
        ] {
            let mut plan = shared_body(&ctx, "SELECT 42").await?;
            plan.relations[0] = tree(reference(ordinal));
            let error = from_substrait_plan(&ctx.state(), &plan).await.unwrap_err();
            assert!(error.to_string().contains(message), "{error}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn rejects_missing_or_multiple_output_roots() -> Result<()> {
        let ctx = SessionContext::new();
        let plan = shared_body(&ctx, "SELECT 42").await?;
        for relations in [
            vec![],
            vec![plan.relations[0].clone(); 2],
            vec![plan.relations[1].clone(); 2],
        ] {
            let malformed = Plan {
                relations,
                ..plan.clone()
            };
            let error = from_substrait_plan(&ctx.state(), &malformed)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("output root"), "{error}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn failed_reference_does_not_leave_a_cycle_marker() -> Result<()> {
        let ctx = SessionContext::new();
        let plan = Plan {
            relations: vec![tree(reference(1)), PlanRel { rel_type: None }],
            ..Default::default()
        };
        let extensions = Extensions::default();
        let state = ctx.state();
        let consumer =
            DefaultSubstraitConsumer::new_with_plan(&extensions, &state, &plan);
        for _ in 0..2 {
            let error = consumer.consume_rel(&reference(0)).await.unwrap_err();
            assert!(
                error.to_string().contains("Referenced relation 1 is empty"),
                "{error}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn reference_to_root_ignores_output_names() -> Result<()> {
        let ctx = SessionContext::new();
        let logical = ctx.sql("SELECT 42").await?.into_unoptimized_plan();
        let mut plan = *to_substrait_plan(&logical, &ctx.state())?;
        let Some(plan_rel::RelType::Root(root)) = &mut plan.relations[0].rel_type else {
            unreachable!();
        };
        // Deliberately wrong arity: ReferenceRel must not apply these names.
        root.names = vec!["ignored".into(), "also_ignored".into()];
        let extensions = Extensions::try_from(&plan.extensions)?;
        let state = ctx.state();
        let consumer =
            DefaultSubstraitConsumer::new_with_plan(&extensions, &state, &plan);
        let logical = consumer.consume_rel(&reference(0)).await?;
        let batches = ctx.execute_logical_plan(logical).await?.collect().await?;
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        Ok(())
    }

    async fn shared_body(ctx: &SessionContext, sql: &str) -> Result<Plan> {
        let logical = ctx.sql(sql).await?.into_unoptimized_plan();
        let mut plan = *to_substrait_plan(&logical, &ctx.state())?;
        let Some(plan_rel::RelType::Root(root)) = plan.relations.remove(0).rel_type
        else {
            unreachable!("producer emits a root");
        };
        plan.relations = vec![
            tree(root.input.unwrap()),
            PlanRel {
                rel_type: Some(plan_rel::RelType::Root(RelRoot {
                    input: Some(Rel {
                        rel_type: Some(rel::RelType::Cross(Box::new(CrossRel {
                            left: Some(Box::new(reference(0))),
                            right: Some(Box::new(reference(0))),
                            ..Default::default()
                        }))),
                    }),
                    names: vec!["a".into(), "b".into()],
                })),
            },
        ];
        Ok(plan)
    }

    fn tree(rel: Rel) -> PlanRel {
        PlanRel {
            rel_type: Some(plan_rel::RelType::Rel(rel)),
        }
    }

    fn reference(ordinal: i32) -> Rel {
        Rel {
            rel_type: Some(rel::RelType::Reference(ReferenceRel {
                subtree_ordinal: ordinal,
            })),
        }
    }
}
