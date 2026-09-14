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

#[cfg(feature = "integration-tests")]
mod tests {
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow_schema::DataType;
    use datafusion_common::DataFusionError;
    use datafusion_common::tree_node::TreeNodeRecursion;
    use datafusion_ffi::execution_plan::{
        ExecutionPlanPrivateData, FFI_ExecutionPlan, ForeignExecutionPlan,
        tests::EmptyExec,
    };
    use datafusion_ffi::tests::utils::{get_byte_metrics_exec, get_module};
    use datafusion_physical_expr_common::metrics::{MetricCategory, MetricValue};
    use datafusion_physical_plan::execution_plan::InvariantLevel;
    use datafusion_physical_plan::{
        ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions,
    };
    use std::sync::Arc;

    #[test]
    #[expect(deprecated)]
    fn test_ffi_execution_plan_partition_statistics_cross_library()
    -> Result<(), DataFusionError> {
        let module = get_module()?;

        // Producer: plan with no explicit statistics → expects Statistics::new_unknown.
        let bare = (module.create_empty_exec)();
        let bare: Arc<dyn ExecutionPlan> = (&bare).try_into()?;
        assert!(bare.is::<ForeignExecutionPlan>());
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));
        let bare_stats = bare.partition_statistics(None)?;
        assert_eq!(
            bare_stats.as_ref(),
            &datafusion_common::Statistics::new_unknown(&schema),
        );

        // Producer: plan with known statistics — round-trip through the cdylib boundary.
        let expected = datafusion_ffi::tests::make_test_statistics();
        let with_stats = (module.create_exec_with_statistics)();
        let with_stats: Arc<dyn ExecutionPlan> = (&with_stats).try_into()?;
        assert!(with_stats.is::<ForeignExecutionPlan>());

        // Both None (all-partition aggregate) and Some(idx) must return the
        // same statistics because EmptyExec ignores the partition argument.
        let observed_all = with_stats.partition_statistics(None)?;
        assert_eq!(observed_all.as_ref(), &expected);

        let observed_part = with_stats.partition_statistics(Some(0))?;
        assert_eq!(observed_part.as_ref(), &expected);

        Ok(())
    }

    #[test]
    fn test_ffi_execution_plan_byte_metrics_cross_library() -> Result<(), DataFusionError>
    {
        let plan = get_byte_metrics_exec()?;
        let plan: Arc<dyn ExecutionPlan> = (&plan).try_into()?;
        assert!(plan.is::<ForeignExecutionPlan>());

        // metrics() crosses the FFI boundary for real here: `plan` is a
        // ForeignExecutionPlan backed by a separately loaded copy of this
        // same cdylib, so this call marshals a MetricsSet containing a
        // Bytes-category MetricValue::Count/Gauge through FFI_MetricsSet
        // across that boundary - not just the in-process From conversions
        // covered by physical_expr::metrics's roundtrip tests.
        let metrics = plan.metrics().expect("plan should report metrics");

        // Assert the transported Bytes category, the generic variant/name/
        // value, and (below) the byte-formatted display output - MetricValue
        // is a public, already-released exhaustive enum, so this crosses the
        // FFI boundary as a generic Count/Gauge tagged MetricCategory::Bytes
        // rather than as a dedicated variant.
        let mut found_bytes_count = false;
        let mut found_bytes_gauge = false;
        for metric in metrics.iter() {
            let is_bytes = metric.metric_category() == Some(MetricCategory::Bytes);
            match metric.value() {
                MetricValue::Count { name, count }
                    if name.as_ref() == "bytes_scanned" =>
                {
                    assert!(is_bytes, "bytes_scanned should be tagged Bytes category");
                    assert_eq!(count.value(), 1536);
                    found_bytes_count = true;
                }
                MetricValue::Gauge { name, gauge }
                    if name.as_ref() == "stream_memory_usage" =>
                {
                    assert!(
                        is_bytes,
                        "stream_memory_usage should be tagged Bytes category"
                    );
                    assert_eq!(gauge.value(), 2048);
                    found_bytes_gauge = true;
                }
                _ => {}
            }
        }
        assert!(
            found_bytes_count,
            "expected a Bytes-category bytes_scanned Count metric, got: {metrics:?}"
        );
        assert!(
            found_bytes_gauge,
            "expected a Bytes-category stream_memory_usage Gauge metric, got: {metrics:?}"
        );

        // Also confirm the byte-unit Display formatting still applies
        // post-round-trip.
        let rendered: Vec<String> = metrics.iter().map(|m| m.to_string()).collect();
        assert!(
            rendered
                .iter()
                .any(|s| s == "bytes_scanned{partition=0}=1536.0 B"),
            "bytes_scanned should survive the FFI round trip byte-formatted, got: {rendered:?}"
        );
        assert!(
            rendered
                .iter()
                .any(|s| s == "stream_memory_usage{partition=0}=2.0 KB"),
            "stream_memory_usage should survive the FFI round trip byte-formatted, got: {rendered:?}"
        );

        Ok(())
    }

    #[test]
    fn test_ffi_execution_plan_expressions_cross_library() -> Result<(), DataFusionError>
    {
        let module = get_module()?;
        let plan = (module.create_exec_with_expressions)();
        let plan: Arc<dyn ExecutionPlan> = (&plan).try_into()?;
        assert!(plan.is::<ForeignExecutionPlan>());

        let mut retained = None;
        plan.apply_expressions(&mut |expr| {
            retained = Some(Arc::clone(expr));
            Ok(TreeNodeRecursion::Continue)
        })?;
        drop(plan);

        assert!(
            retained
                .as_ref()
                .and_then(|expr| expr.expression_id())
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn test_ffi_execution_plan_dynamic_expressions_cross_library()
    -> Result<(), DataFusionError> {
        let module = get_module()?;
        let plan = (module.create_exec_with_dynamic_expressions)();
        let plan: Arc<dyn ExecutionPlan> = (&plan).try_into()?;
        assert!(plan.is::<ForeignExecutionPlan>());
        plan.check_invariants(InvariantLevel::Always)?;

        let produced = plan.dynamic_expressions_produced();
        assert_eq!(produced.len(), 1);
        assert!(produced[0].expression_id().is_some());
        drop(plan);
        assert!(produced[0].expression_id().is_some());
        Ok(())
    }

    #[test]
    fn test_ffi_execution_plan_new_sets_runtimes_on_children()
    -> Result<(), DataFusionError> {
        // We want to test the case where we have two libraries.
        // Library A will have a foreign plan from Library B, called child_plan.
        // Library A will add a plan called grandchild_plan under child_plan
        // Library A will create a plan called parent_plan, that has child_plan
        // under it. So we should have:
        // parent_plan (local) -> child_plan (foreign) -> grandchild_plan (local)
        // Then we want to turn parent_plan into a FFI plan.
        // Verify that grandchild_plan also gets the same runtime as parent_plan.

        let module = get_module()?;

        fn generate_local_plan() -> Arc<dyn ExecutionPlan> {
            let schema =
                Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

            Arc::new(EmptyExec::new(schema))
        }

        let child_plan = (module.create_empty_exec)();
        let child_plan: Arc<dyn ExecutionPlan> = (&child_plan)
            .try_into()
            .expect("should be able create plan");
        assert!(child_plan.is::<ForeignExecutionPlan>());

        let grandchild_plan = generate_local_plan();

        let child_plan = child_plan.replace_children(
            vec![grandchild_plan],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;

        unsafe {
            // Originally the runtime is not set. We go through the unsafe casting
            // of data here because the `inner()` function is private and this is
            // only an integration test so we do not want to expose it.
            let ffi_child = FFI_ExecutionPlan::new(Arc::clone(&child_plan), None);
            let ffi_grandchild =
                (ffi_child.children)(&ffi_child).into_iter().next().unwrap();

            let grandchild_private_data =
                ffi_grandchild.private_data as *const ExecutionPlanPrivateData;
            assert!((*grandchild_private_data).runtime.is_none());
        }

        let parent_plan = generate_local_plan().replace_children(
            vec![child_plan],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;

        // Adding the grandchild beneath this FFI plan should get the runtime passed down.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let ffi_parent =
            FFI_ExecutionPlan::new(parent_plan, Some(runtime.handle().clone()));

        unsafe {
            let ffi_child = (ffi_parent.children)(&ffi_parent)
                .into_iter()
                .next()
                .unwrap();
            let ffi_grandchild =
                (ffi_child.children)(&ffi_child).into_iter().next().unwrap();
            assert_eq!(
                (ffi_grandchild.library_marker_id)(),
                (ffi_parent.library_marker_id)()
            );

            let grandchild_private_data =
                ffi_grandchild.private_data as *const ExecutionPlanPrivateData;
            assert!((*grandchild_private_data).runtime.is_some());
        }

        Ok(())
    }
}
