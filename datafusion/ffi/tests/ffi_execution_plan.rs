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
    use datafusion_ffi::execution_plan::FFI_ExecutionPlan;
    use datafusion_ffi::execution_plan::ForeignExecutionPlan;
    use datafusion_ffi::execution_plan::{ExecutionPlanPrivateData, tests::EmptyExec};
    use datafusion_ffi::tests::utils::get_module;
    use datafusion_physical_plan::ExecutionPlan;
    use std::sync::Arc;

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

        let child_plan =
            module
                .create_empty_exec()
                .ok_or(DataFusionError::NotImplemented(
                    "External module failed to implement create_empty_exec".to_string(),
                ))?();
        let child_plan: Arc<dyn ExecutionPlan> = (&child_plan)
            .try_into()
            .expect("should be able create plan");
        assert!(child_plan.as_any().is::<ForeignExecutionPlan>());

        let grandchild_plan = generate_local_plan();

        let child_plan = child_plan.with_new_children(vec![grandchild_plan])?;

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

        let parent_plan = generate_local_plan().with_new_children(vec![child_plan])?;

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
