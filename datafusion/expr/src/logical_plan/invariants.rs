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

use datafusion_common::{
    tree_node::TreeNodeRecursion, DFSchemaRef, DataFusionError, Result,
};

use super::LogicalPlan;

/// Returns an error if plan, and subplans, do not have unique fields.
///
/// This invariant is subject to change.
/// refer: <https://github.com/apache/datafusion/issues/13525#issuecomment-2494046463>
pub fn assert_unique_field_names(plan: &LogicalPlan) -> Result<()> {
    plan.schema().check_names()?;

    plan.apply_with_subqueries(|plan: &LogicalPlan| {
        plan.schema().check_names()?;
        Ok(TreeNodeRecursion::Continue)
    })
    .map(|_| ())
}

/// Returns an error if the plan does not have the expected schema.
/// Ignores metadata and nullability.
pub fn assert_expected_schema(
    rule_name: &str,
    schema: &DFSchemaRef,
    plan: &LogicalPlan,
) -> Result<()> {
    let equivalent = plan.schema().equivalent_names_and_types(schema);

    if !equivalent {
        let e = DataFusionError::Internal(format!(
            "Failed due to a difference in schemas, original schema: {:?}, new schema: {:?}",
            schema,
            plan.schema()
        ));
        Err(DataFusionError::Context(
            String::from(rule_name),
            Box::new(e),
        ))
    } else {
        Ok(())
    }
}
