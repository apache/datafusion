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

//! [`EliminateCommonPhysicalSubexprs`] to avoid redundant computation of common physical
//! sub-expressions.

use datafusion_common::alias::AliasGenerator;
use datafusion_common::config::ConfigOptions;
use datafusion_common::cse::{CSEController, FoundCommonNodes, CSE};
use datafusion_common::Result;
use datafusion_physical_plan::ExecutionPlan;
use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_expr_common::operator::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, CaseExpr, Column};
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion_physical_plan::projection::ProjectionExec;

const CSE_PREFIX: &str = "__common_physical_expr";

// Optimizer rule to avoid redundant computation of common physical subexpressions
#[derive(Default, Debug)]
pub struct EliminateCommonPhysicalSubexprs {}

impl EliminateCommonPhysicalSubexprs {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for EliminateCommonPhysicalSubexprs {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|plan| {
            let plan_any = plan.as_any();
            if let Some(p) = plan_any.downcast_ref::<ProjectionExec>() {
                match CSE::new(PhysicalExprCSEController::new(
                    config.alias_generator.as_ref(),
                    p.input().schema().fields().len(),
                ))
                .extract_common_nodes(vec![p
                    .expr()
                    .iter()
                    .map(|(e, _)| e)
                    .cloned()
                    .collect()])?
                {
                    FoundCommonNodes::Yes {
                        common_nodes: common_exprs,
                        new_nodes_list: mut new_exprs_list,
                        original_nodes_list: _,
                    } => {
                        let common_exprs = p
                            .input()
                            .schema()
                            .fields()
                            .iter()
                            .enumerate()
                            .map(|(i, field)| {
                                (
                                    Arc::new(Column::new(field.name(), i))
                                        as Arc<dyn PhysicalExpr>,
                                    field.name().to_string(),
                                )
                            })
                            .chain(common_exprs)
                            .collect();
                        let common = Arc::new(ProjectionExec::try_new(
                            common_exprs,
                            Arc::clone(p.input()),
                        )?);

                        let new_exprs = new_exprs_list
                            .pop()
                            .unwrap()
                            .into_iter()
                            .zip(p.expr().iter().map(|(_, alias)| alias.to_string()))
                            .collect();
                        let new_project =
                            Arc::new(ProjectionExec::try_new(new_exprs, common)?)
                                as Arc<dyn ExecutionPlan>;

                        Ok(Transformed::yes(new_project))
                    }
                    FoundCommonNodes::No { .. } => Ok(Transformed::no(plan)),
                }
            } else {
                Ok(Transformed::no(plan))
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "eliminate_common_physical_subexpressions"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

pub struct PhysicalExprCSEController<'a> {
    alias_generator: &'a AliasGenerator,
    base_index: usize,
}

impl<'a> PhysicalExprCSEController<'a> {
    fn new(alias_generator: &'a AliasGenerator, base_index: usize) -> Self {
        Self {
            alias_generator,
            base_index,
        }
    }
}

impl CSEController for PhysicalExprCSEController<'_> {
    type Node = Arc<dyn PhysicalExpr>;

    fn conditional_children(
        node: &Self::Node,
    ) -> Option<(Vec<&Self::Node>, Vec<&Self::Node>)> {
        if let Some(s) = node.as_any().downcast_ref::<ScalarFunctionExpr>() {
            // In case of `ScalarFunction`s all children can be conditionally executed.
            if s.fun().short_circuits() {
                Some((vec![], s.args().iter().collect()))
            } else {
                None
            }
        } else if let Some(b) = node.as_any().downcast_ref::<BinaryExpr>() {
            // In case of `And` and `Or` the first child is surely executed, but we
            // account subexpressions as conditional in the second.
            if *b.op() == Operator::And || *b.op() == Operator::Or {
                Some((vec![b.left()], vec![b.right()]))
            } else {
                None
            }
        } else {
            node.as_any().downcast_ref::<CaseExpr>().map(|c| {
                (
                    // In case of `Case` the optional base expression and the first when
                    // expressions are surely executed, but we account subexpressions as
                    // conditional in the others.
                    c.expr()
                        .into_iter()
                        .chain(c.when_then_expr().iter().take(1).map(|(when, _)| when))
                        .collect(),
                    c.when_then_expr()
                        .iter()
                        .take(1)
                        .map(|(_, then)| then)
                        .chain(
                            c.when_then_expr()
                                .iter()
                                .skip(1)
                                .flat_map(|(when, then)| [when, then]),
                        )
                        .chain(c.else_expr())
                        .collect(),
                )
            })
        }
    }

    fn is_valid(node: &Self::Node) -> bool {
        !node.is_volatile()
    }

    fn is_ignored(&self, node: &Self::Node) -> bool {
        node.children().is_empty()
    }

    fn generate_alias(&self) -> String {
        self.alias_generator.next(CSE_PREFIX)
    }

    fn rewrite(&mut self, _node: &Self::Node, alias: &str, index: usize) -> Self::Node {
        Arc::new(Column::new(alias, self.base_index + index))
    }

    fn rewrite_f_down(&mut self, _node: &Self::Node) {}

    fn rewrite_f_up(&mut self, _node: &Self::Node) {}
}

#[cfg(test)]
mod tests {
    use crate::eliminate_common_physical_subexprs::EliminateCommonPhysicalSubexprs;
    use crate::optimizer::PhysicalOptimizerRule;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::Result;
    use datafusion_expr::{ScalarUDF, ScalarUDFImpl};
    use datafusion_expr_common::columnar_value::ColumnarValue;
    use datafusion_expr_common::operator::Operator;
    use datafusion_expr_common::signature::{Signature, Volatility};
    use datafusion_physical_expr::expressions::{binary, col, lit};
    use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};
    use datafusion_physical_plan::memory::MemorySourceConfig;
    use datafusion_physical_plan::projection::ProjectionExec;
    use datafusion_physical_plan::source::DataSourceExec;
    use datafusion_physical_plan::{get_plan_string, ExecutionPlan};
    use std::any::Any;
    use std::sync::Arc;

    fn mock_data() -> Arc<DataSourceExec> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));

        Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![]], Arc::clone(&schema), None).unwrap(),
        )))
    }

    #[test]
    fn subexpr_in_same_order() -> Result<()> {
        let table_scan = mock_data();

        let a = col("a", &table_scan.schema())?;
        let lit_1 = lit(1);
        let _1_plus_a = binary(lit_1, Operator::Plus, a, &table_scan.schema())?;

        let exprs = vec![
            (Arc::clone(&_1_plus_a), "first".to_string()),
            (_1_plus_a, "second".to_string()),
        ];
        let plan = Arc::new(ProjectionExec::try_new(exprs, mock_data())?);

        let config = ConfigOptions::new();
        let optimizer = EliminateCommonPhysicalSubexprs::new();
        let optimized = optimizer.optimize(plan, &config)?;

        let actual = get_plan_string(&optimized);
        let expected = [
            "ProjectionExec: expr=[__common_physical_expr_1@2 as first, __common_physical_expr_1@2 as second]",
            "  ProjectionExec: expr=[a@0 as a, b@1 as b, 1 + a@0 as __common_physical_expr_1]",
            "    DataSourceExec: partitions=1, partition_sizes=[0]"];
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn subexpr_in_different_order() -> Result<()> {
        let table_scan = mock_data();

        let a = col("a", &table_scan.schema())?;
        let lit_1 = lit(1);
        let _1_plus_a = binary(
            Arc::clone(&lit_1),
            Operator::Plus,
            Arc::clone(&a),
            &table_scan.schema(),
        )?;
        let a_plus_1 = binary(a, Operator::Plus, lit_1, &table_scan.schema())?;

        let exprs = vec![
            (_1_plus_a, "first".to_string()),
            (a_plus_1, "second".to_string()),
        ];
        let plan = Arc::new(ProjectionExec::try_new(exprs, mock_data())?);

        let config = ConfigOptions::new();
        let optimizer = EliminateCommonPhysicalSubexprs::new();
        let optimized = optimizer.optimize(plan, &config)?;

        let actual = get_plan_string(&optimized);
        let expected = [
            "ProjectionExec: expr=[1 + a@0 as first, a@0 + 1 as second]",
            "  DataSourceExec: partitions=1, partition_sizes=[0]",
        ];
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn test_short_circuits() -> Result<()> {
        let table_scan = mock_data();

        let a = col("a", &table_scan.schema())?;
        let b = col("b", &table_scan.schema())?;

        let extracted_short_circuit = binary(
            binary(Arc::clone(&a), Operator::Eq, lit(0), &table_scan.schema())?,
            Operator::Or,
            binary(Arc::clone(&b), Operator::Eq, lit(0), &table_scan.schema())?,
            &table_scan.schema(),
        )?;
        let extracted_short_circuit_leg_1 = binary(
            binary(
                Arc::clone(&a),
                Operator::Plus,
                Arc::clone(&b),
                &table_scan.schema(),
            )?,
            Operator::Eq,
            lit(0),
            &table_scan.schema(),
        )?;
        let not_extracted_short_circuit_leg_2 = binary(
            binary(
                Arc::clone(&a),
                Operator::Minus,
                Arc::clone(&b),
                &table_scan.schema(),
            )?,
            Operator::Eq,
            lit(0),
            &table_scan.schema(),
        )?;
        let extracted_short_circuit_leg_3 = binary(
            binary(a, Operator::Multiply, b, &table_scan.schema())?,
            Operator::Eq,
            lit(0),
            &table_scan.schema(),
        )?;

        let exprs = vec![
            (Arc::clone(&extracted_short_circuit), "c1".to_string()),
            (extracted_short_circuit, "c2".to_string()),
            (
                binary(
                    Arc::clone(&extracted_short_circuit_leg_1),
                    Operator::Or,
                    Arc::clone(&not_extracted_short_circuit_leg_2),
                    &table_scan.schema(),
                )?,
                "c3".to_string(),
            ),
            (
                binary(
                    extracted_short_circuit_leg_1,
                    Operator::And,
                    Arc::clone(&not_extracted_short_circuit_leg_2),
                    &table_scan.schema(),
                )?,
                "c4".to_string(),
            ),
            (
                binary(
                    Arc::clone(&extracted_short_circuit_leg_3),
                    Operator::Or,
                    extracted_short_circuit_leg_3,
                    &table_scan.schema(),
                )?,
                "c5".to_string(),
            ),
        ];
        let plan = Arc::new(ProjectionExec::try_new(exprs, mock_data())?);

        let config = ConfigOptions::new();
        let optimizer = EliminateCommonPhysicalSubexprs::new();
        let optimized = optimizer.optimize(plan, &config)?;

        let actual = get_plan_string(&optimized);
        let expected = [
            "ProjectionExec: expr=[__common_physical_expr_1@2 as c1, __common_physical_expr_1@2 as c2, __common_physical_expr_2@3 OR a@0 - b@1 = 0 as c3, __common_physical_expr_2@3 AND a@0 - b@1 = 0 as c4, __common_physical_expr_3@4 OR __common_physical_expr_3@4 as c5]",
            "  ProjectionExec: expr=[a@0 as a, b@1 as b, a@0 = 0 OR b@1 = 0 as __common_physical_expr_1, a@0 + b@1 = 0 as __common_physical_expr_2, a@0 * b@1 = 0 as __common_physical_expr_3]",
            "    DataSourceExec: partitions=1, partition_sizes=[0]"];
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn test_volatile() -> Result<()> {
        let table_scan = mock_data();

        let a = col("a", &table_scan.schema())?;
        let b = col("b", &table_scan.schema())?;
        let extracted_child = binary(a, Operator::Plus, b, &table_scan.schema())?;
        let rand = rand_expr();
        let not_extracted_volatile =
            binary(extracted_child, Operator::Plus, rand, &table_scan.schema())?;

        let exprs = vec![
            (Arc::clone(&not_extracted_volatile), "c1".to_string()),
            (not_extracted_volatile, "c2".to_string()),
        ];
        let plan = Arc::new(ProjectionExec::try_new(exprs, mock_data())?);

        let config = ConfigOptions::new();
        let optimizer = EliminateCommonPhysicalSubexprs::new();
        let optimized = optimizer.optimize(plan, &config)?;

        let actual = get_plan_string(&optimized);
        let expected = [
            "ProjectionExec: expr=[__common_physical_expr_1@2 + random() as c1, __common_physical_expr_1@2 + random() as c2]",
            "  ProjectionExec: expr=[a@0 as a, b@1 as b, a@0 + b@1 as __common_physical_expr_1]",
            "    DataSourceExec: partitions=1, partition_sizes=[0]"];
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn test_volatile_short_circuits() -> Result<()> {
        let table_scan = mock_data();

        let a = col("a", &table_scan.schema())?;
        let b = col("b", &table_scan.schema())?;
        let rand = rand_expr();
        let rand_eq_0 = binary(rand, Operator::Eq, lit(0), &table_scan.schema())?;

        let extracted_short_circuit_leg_1 =
            binary(a, Operator::Eq, lit(0), &table_scan.schema())?;
        let not_extracted_volatile_short_circuit_1 = binary(
            extracted_short_circuit_leg_1,
            Operator::Or,
            Arc::clone(&rand_eq_0),
            &table_scan.schema(),
        )?;

        let not_extracted_short_circuit_leg_2 =
            binary(b, Operator::Eq, lit(0), &table_scan.schema())?;
        let not_extracted_volatile_short_circuit_2 = binary(
            rand_eq_0,
            Operator::Or,
            not_extracted_short_circuit_leg_2,
            &table_scan.schema(),
        )?;

        let exprs = vec![
            (
                Arc::clone(&not_extracted_volatile_short_circuit_1),
                "c1".to_string(),
            ),
            (not_extracted_volatile_short_circuit_1, "c2".to_string()),
            (
                Arc::clone(&not_extracted_volatile_short_circuit_2),
                "c3".to_string(),
            ),
            (not_extracted_volatile_short_circuit_2, "c4".to_string()),
        ];
        let plan = Arc::new(ProjectionExec::try_new(exprs, mock_data())?);

        let config = ConfigOptions::new();
        let optimizer = EliminateCommonPhysicalSubexprs::new();
        let optimized = optimizer.optimize(plan, &config)?;

        let actual = get_plan_string(&optimized);
        let expected = [
            "ProjectionExec: expr=[__common_physical_expr_1@2 OR random() = 0 as c1, __common_physical_expr_1@2 OR random() = 0 as c2, random() = 0 OR b@1 = 0 as c3, random() = 0 OR b@1 = 0 as c4]",
            "  ProjectionExec: expr=[a@0 as a, b@1 as b, a@0 = 0 as __common_physical_expr_1]",
            "    DataSourceExec: partitions=1, partition_sizes=[0]"];
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn test_non_top_level_common_expression() -> Result<()> {
        let table_scan = mock_data();

        let a = col("a", &table_scan.schema())?;
        let b = col("b", &table_scan.schema())?;
        let common_expr = binary(a, Operator::Plus, b, &table_scan.schema())?;

        let exprs = vec![
            (Arc::clone(&common_expr), "c1".to_string()),
            (common_expr, "c2".to_string()),
        ];
        let plan = Arc::new(ProjectionExec::try_new(exprs, mock_data())?);

        let c1 = col("c1", &plan.schema())?;
        let c2 = col("c2", &plan.schema())?;

        let exprs = vec![(c1, "c1".to_string()), (c2, "c2".to_string())];
        let plan = Arc::new(ProjectionExec::try_new(exprs, plan)?);

        let config = ConfigOptions::new();
        let optimizer = EliminateCommonPhysicalSubexprs::new();
        let optimized = optimizer.optimize(plan, &config)?;

        let actual = get_plan_string(&optimized);
        let expected = [
            "ProjectionExec: expr=[c1@0 as c1, c2@1 as c2]",
            "  ProjectionExec: expr=[__common_physical_expr_1@2 as c1, __common_physical_expr_1@2 as c2]",
            "    ProjectionExec: expr=[a@0 as a, b@1 as b, a@0 + b@1 as __common_physical_expr_1]",
            "      DataSourceExec: partitions=1, partition_sizes=[0]"];
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn test_nested_common_expression() -> Result<()> {
        let table_scan = mock_data();

        let a = col("a", &table_scan.schema())?;
        let b = col("b", &table_scan.schema())?;
        let nested_common_expr = binary(a, Operator::Plus, b, &table_scan.schema())?;
        let common_expr = binary(
            Arc::clone(&nested_common_expr),
            Operator::Multiply,
            nested_common_expr,
            &table_scan.schema(),
        )?;

        let exprs = vec![
            (Arc::clone(&common_expr), "c1".to_string()),
            (common_expr, "c2".to_string()),
        ];
        let plan = Arc::new(ProjectionExec::try_new(exprs, mock_data())?);

        let config = ConfigOptions::new();
        let optimizer = EliminateCommonPhysicalSubexprs::new();
        let optimized = optimizer.optimize(plan, &config)?;

        let actual = get_plan_string(&optimized);
        let expected = [
            "ProjectionExec: expr=[__common_physical_expr_1@2 as c1, __common_physical_expr_1@2 as c2]",
            "  ProjectionExec: expr=[a@0 as a, b@1 as b, __common_physical_expr_2@2 * __common_physical_expr_2@2 as __common_physical_expr_1]",
            "    ProjectionExec: expr=[a@0 as a, b@1 as b, a@0 + b@1 as __common_physical_expr_2]",
            "      DataSourceExec: partitions=1, partition_sizes=[0]"];
        assert_eq!(actual, expected);

        Ok(())
    }

    fn rand_expr() -> Arc<dyn PhysicalExpr> {
        let r = RandomStub::new();
        let n = r.name().to_string();
        let t = r.return_type(&[]).unwrap();
        Arc::new(ScalarFunctionExpr::new(
            &n,
            Arc::new(ScalarUDF::new_from_impl(r)),
            vec![],
            t,
        ))
    }

    #[derive(Debug)]
    struct RandomStub {
        signature: Signature,
    }

    impl RandomStub {
        fn new() -> Self {
            Self {
                signature: Signature::exact(vec![], Volatility::Volatile),
            }
        }
    }
    impl ScalarUDFImpl for RandomStub {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            "random"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Float64)
        }

        fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
            unimplemented!()
        }
    }
}
