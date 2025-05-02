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

//! Test SQL macro functionality

#![allow(unknown_lints)]
#![allow(static_mut_refs)]

use datafusion_common::{not_impl_err, DataFusionError, Result, ScalarValue};
use datafusion_sql::parser::Statement;
use std::sync::OnceLock;

mod common;

#[cfg(test)]
mod tests {

    use super::*;

    fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
        let statements = datafusion_sql::parser::DFParser::parse_sql(sql)
            .map_err(|e| DataFusionError::Plan(format!("Parse SQL error: {:?}", e)))?;
        Ok(statements.into_iter().collect())
    }

    use crate::common::{MockContextProvider, MockMacroCatalog, MockSessionState};
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::MacroCatalog;
    use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
    use datafusion_expr::planner::{ExprPlanner, TypePlanner};
    use datafusion_expr::Accumulator;
    use datafusion_expr::AggregateUDFImpl;
    use datafusion_expr::WindowUDFImpl;
    use datafusion_expr::{
        AggregateUDF, PartitionEvaluator, ScalarUDF, Signature, Volatility, WindowUDF,
    };
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
    use sqlparser::ast::DataType as SQLDataType;
    use std::sync::Arc;

    #[derive(Debug)]
    struct TestAggregateImpl {}

    impl AggregateUDFImpl for TestAggregateImpl {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "test_agg"
        }

        fn signature(&self) -> &Signature {
            static mut SIG: Option<Signature> = None;
            if unsafe { SIG.is_none() } {
                unsafe {
                    SIG = Some(Signature::exact(vec![], Volatility::Immutable));
                }
            }
            unsafe { SIG.as_ref().unwrap() }
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int64)
        }

        fn accumulator(
            &self,
            _arg_types: AccumulatorArgs<'_>,
        ) -> Result<Box<dyn Accumulator>> {
            #[derive(Debug)]
            struct DummyAccumulator {}

            impl Accumulator for DummyAccumulator {
                fn state(&mut self) -> Result<Vec<ScalarValue>> {
                    Ok(vec![])
                }

                fn update_batch(
                    &mut self,
                    _values: &[Arc<dyn arrow::array::Array>],
                ) -> Result<()> {
                    Ok(())
                }

                fn merge_batch(
                    &mut self,
                    _states: &[Arc<dyn arrow::array::Array>],
                ) -> Result<()> {
                    Ok(())
                }

                fn evaluate(&mut self) -> Result<ScalarValue> {
                    Ok(ScalarValue::Int64(Some(0)))
                }

                fn size(&self) -> usize {
                    0
                }
            }

            Ok(Box::new(DummyAccumulator {}))
        }

        fn state_fields(&self, _args: StateFieldsArgs<'_>) -> Result<Vec<Field>> {
            Ok(vec![])
        }
    }

    #[derive(Debug)]
    struct TestWindowImpl {}

    impl WindowUDFImpl for TestWindowImpl {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "test_window"
        }

        fn signature(&self) -> &Signature {
            static mut SIG: Option<Signature> = None;
            if unsafe { SIG.is_none() } {
                unsafe {
                    SIG = Some(Signature::exact(vec![], Volatility::Immutable));
                }
            }
            unsafe { SIG.as_ref().unwrap() }
        }

        fn partition_evaluator(
            &self,
            _partition_evaluator_args: datafusion_expr::function::PartitionEvaluatorArgs,
        ) -> Result<Box<dyn PartitionEvaluator>> {
            not_impl_err!("Test implementation")
        }

        fn field(
            &self,
            _field_args: datafusion_expr::function::WindowUDFFieldArgs,
        ) -> Result<Field> {
            Ok(Field::new("test_window", DataType::Int64, false))
        }
    }

    fn create_test_udf() -> Arc<ScalarUDF> {
        #[derive(Debug)]
        struct TestUDFImpl {}

        impl ScalarUDFImpl for TestUDFImpl {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn name(&self) -> &str {
                "test_function"
            }

            fn signature(&self) -> &Signature {
                static SIG: OnceLock<Signature> = OnceLock::new();
                SIG.get_or_init(|| {
                    Signature::uniform(0, vec![DataType::Int32], Volatility::Immutable)
                })
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(DataType::Int32)
            }

            fn invoke_with_args(
                &self,
                _args: ScalarFunctionArgs<'_>,
            ) -> Result<ColumnarValue> {
                not_impl_err!("Test implementation")
            }
        }

        Arc::new(ScalarUDF::new_from_shared_impl(Arc::new(TestUDFImpl {})))
    }

    #[derive(Debug)]
    struct MockTypePlanner {}

    impl TypePlanner for MockTypePlanner {
        fn plan_type(&self, _expr: &SQLDataType) -> Result<Option<DataType>> {
            Ok(Some(DataType::Int32))
        }
    }

    #[derive(Debug)]
    struct MockExprPlanner {}

    impl ExprPlanner for MockExprPlanner {
        // All methods in ExprPlanner have default implementations
    }

    #[test]
    fn test_create_macro() {
        let state = MockSessionState::default()
            .with_scalar_function(create_test_udf())
            .with_type_planner(Arc::new(MockTypePlanner {}))
            .with_expr_planner(Arc::new(MockExprPlanner {}))
            .with_aggregate_function(Arc::new(AggregateUDF::new_from_shared_impl(
                Arc::new(TestAggregateImpl {}),
            )))
            .with_window_function(Arc::new(WindowUDF::new_from_shared_impl(Arc::new(
                TestWindowImpl {},
            ))));

        let ctx_provider = MockContextProvider {
            state,
            macro_catalog: MockMacroCatalog::default(),
        };

        let sql = "CREATE MACRO simple_macro(param1) AS TABLE (SELECT * FROM test_table WHERE id = param1)";
        let stmts = parse_sql(sql).unwrap();

        assert_eq!(stmts.len(), 1);

        assert!(matches!(stmts[0], Statement::CreateMacro { .. }));

        assert!(!ctx_provider.macro_catalog.macro_exists("simple_macro"));
    }

    #[test]
    fn test_macro_with_multiple_parameters() {
        let sql = "CREATE MACRO range_macro(min_val, max_val) AS TABLE (SELECT * FROM test_table WHERE id >= min_val AND id <= max_val)";
        let stmts = parse_sql(sql).unwrap();

        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], Statement::CreateMacro { .. }))
    }

    #[test]
    fn test_columns_function_no_pattern() {
        let sql = "SELECT COLUMNS(test_table) FROM test_table";
        let stmts = parse_sql(sql).unwrap();

        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], Statement::Statement(_)))
    }

    #[test]
    fn test_columns_function_with_pattern() {
        let sql = "SELECT COLUMNS(test_table, 'id%') FROM test_table";
        let stmts = parse_sql(sql).unwrap();

        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], Statement::Statement(_)))
    }

    #[test]
    fn test_macro_composition() {
        let sql = "CREATE MACRO row_filter(min_val) AS TABLE (SELECT * FROM test_table WHERE id >= min_val)";
        let stmts = parse_sql(sql).unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], Statement::CreateMacro { .. }));

        let sql = "CREATE MACRO combined_macro(min_val) AS TABLE (SELECT COLUMNS(row_filter(min_val), 'id%') FROM row_filter(min_val))";
        let stmts = parse_sql(sql).unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], Statement::CreateMacro { .. }))
    }
}
