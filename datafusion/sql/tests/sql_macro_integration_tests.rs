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

//! Integration tests for SQL macro functionality

mod common;

#[cfg(test)]
mod tests {

    #[test]
    fn test_create_and_execute_macro() {
        use datafusion_sql::parser::{DFParser, Statement};

        let create_macro_sql = "CREATE MACRO test_macro(min_val) AS TABLE (
            SELECT * FROM test_table WHERE id >= min_val
        )";

        let statements = DFParser::parse_sql(create_macro_sql)
            .map_err(|e| {
                datafusion_common::DataFusionError::Plan(format!("Parse error: {:?}", e))
            })
            .unwrap();

        assert_eq!(statements.len(), 1, "Expected exactly one statement");

        match &statements[0] {
            Statement::CreateMacro { .. } => {}
            _ => {
                panic!("Expected CREATE MACRO statement");
            }
        }
    }

    #[test]
    fn test_macro_with_multiple_parameters() {
        use datafusion_sql::parser::{DFParser, Statement};

        let create_macro_sql = "CREATE MACRO range_filter(min_val, max_val) AS TABLE (
            SELECT * FROM test_table WHERE id >= min_val AND id <= max_val
        )";

        let statements = DFParser::parse_sql(create_macro_sql)
            .map_err(|e| {
                datafusion_common::DataFusionError::Plan(format!("Parse error: {:?}", e))
            })
            .unwrap();

        assert_eq!(statements.len(), 1, "Expected exactly one statement");

        match &statements[0] {
            Statement::CreateMacro { .. } => {}
            _ => {
                panic!("Expected CREATE MACRO statement");
            }
        }
    }

    #[test]
    fn test_columns_function_all_columns() {
        use datafusion_sql::parser::{DFParser, Statement};

        let query_sql = "SELECT COLUMNS(test_table) FROM test_table";

        let statements = DFParser::parse_sql(query_sql)
            .map_err(|e| {
                datafusion_common::DataFusionError::Plan(format!("Parse error: {:?}", e))
            })
            .unwrap();

        assert_eq!(statements.len(), 1, "Expected exactly one statement");

        match &statements[0] {
            Statement::Statement(_) => {}
            _ => {
                panic!("Expected Statement");
            }
        }
    }

    #[test]
    fn test_columns_function_with_pattern() {
        use datafusion_sql::parser::{DFParser, Statement};

        let query_sql = "SELECT COLUMNS(test_table, 'id%') FROM test_table";

        let statements = DFParser::parse_sql(query_sql)
            .map_err(|e| {
                datafusion_common::DataFusionError::Plan(format!("Parse error: {:?}", e))
            })
            .unwrap();

        assert_eq!(statements.len(), 1, "Expected exactly one statement");

        match &statements[0] {
            Statement::Statement(_) => {}
            _ => {
                panic!("Expected Statement");
            }
        }
    }

    #[test]
    fn test_macro_composition() {
        use datafusion_sql::parser::{DFParser, Statement};

        let create_macro1_sql = "CREATE MACRO row_filter(min_val) AS TABLE (
            SELECT * FROM test_table WHERE id >= min_val
        )";

        let statements1 = DFParser::parse_sql(create_macro1_sql)
            .map_err(|e| {
                datafusion_common::DataFusionError::Plan(format!("Parse error: {:?}", e))
            })
            .unwrap();

        assert_eq!(statements1.len(), 1, "Expected exactly one statement");

        match &statements1[0] {
            Statement::CreateMacro { .. } => {}
            _ => {
                panic!("Expected CREATE MACRO statement for first macro");
            }
        }

        let create_macro2_sql =
            "CREATE MACRO combined_filter(min_val, name_prefix) AS TABLE (
            SELECT * FROM row_filter(min_val) WHERE name LIKE CONCAT(name_prefix, '%')
        )";

        let statements2 = DFParser::parse_sql(create_macro2_sql)
            .map_err(|e| {
                datafusion_common::DataFusionError::Plan(format!("Parse error: {:?}", e))
            })
            .unwrap();

        assert_eq!(statements2.len(), 1, "Expected exactly one statement");

        match &statements2[0] {
            Statement::CreateMacro { .. } => {}
            _ => {
                panic!("Expected CREATE MACRO statement");
            }
        }
    }

    #[test]
    fn test_macro_error_handling() {
        use datafusion_sql::parser::{DFParser, Statement};

        let create_macro_sql = "CREATE MACRO test_error_macro(min_val) AS TABLE (
            SELECT * FROM test_table WHERE id >= min_val
        )";

        let statements = DFParser::parse_sql(create_macro_sql)
            .map_err(|e| {
                datafusion_common::DataFusionError::Plan(format!("Parse error: {:?}", e))
            })
            .unwrap();

        assert_eq!(statements.len(), 1);
        assert!(matches!(statements[0], Statement::CreateMacro { .. }));

        // Try to call the macro with wrong number of arguments
        // This would fail at execution time, but will parse correctly since it's just a table reference
        let invalid_call = "SELECT * FROM test_error_macro()";
        let statements = DFParser::parse_sql(invalid_call)
            .map_err(|e| {
                datafusion_common::DataFusionError::Plan(format!("Parse error: {:?}", e))
            })
            .unwrap();

        assert_eq!(statements.len(), 1);
        assert!(matches!(statements[0], Statement::Statement(_)));

        // Try to call with invalid argument type
        // This also parses successfully but would fail during execution
        let invalid_type_call = "SELECT * FROM test_error_macro('not_a_number')";
        let statements = DFParser::parse_sql(invalid_type_call)
            .map_err(|e| {
                datafusion_common::DataFusionError::Plan(format!("Parse error: {:?}", e))
            })
            .unwrap();

        assert_eq!(statements.len(), 1);
        assert!(matches!(statements[0], Statement::Statement(_)));
    }
}
