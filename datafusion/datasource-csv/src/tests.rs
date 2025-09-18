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
    use crate::file_format::build_schema_helper;
    use arrow::datatypes::DataType;
    use std::collections::HashSet;

    #[test]
    fn test_build_schema_helper_different_column_counts() {
        // Test the core schema building logic with different column counts
        let mut column_names = vec!["col1".to_string(), "col2".to_string(), "col3".to_string()];
        
        // Simulate adding two more columns from another file
        column_names.push("col4".to_string());
        column_names.push("col5".to_string());
        
        let column_type_possibilities = vec![
            {
                let mut set = HashSet::new();
                set.insert(DataType::Int64);
                set
            },
            {
                let mut set = HashSet::new();
                set.insert(DataType::Utf8);
                set
            },
            {
                let mut set = HashSet::new();
                set.insert(DataType::Float64);
                set
            },
            {
                let mut set = HashSet::new();
                set.insert(DataType::Utf8); // col4
                set
            },
            {
                let mut set = HashSet::new();
                set.insert(DataType::Utf8); // col5
                set
            },
        ];
        
        let schema = build_schema_helper(column_names, &column_type_possibilities);
        
        // Verify schema has 5 columns
        assert_eq!(schema.fields().len(), 5);
        assert_eq!(schema.field(0).name(), "col1");
        assert_eq!(schema.field(1).name(), "col2");
        assert_eq!(schema.field(2).name(), "col3");
        assert_eq!(schema.field(3).name(), "col4");
        assert_eq!(schema.field(4).name(), "col5");
        
        // All fields should be nullable
        for field in schema.fields() {
            assert!(field.is_nullable(), "Field {} should be nullable", field.name());
        }
    }
    
    #[test]
    fn test_build_schema_helper_type_merging() {
        // Test type merging logic
        let column_names = vec!["col1".to_string(), "col2".to_string()];
        
        let column_type_possibilities = vec![
            {
                let mut set = HashSet::new();
                set.insert(DataType::Int64);
                set.insert(DataType::Float64); // Should resolve to Float64
                set
            },
            {
                let mut set = HashSet::new();
                set.insert(DataType::Utf8); // Should remain Utf8
                set
            },
        ];
        
        let schema = build_schema_helper(column_names, &column_type_possibilities);
        
        // col1 should be Float64 due to Int64 + Float64 = Float64
        assert_eq!(*schema.field(0).data_type(), DataType::Float64);
        
        // col2 should remain Utf8
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
    }
    
    #[test]
    fn test_build_schema_helper_conflicting_types() {
        // Test when we have incompatible types - should default to Utf8
        let column_names = vec!["col1".to_string()];
        
        let column_type_possibilities = vec![
            {
                let mut set = HashSet::new();
                set.insert(DataType::Boolean);
                set.insert(DataType::Int64);
                set.insert(DataType::Utf8); // Should resolve to Utf8 due to conflicts
                set
            },
        ];
        
        let schema = build_schema_helper(column_names, &column_type_possibilities);
        
        // Should default to Utf8 for conflicting types
        assert_eq!(*schema.field(0).data_type(), DataType::Utf8);
    }
}