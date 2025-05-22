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

use datafusion_catalog::MemoryMacroCatalog;
use datafusion_common::{MacroCatalog, MacroDefinition};
use std::sync::Arc;

#[test]
fn test_memory_macro_catalog() {
    let catalog = MemoryMacroCatalog::new();

    let macro_def = MacroDefinition {
        name: "test_macro".to_string(),
        parameters: vec!["param1".to_string(), "param2".to_string()],
        body: "SELECT * FROM table WHERE id > param1 AND name LIKE param2".to_string(),
    };

    catalog
        .register_macro("test_macro", Arc::new(macro_def), false)
        .unwrap();

    assert!(catalog.macro_exists("test_macro"));

    let retrieved_macro = catalog.get_macro("test_macro").unwrap();
    assert_eq!(retrieved_macro.name, "test_macro");
    assert_eq!(retrieved_macro.parameters.len(), 2);
    assert_eq!(retrieved_macro.parameters[0], "param1");
    assert_eq!(retrieved_macro.parameters[1], "param2");

    let replacement_macro = MacroDefinition {
        name: "test_macro".to_string(),
        parameters: vec!["single_param".to_string()],
        body: "SELECT id FROM table WHERE id = single_param".to_string(),
    };

    catalog
        .register_macro("test_macro", Arc::new(replacement_macro), true)
        .unwrap();

    let updated_macro = catalog.get_macro("test_macro").unwrap();
    assert_eq!(updated_macro.parameters.len(), 1);
    assert_eq!(updated_macro.parameters[0], "single_param");

    let another_macro = MacroDefinition {
        name: "test_macro".to_string(),
        parameters: vec!["x".to_string(), "y".to_string()],
        body: "SELECT x, y FROM table".to_string(),
    };

    let result = catalog.register_macro("test_macro", Arc::new(another_macro), false);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("already exists"));

    let another_macro = MacroDefinition {
        name: "another_macro".to_string(),
        parameters: vec!["x".to_string(), "y".to_string()],
        body: "SELECT x, y FROM table".to_string(),
    };

    catalog
        .register_macro("another_macro", Arc::new(another_macro), false)
        .unwrap();

    assert!(catalog.macro_exists("test_macro"));
    assert!(catalog.macro_exists("another_macro"));
}

#[test]
fn test_macro_catalog_case_sensitivity() {
    let catalog = MemoryMacroCatalog::new();

    let macro_def = MacroDefinition {
        name: "MixedCaseMacro".to_string(),
        parameters: vec!["Param".to_string()],
        body: "SELECT * FROM table WHERE id = Param".to_string(),
    };

    catalog
        .register_macro("MixedCaseMacro", Arc::new(macro_def), false)
        .unwrap();

    assert!(catalog.macro_exists("MixedCaseMacro"));
    assert!(!catalog.macro_exists("mixedcasemacro"));
    assert!(!catalog.macro_exists("MIXEDCASEMACRO"));

    let result = catalog.get_macro("MixedCaseMacro");
    assert!(result.is_ok());

    let result = catalog.get_macro("mixedcasemacro");
    assert!(result.is_err());
}
