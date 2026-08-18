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

use datafusion_optimizer::analyzer::Analyzer;
use datafusion_optimizer::optimizer::Optimizer;
use datafusion_physical_optimizer::optimizer::PhysicalOptimizer;

const OPTIMIZER_RULE_REFERENCE: &str = include_str!("optimizer_rule_reference.md");

fn documented_rules(section_heading: &str) -> Vec<String> {
    let mut in_section = false;
    let mut names = vec![];

    for line in OPTIMIZER_RULE_REFERENCE.lines() {
        if line == section_heading {
            in_section = true;
            continue;
        }

        if in_section && line.starts_with("### ") {
            break;
        }

        if !in_section || !line.starts_with('|') || line.contains("---") {
            continue;
        }

        let columns: Vec<_> = line.split('|').map(str::trim).collect();

        if columns.len() < 4 || columns[1] == "order" {
            continue;
        }

        names.push(columns[2].trim_matches('`').to_string());
    }

    names
}

#[test]
fn analyzer_rules_match_documented_order() {
    let rules: Vec<_> = Analyzer::new()
        .rules
        .iter()
        .map(|rule| rule.name().to_string())
        .collect();

    assert_eq!(documented_rules("### Analyzer Rules"), rules);
}

#[test]
fn logical_rules_match_documented_order() {
    let rules: Vec<_> = Optimizer::new()
        .rules
        .iter()
        .map(|rule| rule.name().to_string())
        .collect();

    assert_eq!(documented_rules("### Logical Optimizer Rules"), rules);
}

#[test]
fn physical_rules_match_documented_order() {
    let rules: Vec<_> = PhysicalOptimizer::new()
        .rules
        .iter()
        .map(|rule| rule.name().to_string())
        .collect();

    assert_eq!(documented_rules("### Physical Optimizer Rules"), rules);
}
