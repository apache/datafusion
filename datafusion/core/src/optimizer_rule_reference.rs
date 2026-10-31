// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_optimizer::analyzer::Analyzer;
use datafusion_optimizer::optimizer::Optimizer;
use datafusion_physical_optimizer::analyzer::PhysicalAnalyzer;
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
fn physical_analyzer_rules_match_documented_order() {
    let rules: Vec<_> = PhysicalAnalyzer::new()
        .rules
        .iter()
        .map(|rule| rule.name().to_string())
        .collect();

    assert_eq!(documented_rules("### Physical Analyzer Rules"), rules);
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
