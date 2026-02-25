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

extern crate cargo;

use cargo::CargoResult;
/// Check for circular dependencies between DataFusion crates
use std::collections::{HashMap, HashSet};
use std::env;
use std::path::Path;

use cargo::util::context::GlobalContext;

/// Verifies that there are no circular dependencies between DataFusion crates
/// (which prevents publishing on crates.io) by parsing the Cargo.toml files and
/// checking the dependency graph.
///
/// See https://github.com/apache/datafusion/issues/9278 for more details
fn main() -> CargoResult<()> {
    let gctx = GlobalContext::default()?;
    // This is the path for the depcheck binary
    let path = env::var("CARGO_MANIFEST_DIR").unwrap();
    let root_cargo_toml = Path::new(&path)
        // dev directory
        .parent()
        .expect("Can not find dev directory")
        // project root directory
        .parent()
        .expect("Can not find project root directory")
        .join("Cargo.toml");

    println!(
        "Checking for circular dependencies in {}",
        root_cargo_toml.display()
    );
    let workspace = cargo::core::Workspace::new(&root_cargo_toml, &gctx)?;
    let (_, resolve) = cargo::ops::resolve_ws(&workspace, false)?;

    let mut package_deps = HashMap::new();
    for package_id in resolve
        .iter()
        .filter(|id| id.name().starts_with("datafusion"))
    {
        let deps: Vec<String> = resolve
            .deps(package_id)
            .filter(|(package_id, _)| package_id.name().starts_with("datafusion"))
            .map(|(package_id, _)| package_id.name().to_string())
            .collect();
        package_deps.insert(package_id.name().to_string(), deps);
    }

    // check for circular dependencies
    for (root_package, deps) in &package_deps {
        let mut seen = HashSet::new();
        for dep in deps {
            check_circular_deps(root_package, dep, &package_deps, &mut seen);
        }
    }
    println!("No circular dependencies found");
    Ok(())
}

fn check_circular_deps(
    root_package: &str,
    current_dep: &str,
    package_deps: &HashMap<String, Vec<String>>,
    seen: &mut HashSet<String>,
) {
    if root_package == current_dep {
        panic!(
            "circular dependency detected from {root_package} to self via one of {:?}",
            seen
        );
    }
    if seen.contains(current_dep) {
        return;
    }
    seen.insert(current_dep.to_string());
    if let Some(deps) = package_deps.get(current_dep) {
        for dep in deps {
            check_circular_deps(root_package, dep, package_deps, seen);
        }
    }
}
