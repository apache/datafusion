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

use datafusion::execution::SessionStateDefaults;
use datafusion_expr::window_doc_sections::doc_sections;
use datafusion_expr::WindowUDF;
use itertools::Itertools;
use std::fmt::Write as _;
use std::sync::Arc;

fn main() {
    let functions = SessionStateDefaults::default_window_functions();
    let mut docs = "".to_string();

    // doc sections only includes sections that have 'include' == true
    for doc_section in doc_sections() {
        // make sure there is a function that is in this doc section
        if !functions
            .iter()
            .any(|f| f.documentation().doc_section == doc_section)
        {
            continue;
        }

        // write out section header
        let _ = writeln!(&mut docs, "## {} ", doc_section.label);

        if let Some(description) = doc_section.description {
            let _ = writeln!(&mut docs, "{description}");
        }

        let filtered = functions
            .clone()
            .into_iter()
            .filter(|f| f.documentation().doc_section == doc_section)
            .collect_vec();

        // names is a sorted list of function names and aliases since we display
        // both in the documentation
        let names = get_names_and_aliases(&filtered);

        // write out the list of function names and aliases
        names.iter().for_each(|name| {
            let _ = writeln!(&mut docs, "- [{name}](#{name})");
        });

        // write out each function and alias in the order of the sorted name list
        for name in names {
            let f = filtered
                .iter()
                .find(|f| f.name() == name || f.aliases().contains(&name))
                .unwrap();
            let documentation = f.documentation();

            // if this name is an alias we need to display what it's an alias of
            if f.aliases().contains(&name) {
                let _ = write!(&mut docs, "_Alias of [{name}](#{name})._");
                continue;
            }

            // otherwise display the documentation for the function

            // first, the name, description and syntax example
            let _ = write!(
                &mut docs,
                r#"
### `{}`

{}

```
{}
```
"#,
                f.name(),
                documentation.description,
                documentation.syntax_example
            );

            // next, arguments
            if let Some(args) = &documentation.arguments {
                let _ = writeln!(&mut docs, "#### Arguments\n");
                for (arg_name, arg_desc) in args {
                    let _ = writeln!(&mut docs, "- **{arg_name}**: {arg_desc}");
                }
            }

            // next, sql example if provided
            if let Some(example) = documentation.sql_example {
                let _ = writeln!(
                    &mut docs,
                    r#"
#### Example

{}
"#,
                    example
                );
            }

            // next, aliases
            if !f.aliases().is_empty() {
                let _ = write!(&mut docs, "#### Aliases");

                for alias in f.aliases() {
                    let _ = writeln!(&mut docs, "- {alias}");
                }
            }

            // finally, any related udfs
            if let Some(related_udfs) = &documentation.related_udfs {
                let _ = writeln!(&mut docs, "\n**Related functions**:");

                for related in related_udfs {
                    let _ = writeln!(&mut docs, "- [{related}](#{related})");
                }
            }
        }
    }

    println!("{docs}");
}

fn get_names_and_aliases(functions: &[Arc<WindowUDF>]) -> Vec<String> {
    functions
        .iter()
        .flat_map(|f| {
            if f.aliases().is_empty() {
                vec![f.name().to_string()]
            } else {
                let mut names = vec![f.name().to_string()];
                names.extend(f.aliases().iter().cloned());
                names
            }
        })
        .sorted()
        .collect_vec()
}
