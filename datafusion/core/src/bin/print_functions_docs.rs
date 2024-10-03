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
use datafusion_expr::{
    aggregate_doc_sections, scalar_doc_sections, window_doc_sections, AggregateUDF,
    DocSection, Documentation, ScalarUDF, WindowUDF,
};
use itertools::Itertools;
use std::env::args;
use std::fmt::Write as _;

fn main() {
    let args: Vec<String> = args().collect();

    if args.len() != 2 {
        panic!(
            "Usage: {} type (one of 'aggregate', 'scalar', 'window')",
            args[0]
        );
    }

    let function_type = args[1].trim().to_lowercase();
    let docs = match function_type.as_str() {
        "aggregate" => print_aggregate_docs(),
        "scalar" => print_scalar_docs(),
        "window" => print_window_docs(),
        _ => {
            panic!("Unknown function type: {}", function_type)
        }
    };

    println!("{docs}");
}

fn print_aggregate_docs() -> String {
    let mut providers: Vec<Box<dyn DocProvider>> = vec![];

    for f in SessionStateDefaults::default_aggregate_functions() {
        providers.push(Box::new(f.as_ref().clone()));
    }

    print_docs(providers, aggregate_doc_sections::doc_sections())
}

fn print_scalar_docs() -> String {
    let mut providers: Vec<Box<dyn DocProvider>> = vec![];

    for f in SessionStateDefaults::default_scalar_functions() {
        providers.push(Box::new(f.as_ref().clone()));
    }

    print_docs(providers, scalar_doc_sections::doc_sections())
}

fn print_window_docs() -> String {
    let mut providers: Vec<Box<dyn DocProvider>> = vec![];

    for f in SessionStateDefaults::default_window_functions() {
        providers.push(Box::new(f.as_ref().clone()));
    }

    print_docs(providers, window_doc_sections::doc_sections())
}

fn print_docs(
    providers: Vec<Box<dyn DocProvider>>,
    doc_sections: Vec<DocSection>,
) -> String {
    let mut docs = "".to_string();

    // doc sections only includes sections that have 'include' == true
    for doc_section in doc_sections {
        // make sure there is a function that is in this doc section
        if !&providers.iter().any(|f| {
            if let Some(documentation) = f.get_documentation() {
                documentation.doc_section == doc_section
            } else {
                false
            }
        }) {
            continue;
        }

        let providers: Vec<&Box<dyn DocProvider>> = providers
            .iter()
            .filter(|&f| {
                if let Some(documentation) = f.get_documentation() {
                    documentation.doc_section == doc_section
                } else {
                    false
                }
            })
            .collect::<Vec<_>>();

        // write out section header
        let _ = writeln!(docs, "## {} ", doc_section.label);

        if let Some(description) = doc_section.description {
            let _ = writeln!(docs, "{description}");
        }

        // names is a sorted list of function names and aliases since we display
        // both in the documentation
        let names = get_names_and_aliases(&providers);

        // write out the list of function names and aliases
        names.iter().for_each(|name| {
            let _ = writeln!(docs, "- [{name}](#{name})");
        });

        // write out each function and alias in the order of the sorted name list
        for name in names {
            let f = providers
                .iter()
                .find(|f| f.get_name() == name || f.get_aliases().contains(&name))
                .unwrap();

            let name = f.get_name();
            let aliases = f.get_aliases();
            let documentation = f.get_documentation();

            // if this name is an alias we need to display what it's an alias of
            if aliases.contains(&name) {
                let _ = write!(docs, "_Alias of [{name}](#{name})._");
                continue;
            }

            // otherwise display the documentation for the function
            let Some(documentation) = documentation else {
                unreachable!()
            };

            // first, the name, description and syntax example
            let _ = write!(
                docs,
                r#"
### `{}`

{}

```
{}
```
"#,
                name, documentation.description, documentation.syntax_example
            );

            // next, arguments
            if let Some(args) = &documentation.arguments {
                let _ = writeln!(docs, "#### Arguments\n");
                for (arg_name, arg_desc) in args {
                    let _ = writeln!(docs, "- **{arg_name}**: {arg_desc}");
                }
            }

            // next, sql example if provided
            if let Some(example) = &documentation.sql_example {
                let _ = writeln!(
                    docs,
                    r#"
#### Example

{}
"#,
                    example
                );
            }

            // next, aliases
            if !f.get_aliases().is_empty() {
                let _ = write!(docs, "#### Aliases");

                for alias in f.get_aliases() {
                    let _ = writeln!(docs, "- {alias}");
                }
            }

            // finally, any related udfs
            if let Some(related_udfs) = &documentation.related_udfs {
                let _ = writeln!(docs, "\n**Related functions**:");

                for related in related_udfs {
                    let _ = writeln!(docs, "- [{related}](#{related})");
                }
            }
        }
    }

    docs
}

trait DocProvider {
    fn get_name(&self) -> String;
    fn get_aliases(&self) -> Vec<String>;
    fn get_documentation(&self) -> Option<&Documentation>;
}

impl DocProvider for AggregateUDF {
    fn get_name(&self) -> String {
        self.name().to_string()
    }
    fn get_aliases(&self) -> Vec<String> {
        self.aliases().iter().map(|a| a.to_string()).collect()
    }
    fn get_documentation(&self) -> Option<&Documentation> {
        self.documentation()
    }
}

impl DocProvider for ScalarUDF {
    fn get_name(&self) -> String {
        self.name().to_string()
    }
    fn get_aliases(&self) -> Vec<String> {
        self.aliases().iter().map(|a| a.to_string()).collect()
    }
    fn get_documentation(&self) -> Option<&Documentation> {
        self.documentation()
    }
}

impl DocProvider for WindowUDF {
    fn get_name(&self) -> String {
        self.name().to_string()
    }
    fn get_aliases(&self) -> Vec<String> {
        self.aliases().iter().map(|a| a.to_string()).collect()
    }
    fn get_documentation(&self) -> Option<&Documentation> {
        self.documentation()
    }
}

#[allow(clippy::borrowed_box)]
#[allow(clippy::ptr_arg)]
fn get_names_and_aliases(functions: &Vec<&Box<dyn DocProvider>>) -> Vec<String> {
    functions
        .iter()
        .flat_map(|f| {
            if f.get_aliases().is_empty() {
                vec![f.get_name().to_string()]
            } else {
                let mut names = vec![f.get_name().to_string()];
                names.extend(f.get_aliases().iter().cloned());
                names
            }
        })
        .sorted()
        .collect_vec()
}
