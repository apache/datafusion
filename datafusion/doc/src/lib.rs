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

#![cfg_attr(test, allow(clippy::needless_pass_by_value))]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod udaf;
mod udf;
mod udwf;

pub use udaf::aggregate_doc_sections;
pub use udf::scalar_doc_sections;
pub use udwf::window_doc_sections;

#[allow(rustdoc::broken_intra_doc_links)]
/// Documentation for use by [`ScalarUDFImpl`](ScalarUDFImpl),
/// [`AggregateUDFImpl`](AggregateUDFImpl) and [`WindowUDFImpl`](WindowUDFImpl) functions.
///
/// See the [`DocumentationBuilder`] to create a new [`Documentation`] struct.
///
/// The DataFusion [SQL function documentation] is automatically  generated from these structs.
/// The name of the udf will be pulled from the [`ScalarUDFImpl::name`](ScalarUDFImpl::name),
/// [`AggregateUDFImpl::name`](AggregateUDFImpl::name) or [`WindowUDFImpl::name`](WindowUDFImpl::name)
/// function as appropriate.
///
/// All strings in the documentation are required to be
/// in [markdown format](https://www.markdownguide.org/basic-syntax/).
///
/// Currently, documentation only supports a single language
/// thus all text should be in English.
///
/// [SQL function documentation]: https://datafusion.apache.org/user-guide/sql/index.html
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Documentation {
    /// The section in the documentation where the UDF will be documented
    pub doc_section: DocSection,
    /// The description for the UDF
    pub description: String,
    /// A brief example of the syntax. For example "ascii(str)"
    pub syntax_example: String,
    /// A sql example for the UDF, usually in the form of a sql prompt
    /// query and output. It is strongly recommended to provide an
    /// example for anything but the most basic UDF's
    pub sql_example: Option<String>,
    /// Arguments for the UDF which will be displayed in array order.
    /// Left member of a pair is the argument name, right is a
    /// description for the argument
    pub arguments: Option<Vec<(String, String)>>,
    /// A list of alternative syntax examples for a function
    pub alternative_syntax: Option<Vec<String>>,
    /// Related functions if any. Values should match the related
    /// udf's name exactly. Related udf's must be of the same
    /// UDF type (scalar, aggregate or window) for proper linking to
    /// occur
    pub related_udfs: Option<Vec<String>>,
}

impl Documentation {
    /// Returns a new [`DocumentationBuilder`] with no options set.
    pub fn builder(
        doc_section: DocSection,
        description: impl Into<String>,
        syntax_example: impl Into<String>,
    ) -> DocumentationBuilder {
        DocumentationBuilder::new_with_details(doc_section, description, syntax_example)
    }

    /// Output the `Documentation` struct in form of custom Rust documentation attributes
    /// It is useful to semi automate during tmigration of UDF documentation
    /// generation from code based to attribute based and can be safely removed after
    pub fn to_doc_attribute(&self) -> String {
        let mut result = String::new();

        result.push_str("#[user_doc(");
        // Doc Section
        result.push_str(
            format!(
                "\n    doc_section({}label = \"{}\"{}),",
                if !self.doc_section.include {
                    "include = \"false\", "
                } else {
                    ""
                },
                self.doc_section.label,
                self.doc_section
                    .description
                    .map(|s| format!(", description = \"{s}\""))
                    .unwrap_or_default(),
            )
            .as_ref(),
        );

        // Description
        result.push_str(format!("\n    description=\"{}\",", self.description).as_ref());
        // Syntax Example
        result.push_str(
            format!("\n    syntax_example=\"{}\",", self.syntax_example).as_ref(),
        );
        // SQL Example
        result.push_str(
            &self
                .sql_example
                .clone()
                .map(|s| format!("\n    sql_example = r#\"{s}\"#,"))
                .unwrap_or_default(),
        );

        let st_arg_token = " expression to operate on. Can be a constant, column, or function, and any combination of operators.";
        // Standard Arguments
        if let Some(args) = self.arguments.clone() {
            args.iter().for_each(|(name, value)| {
                if value.contains(st_arg_token) {
                    if name.starts_with("The ") {
                        result.push_str(format!("\n    standard_argument(\n        name = \"{name}\"),").as_ref());
                    } else {
                        result.push_str(format!("\n    standard_argument(\n        name = \"{}\",\n        prefix = \"{}\"\n    ),", name, value.replace(st_arg_token, "")).as_ref());
                    }
                }
            });
        }

        // Arguments
        if let Some(args) = self.arguments.clone() {
            args.iter().for_each(|(name, value)| {
                if !value.contains(st_arg_token) {
                    result.push_str(format!("\n    argument(\n        name = \"{name}\",\n        description = \"{value}\"\n    ),").as_ref());
                }
            });
        }

        if let Some(alt_syntax) = self.alternative_syntax.clone() {
            alt_syntax.iter().for_each(|syntax| {
                result.push_str(
                    format!("\n    alternative_syntax = \"{syntax}\",").as_ref(),
                );
            });
        }

        // Related UDFs
        if let Some(related_udf) = self.related_udfs.clone() {
            related_udf.iter().for_each(|udf| {
                result.push_str(format!("\n    related_udf(name = \"{udf}\"),").as_ref());
            });
        }

        result.push_str("\n)]");

        result
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DocSection {
    /// True to include this doc section in the public
    /// documentation, false otherwise
    pub include: bool,
    /// A display label for the doc section. For example: "Math Expressions"
    pub label: &'static str,
    /// An optional description for the doc section
    pub description: Option<&'static str>,
}

impl Default for DocSection {
    /// Returns a "default" Doc section.
    ///
    /// This is suitable for user defined functions that do not appear in the
    /// DataFusion documentation.
    fn default() -> Self {
        Self {
            include: true,
            label: "Default",
            description: None,
        }
    }
}

/// A builder for [`Documentation`]'s.
///
/// Example:
///
/// ```rust
///
/// # fn main() {
///     use datafusion_doc::{DocSection, Documentation};
///     let doc_section = DocSection {
///         include: true,
///         label: "Display Label",
///         description: None,
///     };
///
///     let documentation = Documentation::builder(doc_section, "Add one to an int32".to_owned(), "add_one(2)".to_owned())
///           .with_argument("arg_1", "The int32 number to add one to")
///           .build();
/// # }
pub struct DocumentationBuilder {
    pub doc_section: DocSection,
    pub description: String,
    pub syntax_example: String,
    pub sql_example: Option<String>,
    pub arguments: Option<Vec<(String, String)>>,
    pub alternative_syntax: Option<Vec<String>>,
    pub related_udfs: Option<Vec<String>>,
}

impl DocumentationBuilder {
    /// Creates a new [`DocumentationBuilder`] with all required fields
    pub fn new_with_details(
        doc_section: DocSection,
        description: impl Into<String>,
        syntax_example: impl Into<String>,
    ) -> Self {
        Self {
            doc_section,
            description: description.into(),
            syntax_example: syntax_example.into(),
            sql_example: None,
            arguments: None,
            alternative_syntax: None,
            related_udfs: None,
        }
    }

    pub fn with_doc_section(mut self, doc_section: DocSection) -> Self {
        self.doc_section = doc_section;
        self
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    pub fn with_syntax_example(mut self, syntax_example: impl Into<String>) -> Self {
        self.syntax_example = syntax_example.into();
        self
    }

    pub fn with_sql_example(mut self, sql_example: impl Into<String>) -> Self {
        self.sql_example = Some(sql_example.into());
        self
    }

    /// Adds documentation for a specific argument to the documentation.
    ///
    /// Arguments are displayed in the order they are added.
    pub fn with_argument(
        mut self,
        arg_name: impl Into<String>,
        arg_description: impl Into<String>,
    ) -> Self {
        let mut args = self.arguments.unwrap_or_default();
        args.push((arg_name.into(), arg_description.into()));
        self.arguments = Some(args);
        self
    }

    /// Add a standard "expression" argument to the documentation
    ///
    /// The argument is rendered like below if Some() is passed through:
    ///
    /// ```text
    /// <arg_name>:
    ///   <expression_type> expression to operate on. Can be a constant, column, or function, and any combination of operators.
    /// ```
    ///
    /// The argument is rendered like below if None is passed through:
    ///
    ///  ```text
    /// <arg_name>:
    ///   The expression to operate on. Can be a constant, column, or function, and any combination of operators.
    /// ```
    pub fn with_standard_argument(
        self,
        arg_name: impl Into<String>,
        expression_type: Option<&str>,
    ) -> Self {
        let description = format!(
            "{} expression to operate on. Can be a constant, column, or function, and any combination of operators.",
            expression_type.unwrap_or("The")
        );
        self.with_argument(arg_name, description)
    }

    pub fn with_alternative_syntax(mut self, syntax_name: impl Into<String>) -> Self {
        let mut alternative_syntax_array = self.alternative_syntax.unwrap_or_default();
        alternative_syntax_array.push(syntax_name.into());
        self.alternative_syntax = Some(alternative_syntax_array);
        self
    }

    pub fn with_related_udf(mut self, related_udf: impl Into<String>) -> Self {
        let mut related = self.related_udfs.unwrap_or_default();
        related.push(related_udf.into());
        self.related_udfs = Some(related);
        self
    }

    /// Build the documentation from provided components
    ///
    /// Panics if `doc_section`, `description` or `syntax_example` is not set
    pub fn build(self) -> Documentation {
        let Self {
            doc_section,
            description,
            syntax_example,
            sql_example,
            arguments,
            alternative_syntax,
            related_udfs,
        } = self;

        Documentation {
            doc_section,
            description,
            syntax_example,
            sql_example,
            arguments,
            alternative_syntax,
            related_udfs,
        }
    }
}
