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

use datafusion_common::exec_err;
use datafusion_common::Result;

/// Documentation for use by [`ScalarUDFImpl`](crate::ScalarUDFImpl),
/// [`AggregateUDFImpl`](crate::AggregateUDFImpl) and [`WindowUDFImpl`](crate::WindowUDFImpl) functions
/// that will be used to generate public documentation.
///
/// The name of the udf will be pulled from the [`ScalarUDFImpl::name`](crate::ScalarUDFImpl::name),
/// [`AggregateUDFImpl::name`](crate::AggregateUDFImpl::name) or [`WindowUDFImpl::name`](crate::WindowUDFImpl::name)
/// function as appropriate.
///
/// All strings in the documentation are required to be
/// in [markdown format](https://www.markdownguide.org/basic-syntax/).
///
/// Currently, documentation only supports a single language
/// thus all text should be in English.
#[derive(Debug, Clone)]
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
    pub fn builder() -> DocumentationBuilder {
        DocumentationBuilder::new()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DocSection {
    /// True to include this doc section in the public
    /// documentation, false otherwise
    pub include: bool,
    /// A display label for the doc section. For example: "Math Expressions"
    pub label: &'static str,
    /// An optional description for the doc section
    pub description: Option<&'static str>,
}

/// A builder to be used for building [`Documentation`]'s.
///
/// Example:
///
/// ```rust
/// # use datafusion_expr::Documentation;
/// # use datafusion_expr::scalar_doc_sections::DOC_SECTION_MATH;
/// # use datafusion_common::Result;
/// #
/// # fn main() -> Result<()> {
///       let documentation = Documentation::builder()
///           .with_doc_section(DOC_SECTION_MATH)
///           .with_description("Add one to an int32")
///           .with_syntax_example("add_one(2)")
///           .with_argument("arg_1", "The int32 number to add one to")
///           .build()?;
///       Ok(())  
/// # }
pub struct DocumentationBuilder {
    pub doc_section: Option<DocSection>,
    pub description: Option<String>,
    pub syntax_example: Option<String>,
    pub sql_example: Option<String>,
    pub arguments: Option<Vec<(String, String)>>,
    pub alternative_syntax: Option<Vec<String>>,
    pub related_udfs: Option<Vec<String>>,
}

impl DocumentationBuilder {
    pub fn new() -> Self {
        Self {
            doc_section: None,
            description: None,
            syntax_example: None,
            sql_example: None,
            arguments: None,
            alternative_syntax: None,
            related_udfs: None,
        }
    }

    pub fn with_doc_section(mut self, doc_section: DocSection) -> Self {
        self.doc_section = Some(doc_section);
        self
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_syntax_example(mut self, syntax_example: impl Into<String>) -> Self {
        self.syntax_example = Some(syntax_example.into());
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

    pub fn build(self) -> Result<Documentation> {
        let Self {
            doc_section,
            description,
            syntax_example,
            sql_example,
            arguments,
            alternative_syntax,
            related_udfs,
        } = self;

        if doc_section.is_none() {
            return exec_err!("Documentation must have a doc section");
        }
        if description.is_none() {
            return exec_err!("Documentation must have a description");
        }
        if syntax_example.is_none() {
            return exec_err!("Documentation must have a syntax_example");
        }

        Ok(Documentation {
            doc_section: doc_section.unwrap(),
            description: description.unwrap(),
            syntax_example: syntax_example.unwrap(),
            sql_example,
            arguments,
            alternative_syntax,
            related_udfs,
        })
    }
}

impl Default for DocumentationBuilder {
    fn default() -> Self {
        Self::new()
    }
}
