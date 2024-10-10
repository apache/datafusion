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

#[derive(Debug, Clone)]
pub struct DocumentationTest {
    /// the section in the documentation where the UDF will be documented
    pub doc_section: DocSectionTest,
    /// the description for the UDF
    pub description: String,
    /// a brief example of the syntax. For example "ascii(str)"
    pub syntax_example: String,
    /// a sql example for the UDF, usually in the form of a sql prompt
    /// query and output. It is strongly recommended to provide an
    /// example for anything but the most basic UDF's
    pub sql_example: Option<String>,
    /// arguments for the UDF which will be displayed in array order.
    /// Left member of a pair is the argument name, right is a
    /// description for the argument
    pub arguments: Option<Vec<(String, String)>>,
    /// related functions if any. Values should match the related
    /// udf's name exactly. Related udf's must be of the same
    /// UDF type (scalar, aggregate or window) for proper linking to
    /// occur
    pub related_udfs: Option<Vec<String>>,
}

impl DocumentationTest {
    /// Returns a new [`DocumentationBuilder`] with no options set.
    pub fn builder() -> DocumentationBuilderTest {
        DocumentationBuilderTest::new()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DocSectionTest {
    /// true to include this doc section in the public
    /// documentation, false otherwise
    pub include: bool,
    /// a display label for the doc section. For example: "Math Expressions"
    pub label: &'static str,
    /// an optional description for the doc section
    pub description: Option<&'static str>,
}

pub struct DocumentationBuilderTest {
    pub doc_section: Option<DocSectionTest>,
    pub description: Option<String>,
    pub syntax_example: Option<String>,
    pub sql_example: Option<String>,
    pub arguments: Option<Vec<(String, String)>>,
    pub related_udfs: Option<Vec<String>>,
}

impl DocumentationBuilderTest {
    pub fn new() -> Self {
        Self {
            doc_section: None,
            description: None,
            syntax_example: None,
            sql_example: None,
            arguments: None,
            related_udfs: None,
        }
    }

    pub fn with_doc_section(mut self, doc_section: DocSectionTest) -> Self {
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
    /// This is similar to  [`Self::with_argument`] except that  a standard
    /// description is appended to the end: `"Can be a constant, column, or
    /// function, and any combination of arithmetic operators."`
    ///
    /// The argument is rendered like
    ///
    /// ```text
    /// <arg_name>:
    ///   <expression_type> expression to operate on. Can be a constant, column, or function, and any combination of arithmetic operators.
    /// ```
    pub fn with_standard_argument(
        self,
        arg_name: impl Into<String>,
        expression_type: impl AsRef<str>,
    ) -> Self {
        let expression_type = expression_type.as_ref();
        self.with_argument(arg_name, format!("{expression_type} expression to operate on. Can be a constant, column, or function, and any combination of operators."))
    }

    pub fn with_related_udf(mut self, related_udf: impl Into<String>) -> Self {
        let mut related = self.related_udfs.unwrap_or_default();
        related.push(related_udf.into());
        self.related_udfs = Some(related);
        self
    }

    pub fn build(self) -> DocumentationTest {
        let Self {
            doc_section,
            description,
            syntax_example,
            sql_example,
            arguments,
            related_udfs,
        } = self;

        if doc_section.is_none() {
            panic!("Documentation must have a doc section");
        }

        if description.is_none() {
            panic!("Documentation must have a description");
        }

        if syntax_example.is_none() {
            panic!("Documentation must have a syntax_example");
        }

        DocumentationTest {
            doc_section: doc_section.unwrap(),
            description: description.unwrap(),
            syntax_example: syntax_example.unwrap(),
            sql_example,
            arguments,
            related_udfs,
        }
    }
}

impl Default for DocumentationBuilderTest {
    fn default() -> Self {
        Self::new()
    }
}
