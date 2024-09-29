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

/// Documentation for use by [`crate::ScalarUDFImpl`],
/// [`crate::AggregateUDFImpl`] and [`crate::WindowUDFImpl`] functions
/// that will be used to generate public documentation.
///
/// The name of the udf will be pulled from the [`crate::ScalarUDFImpl::name`],
/// [`crate::AggregateUDFImpl::name`] or [`crate::WindowUDFImpl::name`] function
/// as appropriate.
///
/// All strings in the documentation are required to be
/// in [markdown format](https://www.markdownguide.org/basic-syntax/).
///
/// Currently, documentation only supports a single language
/// thus all text should be in English.
#[derive(Debug, Clone)]
pub struct Documentation {
    /// the section in the documentation where the UDF will be documented
    pub doc_section: DocSection,
    /// the description for the UDF
    pub description: &'static str,
    pub syntax_example: &'static str,
    /// a sql example for the UDF, usually in the form of a sql prompt
    /// query and output. It is strongly recommended to provide an
    /// example for anything but the most basic UDF's
    pub sql_example: Option<&'static str>,
    /// arguments for the UDF which will be displayed in array order.
    /// Left member of a pair is the argument name, right is a
    /// description for the argument
    pub arguments: Option<&'static [(&'static str, &'static str)]>,
    /// related functions if any. Values should match the related
    /// udf's name exactly. Related udf's must be of the same
    /// UDF type (scalar, aggregate or window) for proper linking to
    /// occur
    pub related_udfs: Option<&'static [&'static str]>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DocSection {
    /// true to include this doc section in the public
    /// documentation, false otherwise
    pub include: bool,
    /// a display label for the doc section. For example: "Math Expressions"
    pub label: &'static str,
    /// an optional description for the doc section
    pub description: Option<&'static str>,
}

pub const DOCUMENTATION_NONE: Documentation = Documentation {
    doc_section: DOC_SECTION_NONE,
    description: "",
    syntax_example: "",
    sql_example: None,
    arguments: None,
    related_udfs: None,
};

/// A doc section that indicated the UDF should not
/// be publicly documented
pub const DOC_SECTION_NONE: DocSection = DocSection {
    include: false,
    label: "",
    description: None,
};
