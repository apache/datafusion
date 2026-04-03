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

// Scalar UDF doc sections for use in public documentation
pub mod scalar_doc_sections {
    use crate::DocSection;

    pub fn doc_sections() -> Vec<DocSection> {
        vec![
            DOC_SECTION_MATH,
            DOC_SECTION_CONDITIONAL,
            DOC_SECTION_STRING,
            DOC_SECTION_BINARY_STRING,
            DOC_SECTION_REGEX,
            DOC_SECTION_DATETIME,
            DOC_SECTION_ARRAY,
            DOC_SECTION_STRUCT,
            DOC_SECTION_MAP,
            DOC_SECTION_HASHING,
            DOC_SECTION_UNION,
            DOC_SECTION_OTHER,
        ]
    }

    pub const fn doc_sections_const() -> &'static [DocSection] {
        &[
            DOC_SECTION_MATH,
            DOC_SECTION_CONDITIONAL,
            DOC_SECTION_STRING,
            DOC_SECTION_BINARY_STRING,
            DOC_SECTION_REGEX,
            DOC_SECTION_DATETIME,
            DOC_SECTION_ARRAY,
            DOC_SECTION_STRUCT,
            DOC_SECTION_MAP,
            DOC_SECTION_HASHING,
            DOC_SECTION_UNION,
            DOC_SECTION_OTHER,
        ]
    }

    pub const DOC_SECTION_MATH: DocSection = DocSection {
        include: true,
        label: "Math Functions",
        description: None,
    };

    pub const DOC_SECTION_CONDITIONAL: DocSection = DocSection {
        include: true,
        label: "Conditional Functions",
        description: None,
    };

    pub const DOC_SECTION_STRING: DocSection = DocSection {
        include: true,
        label: "String Functions",
        description: None,
    };

    pub const DOC_SECTION_BINARY_STRING: DocSection = DocSection {
        include: true,
        label: "Binary String Functions",
        description: None,
    };

    pub const DOC_SECTION_REGEX: DocSection = DocSection {
        include: true,
        label: "Regular Expression Functions",
        description: Some(
            r#"Apache DataFusion uses a [PCRE-like](https://en.wikibooks.org/wiki/Regular_Expressions/Perl-Compatible_Regular_Expressions)
regular expression [syntax](https://docs.rs/regex/latest/regex/#syntax)
(minus support for several features including look-around and backreferences).
The following regular expression functions are supported:"#,
        ),
    };

    pub const DOC_SECTION_DATETIME: DocSection = DocSection {
        include: true,
        label: "Time and Date Functions",
        description: None,
    };

    pub const DOC_SECTION_ARRAY: DocSection = DocSection {
        include: true,
        label: "Array Functions",
        description: None,
    };

    pub const DOC_SECTION_STRUCT: DocSection = DocSection {
        include: true,
        label: "Struct Functions",
        description: None,
    };

    pub const DOC_SECTION_MAP: DocSection = DocSection {
        include: true,
        label: "Map Functions",
        description: None,
    };

    pub const DOC_SECTION_HASHING: DocSection = DocSection {
        include: true,
        label: "Hashing Functions",
        description: None,
    };

    pub const DOC_SECTION_OTHER: DocSection = DocSection {
        include: true,
        label: "Other Functions",
        description: None,
    };

    pub const DOC_SECTION_UNION: DocSection = DocSection {
        include: true,
        label: "Union Functions",
        description: Some(
            "Functions to work with the union data type, also know as tagged unions, variant types, enums or sum types. Note: Not related to the SQL UNION operator",
        ),
    };
}
