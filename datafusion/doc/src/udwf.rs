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

// Window UDF doc sections for use in public documentation
pub mod window_doc_sections {
    use crate::DocSection;

    pub fn doc_sections() -> Vec<DocSection> {
        vec![
            DOC_SECTION_AGGREGATE,
            DOC_SECTION_RANKING,
            DOC_SECTION_ANALYTICAL,
        ]
    }

    pub const DOC_SECTION_AGGREGATE: DocSection = DocSection {
        include: true,
        label: "Aggregate Functions",
        description: Some("All aggregate functions can be used as window functions."),
    };

    pub const DOC_SECTION_RANKING: DocSection = DocSection {
        include: true,
        label: "Ranking Functions",
        description: None,
    };

    pub const DOC_SECTION_ANALYTICAL: DocSection = DocSection {
        include: true,
        label: "Analytical Functions",
        description: None,
    };
}
