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

// Aggregate UDF doc sections for use in public documentation
pub mod aggregate_doc_sections {
    use crate::DocSection;

    pub fn doc_sections() -> Vec<DocSection> {
        vec![
            DOC_SECTION_GENERAL,
            DOC_SECTION_STATISTICAL,
            DOC_SECTION_APPROXIMATE,
        ]
    }

    pub const DOC_SECTION_GENERAL: DocSection = DocSection {
        include: true,
        label: "General Functions",
        description: None,
    };

    pub const DOC_SECTION_STATISTICAL: DocSection = DocSection {
        include: true,
        label: "Statistical Functions",
        description: None,
    };

    pub const DOC_SECTION_APPROXIMATE: DocSection = DocSection {
        include: true,
        label: "Approximate Functions",
        description: None,
    };
}
