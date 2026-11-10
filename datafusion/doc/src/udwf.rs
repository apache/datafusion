// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
