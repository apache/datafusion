// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
