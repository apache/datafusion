use indexmap::IndexMap;

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
    /// arguments for the UDF which will be displayed in insertion
    /// order. Key is the argument name, value is a description for
    /// the argument
    pub arguments: Option<IndexMap<&'static str, &'static str>>,
    /// related functions if any. Values should match the related
    /// udf's name exactly. Related udf's must be of the same
    /// UDF type (scalar, aggregate or window) for proper linking to
    /// occur
    pub related_udfs: Option<Vec<&'static str>>,
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
