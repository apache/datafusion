#[derive(Debug, Clone, PartialEq)]
pub struct DocumentationTest {
    /// the description for the UDF
    pub description: String,
    /// a brief example of the syntax. For example "ascii(str)"
    pub syntax_example: String,
}