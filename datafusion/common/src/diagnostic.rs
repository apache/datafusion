use sqlparser::tokenizer::Span;

#[derive(Debug, Clone)]
pub struct Diagnostic {
    pub entries: Vec<DiagnosticEntry>,
}

#[derive(Debug, Clone)]
pub struct DiagnosticEntry {
    pub span: Span,
    pub message: String,
    pub kind: DiagnosticEntryKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiagnosticEntryKind {
    Error,
    Warning,
    Note,
    Help,
}
