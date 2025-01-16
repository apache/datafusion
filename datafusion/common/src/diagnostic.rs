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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticEntryKind {
    Error,
    Warning,
    Note,
    Help,
}

impl Diagnostic {
    pub fn new(entries: impl IntoIterator<Item = DiagnosticEntry>) -> Self {
        Diagnostic::from_iter(entries)
    }
}

impl FromIterator<DiagnosticEntry> for Diagnostic {
    fn from_iter<T: IntoIterator<Item = DiagnosticEntry>>(iter: T) -> Self {
        Diagnostic {
            entries: iter.into_iter().collect(),
        }
    }
}

impl DiagnosticEntry {
    pub fn new(
        message: impl Into<String>,
        kind: DiagnosticEntryKind,
        span: Span,
    ) -> Self {
        DiagnosticEntry {
            span,
            message: message.into(),
            kind,
        }
    }

    pub fn new_without_span(
        message: impl Into<String>,
        kind: DiagnosticEntryKind,
    ) -> Self {
        Self::new(message, kind, Span::empty())
    }
}
