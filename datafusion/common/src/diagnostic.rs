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
    pub fn new() -> Self {
        Default::default()
    }
}

impl Default for Diagnostic {
    fn default() -> Self {
        Diagnostic {
            entries: Vec::new(),
        }
    }
}

impl FromIterator<DiagnosticEntry> for Diagnostic {
    fn from_iter<T: IntoIterator<Item = DiagnosticEntry>>(iter: T) -> Self {
        Diagnostic {
            entries: iter.into_iter().collect(),
        }
    }
}

macro_rules! with_kind {
    ($name:ident, $kind:expr) => {
        pub fn $name(mut self, message: impl Into<String>, span: Span) -> Self {
            let entry = DiagnosticEntry {
                span,
                message: message.into(),
                kind: $kind,
            };
            self.entries.push(entry);
            self
        }
    };
}

macro_rules! add_kind {
    ($name:ident, $kind:expr) => {
        pub fn $name(&mut self, message: impl Into<String>, span: Span) {
            let entry = DiagnosticEntry {
                span,
                message: message.into(),
                kind: $kind,
            };
            self.entries.push(entry);
        }
    };
}

impl Diagnostic {
    with_kind!(with_error, DiagnosticEntryKind::Error);
    with_kind!(with_warning, DiagnosticEntryKind::Warning);
    with_kind!(with_note, DiagnosticEntryKind::Note);
    with_kind!(with_help, DiagnosticEntryKind::Help);

    add_kind!(add_error, DiagnosticEntryKind::Error);
    add_kind!(add_warning, DiagnosticEntryKind::Warning);
    add_kind!(add_note, DiagnosticEntryKind::Note);
    add_kind!(add_help, DiagnosticEntryKind::Help);
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
