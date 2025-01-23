use smallvec::SmallVec;
use sqlparser::tokenizer::Span;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

// / A collection of [`Span`], meant to be used as a field of entities whose
/// location in the original SQL query is desired to be tracked. Sometimes an
/// entity can have multiple spans. e.g. if you want to track the position of
/// the column a that comes from SELECT 1 AS a UNION ALL SELECT 2 AS a you'll
/// need two spans.
#[derive(Debug, Clone)]
// Store teh first [`Span`] on the stack because that is by far the most common
// case. More will spill onto the heap.
pub struct Spans(pub SmallVec<[Span; 1]>);

impl Spans {
    pub fn new() -> Self {
        Spans(SmallVec::new())
    }

    pub fn first_or_empty(&self) -> Span {
        self.0.get(0).copied().unwrap_or(Span::empty())
    }

    pub fn get_spans(&self) -> &[Span] {
        &self.0
    }

    pub fn add_span(&mut self, span: Span) {
        self.0.push(span);
    }

    pub fn iter(&self) -> impl Iterator<Item = &Span> {
        self.0.iter()
    }
}

impl Default for Spans {
    fn default() -> Self {
        Self::new()
    }
}

// Since [`Spans`] will be used as a field in other structs, we don't want it to
// interfere with the equality and ordering of the entities themselves, since
// this is just diagnostics information for the end user.
impl PartialEq for Spans {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

// Since [`Spans`] will be used as a field in other structs, we don't want it to
// interfere with the equality and ordering of the entities themselves, since
// this is just diagnostics information for the end user.
impl Eq for Spans {}

// Since [`Spans`] will be used as a field in other structs, we don't want it to
// interfere with the equality and ordering of the entities themselves, since
// this is just diagnostics information for the end user.
impl PartialOrd for Spans {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        Some(Ordering::Equal)
    }
}

// Since [`Spans`] will be used as a field in other structs, we don't want it to
// interfere with the equality and ordering of the entities themselves, since
// this is just diagnostics information for the end user.
impl Ord for Spans {
    fn cmp(&self, _other: &Self) -> Ordering {
        Ordering::Equal
    }
}

// Since [`Spans`] will be used as a field in other structs, we don't want it to
// interfere with the equality and ordering of the entities themselves, since
// this is just diagnostics information for the end user.
impl Hash for Spans {
    fn hash<H: Hasher>(&self, _state: &mut H) {}
}
