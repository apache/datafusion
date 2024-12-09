use std::{
    cmp::Ordering,
    fmt::{self, Debug, Display},
    ops::{Deref, DerefMut},
};

use derivative::Derivative;
use sqlparser::tokenizer::Span;

/// A wrapper type for entitites that somehow refer to a location in the source
/// code. For example, a [`DataType`](arrow_schema::DataType) can be tied to an
/// expression which is being planned for execution, and the expression is in
/// turn located somewhere in the source code.
#[derive(Clone, Derivative)]
#[derivative(PartialEq, Eq, PartialOrd, Ord)]
pub struct WithSpans<T> {
    #[derivative(PartialEq = "ignore", PartialOrd = "ignore", Ord = "ignore")]
    pub spans: Vec<Span>,
    pub inner: T,
}

impl<T> WithSpans<T> {
    pub fn into_inner(self) -> T {
        self.inner
    }

    pub fn new<IT>(inner: T, spans: IT) -> Self
    where
        IT: IntoIterator<Item = Span>,
    {
        Self {
            spans: spans.into_iter().collect(),
            inner,
        }
    }

    pub fn new_without_span(inner: T) -> Self {
        Self {
            spans: vec![],
            inner,
        }
    }
}

impl<T> From<T> for WithSpans<T> {
    fn from(inner: T) -> Self {
        Self::new_without_span(inner)
    }
}

impl<T> Deref for WithSpans<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for WithSpans<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T> AsRef<T> for WithSpans<T> {
    fn as_ref(&self) -> &T {
        &self.inner
    }
}

impl<T: Debug> Debug for WithSpans<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: Display> Display for WithSpans<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}
