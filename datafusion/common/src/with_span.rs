use std::{cmp::Ordering, fmt::{self, Debug, Display}, ops::{Deref, DerefMut}};

use sqlparser::tokenizer::Span;

/// A wrapper type for entitites that somehow refer to a location in the source
/// code. For example, a [`DataType`](arrow_schema::DataType) can be tied to an
/// expression which is being planned for execution, and the expression is in
/// turn located somewhere in the source code.
pub struct WithSpan<T> {
    pub span: Span,
    pub inner: T,
}

impl<T> WithSpan<T> {
    pub fn into_inner(self) -> T {
        self.inner
    }

    pub fn new(inner: T, span: Span) -> Self {
        Self { span, inner }
    }

    pub fn new_without_span(inner: T) -> Self {
        Self {
            span: Span::empty(),
            inner,
        }
    }
}

impl<T> From<T> for WithSpan<T> {
    fn from(inner: T) -> Self {
        Self::new_without_span(inner)
    }
}

impl<T> Deref for WithSpan<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for WithSpan<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T> AsRef<T> for WithSpan<T> {
    fn as_ref(&self) -> &T {
        &self.inner
    }
}

impl<T: PartialEq> PartialEq for WithSpan<T> {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl<T: Eq> Eq for WithSpan<T> {}

impl<T: PartialOrd> PartialOrd for WithSpan<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.inner.partial_cmp(&other.inner)
    }
}

impl<T: Ord> Ord for WithSpan<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.inner.cmp(&other.inner)
    }
}

impl<T: Debug> Debug for WithSpan<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: Display> Display for WithSpan<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: Clone> Clone for WithSpan<T> {
    fn clone(&self) -> Self {
        Self {
            span: self.span,
            inner: self.inner.clone(),
        }
    }
}
