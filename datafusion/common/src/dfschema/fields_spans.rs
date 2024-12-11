use sqlparser::tokenizer::Span;

use crate::JoinType;

/// The location in the source code where the fields are defined (e.g. in the
/// body of a CTE), if any.
#[derive(Debug, Clone)]
pub struct FieldsSpans(Vec<Vec<Span>>);

impl FieldsSpans {
    pub fn empty(field_count: usize) -> Self {
        Self((0..field_count).map(|_| Vec::new()).collect())
    }

    pub fn iter(&self) -> impl Iterator<Item = &Vec<Span>> {
        self.0.iter()
    }

    pub fn join(
        &self,
        other: &FieldsSpans,
        join_type: &JoinType,
        _left_cols_len: usize,
    ) -> FieldsSpans {
        match join_type {
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                FieldsSpans(self.0.iter().chain(other.0.iter()).cloned().collect())
            }
            JoinType::LeftSemi => todo!(),
            JoinType::RightSemi => todo!(),
            JoinType::LeftAnti => todo!(),
            JoinType::RightAnti => todo!(),
            JoinType::LeftMark => todo!(),
        }
    }
}

impl FromIterator<Vec<Span>> for FieldsSpans {
    fn from_iter<T: IntoIterator<Item = Vec<Span>>>(iter: T) -> Self {
        FieldsSpans(iter.into_iter().collect())
    }
}
