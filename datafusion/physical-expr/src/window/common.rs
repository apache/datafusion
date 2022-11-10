use arrow::array::ArrayRef;
use arrow::compute::concat;
use datafusion_common::Result;

pub fn concat_cols(
    lhs_col: Option<ArrayRef>,
    rhs_col: Option<ArrayRef>,
) -> Result<Option<ArrayRef>> {
    Ok(match (lhs_col, rhs_col) {
        (Some(state_col), Some(col)) => Some(concat(&[&state_col, &col])?),
        (Some(state_col), None) => Some(state_col),
        (None, Some(col)) => Some(col),
        (None, None) => None,
    })
}
