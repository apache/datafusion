use arrow::datatypes::DataType;

#[cfg(test)]
pub fn create_decimal_array(
    array: &[Option<i128>],
    precision: usize,
    scale: usize,
) -> datafusion_common::Result<arrow::array::Int128Array> {
    use arrow::array::{Int128Vec, TryPush};
    let mut decimal_builder = Int128Vec::from_data(
        DataType::Decimal(precision, scale),
        Vec::<i128>::with_capacity(array.len()),
        None,
    );

    for value in array {
        match value {
            None => {
                decimal_builder.push(None);
            }
            Some(v) => {
                decimal_builder.try_push(Some(*v))?;
            }
        }
    }
    Ok(decimal_builder.into())
}

#[cfg(test)]
pub fn create_decimal_array_from_slice(
    array: &[i128],
    precision: usize,
    scale: usize,
) -> datafusion_common::Result<arrow::array::Int128Array> {
    let decimal_array_values: Vec<Option<i128>> =
        array.into_iter().map(|v| Some(*v)).collect();
    create_decimal_array(&decimal_array_values, precision, scale)
}
