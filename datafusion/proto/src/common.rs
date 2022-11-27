use datafusion_common::DataFusionError;

pub fn byte_to_string(b: u8) -> Result<String, DataFusionError> {
    let b = &[b];
    let b = std::str::from_utf8(b)
        .map_err(|_| DataFusionError::Internal("Invalid CSV delimiter".to_owned()))?;
    Ok(b.to_owned())
}

pub fn str_to_byte(s: &String) -> Result<u8, DataFusionError> {
    if s.len() != 1 {
        return Err(DataFusionError::Internal(
            "Invalid CSV delimiter".to_owned(),
        ));
    }
    Ok(s.as_bytes()[0])
}

pub(crate) fn proto_error<S: Into<String>>(message: S) -> DataFusionError {
    DataFusionError::Internal(message.into())
}
