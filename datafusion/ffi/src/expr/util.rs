use abi_stable::std_types::RVec;
use prost::Message;
use datafusion_common::{exec_datafusion_err, DataFusionError, Result, ScalarValue};

pub fn scalar_value_to_rvec_u8(value: &ScalarValue) -> Result<RVec<u8>> {
    let value: datafusion_proto_common::ScalarValue = value.try_into()?;
    Ok(value.encode_to_vec().into())
}

pub fn rvec_u8_to_scalar_value(value: &RVec<u8>) -> Result<ScalarValue> {

    let value = datafusion_proto_common::ScalarValue::decode(value.as_ref())
        .map_err(|err| exec_datafusion_err!("{err}"))?;

    (&value).try_into().map_err(|err| exec_datafusion_err!("{err}"))
}