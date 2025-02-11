use crate::udf::FFI_ScalarUDF;
use datafusion::{functions::math::abs::AbsFunc, logical_expr::ScalarUDF};

use std::sync::Arc;

pub(crate) extern "C" fn create_ffi_abs_func() -> FFI_ScalarUDF {
    let udf: Arc<ScalarUDF> = Arc::new(AbsFunc::new().into());

    udf.into()
}
