// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{ffi::c_void, sync::Arc};

use abi_stable::{
    StableAbi,
    std_types::{RResult, RVec},
};

use datafusion::error::Result;
use datafusion::{
    catalog::{TableFunctionImpl, TableProvider},
    prelude::{Expr, SessionContext},
};
use datafusion_proto::{
    logical_plan::{
        DefaultLogicalExtensionCodec, from_proto::parse_exprs, to_proto::serialize_exprs,
    },
    protobuf::LogicalExprList,
};
use prost::Message;
use tokio::runtime::Handle;

use crate::util::FFIResult;
use crate::{df_result, rresult_return, table_provider::FFI_TableProvider};

/// A stable struct for sharing a [`TableFunctionImpl`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_TableFunction {
    /// Equivalent to the `call` function of the TableFunctionImpl.
    /// The arguments are Expr passed as protobuf encoded bytes.
    pub call:
        unsafe extern "C" fn(udtf: &Self, args: RVec<u8>) -> FFIResult<FFI_TableProvider>,

    /// Used to create a clone on the provider of the udtf. This should
    /// only need to be called by the receiver of the udtf.
    pub clone: unsafe extern "C" fn(udtf: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(udtf: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the udtf.
    /// A [`ForeignTableFunction`] should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`] and
    /// the crate's `README.md` for more information.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_TableFunction {}
unsafe impl Sync for FFI_TableFunction {}

pub struct TableFunctionPrivateData {
    udtf: Arc<dyn TableFunctionImpl>,
    runtime: Option<Handle>,
}

impl FFI_TableFunction {
    fn inner(&self) -> &Arc<dyn TableFunctionImpl> {
        let private_data = self.private_data as *const TableFunctionPrivateData;
        unsafe { &(*private_data).udtf }
    }

    fn runtime(&self) -> Option<Handle> {
        let private_data = self.private_data as *const TableFunctionPrivateData;
        unsafe { (*private_data).runtime.clone() }
    }
}

unsafe extern "C" fn call_fn_wrapper(
    udtf: &FFI_TableFunction,
    args: RVec<u8>,
) -> FFIResult<FFI_TableProvider> {
    let runtime = udtf.runtime();
    let udtf = udtf.inner();

    let default_ctx = SessionContext::new();
    let codec = DefaultLogicalExtensionCodec {};

    let proto_filters = rresult_return!(LogicalExprList::decode(args.as_ref()));

    let args =
        rresult_return!(parse_exprs(proto_filters.expr.iter(), &default_ctx, &codec));

    let table_provider = rresult_return!(udtf.call(&args));
    RResult::ROk(FFI_TableProvider::new(table_provider, false, runtime))
}

unsafe extern "C" fn release_fn_wrapper(udtf: &mut FFI_TableFunction) {
    unsafe {
        debug_assert!(!udtf.private_data.is_null());
        let private_data =
            Box::from_raw(udtf.private_data as *mut TableFunctionPrivateData);
        drop(private_data);
        udtf.private_data = std::ptr::null_mut();
    }
}

unsafe extern "C" fn clone_fn_wrapper(udtf: &FFI_TableFunction) -> FFI_TableFunction {
    let runtime = udtf.runtime();
    let udtf = udtf.inner();

    FFI_TableFunction::new(Arc::clone(udtf), runtime)
}

impl Clone for FFI_TableFunction {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl FFI_TableFunction {
    pub fn new(udtf: Arc<dyn TableFunctionImpl>, runtime: Option<Handle>) -> Self {
        let private_data = Box::new(TableFunctionPrivateData { udtf, runtime });

        Self {
            call: call_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

impl From<Arc<dyn TableFunctionImpl>> for FFI_TableFunction {
    fn from(udtf: Arc<dyn TableFunctionImpl>) -> Self {
        let private_data = Box::new(TableFunctionPrivateData {
            udtf,
            runtime: None,
        });

        Self {
            call: call_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

impl Drop for FFI_TableFunction {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

/// This struct is used to access an UDTF provided by a foreign
/// library across a FFI boundary.
///
/// The ForeignTableFunction is to be used by the caller of the UDTF, so it has
/// no knowledge or access to the private data. All interaction with the UDTF
/// must occur through the functions defined in FFI_TableFunction.
#[derive(Debug)]
pub struct ForeignTableFunction(FFI_TableFunction);

unsafe impl Send for ForeignTableFunction {}
unsafe impl Sync for ForeignTableFunction {}

impl From<FFI_TableFunction> for Arc<dyn TableFunctionImpl> {
    fn from(value: FFI_TableFunction) -> Self {
        if (value.library_marker_id)() == crate::get_library_marker_id() {
            Arc::clone(value.inner())
        } else {
            Arc::new(ForeignTableFunction(value))
        }
    }
}

impl TableFunctionImpl for ForeignTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let codec = DefaultLogicalExtensionCodec {};
        let expr_list = LogicalExprList {
            expr: serialize_exprs(args, &codec)?,
        };
        let filters_serialized = expr_list.encode_to_vec().into();

        let table_provider = unsafe { (self.0.call)(&self.0, filters_serialized) };

        let table_provider = df_result!(table_provider)?;
        let table_provider: Arc<dyn TableProvider> = (&table_provider).into();

        Ok(table_provider)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{
            ArrayRef, Float64Array, RecordBatch, StringArray, UInt64Array, record_batch,
        },
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::logical_expr::ptr_eq::arc_ptr_eq;
    use datafusion::{
        catalog::MemTable, common::exec_err, prelude::lit, scalar::ScalarValue,
    };

    #[derive(Debug)]
    struct TestUDTF {}

    impl TableFunctionImpl for TestUDTF {
        fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
            let args = args
                .iter()
                .map(|arg| {
                    if let Expr::Literal(scalar, _) = arg {
                        Ok(scalar)
                    } else {
                        exec_err!("Expected only literal arguments to table udf")
                    }
                })
                .collect::<Result<Vec<_>>>()?;

            if args.len() < 2 {
                exec_err!("Expected at least two arguments to table udf")?
            }

            let ScalarValue::UInt64(Some(num_rows)) = args[0].to_owned() else {
                exec_err!(
                    "First argument must be the number of elements to create as u64"
                )?
            };
            let num_rows = num_rows as usize;

            let mut fields = Vec::default();
            let mut arrays1 = Vec::default();
            let mut arrays2 = Vec::default();

            let split = num_rows / 3;
            for (idx, arg) in args[1..].iter().enumerate() {
                let (field, array) = match arg {
                    ScalarValue::Utf8(s) => {
                        let s_vec = vec![s.to_owned(); num_rows];
                        (
                            Field::new(format!("field-{idx}"), DataType::Utf8, true),
                            Arc::new(StringArray::from(s_vec)) as ArrayRef,
                        )
                    }
                    ScalarValue::UInt64(v) => {
                        let v_vec = vec![v.to_owned(); num_rows];
                        (
                            Field::new(format!("field-{idx}"), DataType::UInt64, true),
                            Arc::new(UInt64Array::from(v_vec)) as ArrayRef,
                        )
                    }
                    ScalarValue::Float64(v) => {
                        let v_vec = vec![v.to_owned(); num_rows];
                        (
                            Field::new(format!("field-{idx}"), DataType::Float64, true),
                            Arc::new(Float64Array::from(v_vec)) as ArrayRef,
                        )
                    }
                    _ => exec_err!(
                        "Test case only supports utf8, u64, and f64. Found {}",
                        arg.data_type()
                    )?,
                };

                fields.push(field);
                arrays1.push(array.slice(0, split));
                arrays2.push(array.slice(split, num_rows - split));
            }

            let schema = Arc::new(Schema::new(fields));
            let batches = vec![
                RecordBatch::try_new(Arc::clone(&schema), arrays1)?,
                RecordBatch::try_new(Arc::clone(&schema), arrays2)?,
            ];

            let table_provider = MemTable::try_new(schema, vec![batches])?;

            Ok(Arc::new(table_provider))
        }
    }

    #[tokio::test]
    async fn test_round_trip_udtf() -> Result<()> {
        let original_udtf = Arc::new(TestUDTF {}) as Arc<dyn TableFunctionImpl>;

        let mut local_udtf: FFI_TableFunction =
            FFI_TableFunction::new(Arc::clone(&original_udtf), None);
        local_udtf.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_udf: Arc<dyn TableFunctionImpl> = local_udtf.into();

        let table = foreign_udf.call(&[lit(6_u64), lit("one"), lit(2.0), lit(3_u64)])?;

        let ctx = SessionContext::default();
        let _ = ctx.register_table("test-table", table)?;

        let returned_batches = ctx.table("test-table").await?.collect().await?;

        assert_eq!(returned_batches.len(), 2);
        let expected_batch_0 = record_batch!(
            ("field-0", Utf8, ["one", "one"]),
            ("field-1", Float64, [2.0, 2.0]),
            ("field-2", UInt64, [3, 3])
        )?;
        assert_eq!(returned_batches[0], expected_batch_0);

        let expected_batch_1 = record_batch!(
            ("field-0", Utf8, ["one", "one", "one", "one"]),
            ("field-1", Float64, [2.0, 2.0, 2.0, 2.0]),
            ("field-2", UInt64, [3, 3, 3, 3])
        )?;
        assert_eq!(returned_batches[1], expected_batch_1);

        Ok(())
    }

    #[test]
    fn test_ffi_udtf_local_bypass() -> Result<()> {
        let original_udtf = Arc::new(TestUDTF {}) as Arc<dyn TableFunctionImpl>;

        let mut ffi_udtf = FFI_TableFunction::from(Arc::clone(&original_udtf));

        // Verify local libraries can be downcast to their original
        let foreign_udtf: Arc<dyn TableFunctionImpl> = ffi_udtf.clone().into();
        assert!(arc_ptr_eq(&original_udtf, &foreign_udtf));

        // Verify different library markers generate foreign providers
        ffi_udtf.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_udtf: Arc<dyn TableFunctionImpl> = ffi_udtf.into();
        assert!(!arc_ptr_eq(&original_udtf, &foreign_udtf));

        Ok(())
    }
}
