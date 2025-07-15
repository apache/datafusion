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

use abi_stable::{
    std_types::{ROption, RResult, RString, RVec},
    StableAbi,
};
use arrow::datatypes::Schema;
use arrow::{
    compute::SortOptions,
    datatypes::{DataType, SchemaRef},
};
use arrow_schema::{Field, FieldRef};
use datafusion::{
    error::DataFusionError,
    logical_expr::{
        function::WindowUDFFieldArgs, type_coercion::functions::fields_with_window_udf,
        PartitionEvaluator,
    },
};
use datafusion::{
    error::Result,
    logical_expr::{Signature, WindowUDF, WindowUDFImpl},
};
use partition_evaluator::{FFI_PartitionEvaluator, ForeignPartitionEvaluator};
use partition_evaluator_args::{
    FFI_PartitionEvaluatorArgs, ForeignPartitionEvaluatorArgs,
};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::{ffi::c_void, sync::Arc};
mod partition_evaluator;
mod partition_evaluator_args;
mod range;

use crate::util::{rvec_wrapped_to_vec_fieldref, vec_fieldref_to_rvec_wrapped};
use crate::{
    arrow_wrappers::WrappedSchema,
    df_result, rresult, rresult_return,
    util::{rvec_wrapped_to_vec_datatype, vec_datatype_to_rvec_wrapped},
    volatility::FFI_Volatility,
};

/// A stable struct for sharing a [`WindowUDF`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_WindowUDF {
    /// FFI equivalent to the `name` of a [`WindowUDF`]
    pub name: RString,

    /// FFI equivalent to the `aliases` of a [`WindowUDF`]
    pub aliases: RVec<RString>,

    /// FFI equivalent to the `volatility` of a [`WindowUDF`]
    pub volatility: FFI_Volatility,

    pub partition_evaluator:
        unsafe extern "C" fn(
            udwf: &Self,
            args: FFI_PartitionEvaluatorArgs,
        ) -> RResult<FFI_PartitionEvaluator, RString>,

    pub field: unsafe extern "C" fn(
        udwf: &Self,
        input_types: RVec<WrappedSchema>,
        display_name: RString,
    ) -> RResult<WrappedSchema, RString>,

    /// Performs type coersion. To simply this interface, all UDFs are treated as having
    /// user defined signatures, which will in turn call coerce_types to be called. This
    /// call should be transparent to most users as the internal function performs the
    /// appropriate calls on the underlying [`WindowUDF`]
    pub coerce_types: unsafe extern "C" fn(
        udf: &Self,
        arg_types: RVec<WrappedSchema>,
    ) -> RResult<RVec<WrappedSchema>, RString>,

    pub sort_options: ROption<FFI_SortOptions>,

    /// Used to create a clone on the provider of the udf. This should
    /// only need to be called by the receiver of the udf.
    pub clone: unsafe extern "C" fn(udf: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(udf: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the udf.
    /// A [`ForeignWindowUDF`] should never attempt to access this data.
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_WindowUDF {}
unsafe impl Sync for FFI_WindowUDF {}

pub struct WindowUDFPrivateData {
    pub udf: Arc<WindowUDF>,
}

impl FFI_WindowUDF {
    unsafe fn inner(&self) -> &Arc<WindowUDF> {
        let private_data = self.private_data as *const WindowUDFPrivateData;
        &(*private_data).udf
    }
}

unsafe extern "C" fn partition_evaluator_fn_wrapper(
    udwf: &FFI_WindowUDF,
    args: FFI_PartitionEvaluatorArgs,
) -> RResult<FFI_PartitionEvaluator, RString> {
    let inner = udwf.inner();

    let args = rresult_return!(ForeignPartitionEvaluatorArgs::try_from(args));

    let evaluator = rresult_return!(inner.partition_evaluator_factory((&args).into()));

    RResult::ROk(evaluator.into())
}

unsafe extern "C" fn field_fn_wrapper(
    udwf: &FFI_WindowUDF,
    input_fields: RVec<WrappedSchema>,
    display_name: RString,
) -> RResult<WrappedSchema, RString> {
    let inner = udwf.inner();

    let input_fields = rresult_return!(rvec_wrapped_to_vec_fieldref(&input_fields));

    let field = rresult_return!(inner.field(WindowUDFFieldArgs::new(
        &input_fields,
        display_name.as_str()
    )));

    let schema = Arc::new(Schema::new(vec![field]));

    RResult::ROk(WrappedSchema::from(schema))
}

unsafe extern "C" fn coerce_types_fn_wrapper(
    udwf: &FFI_WindowUDF,
    arg_types: RVec<WrappedSchema>,
) -> RResult<RVec<WrappedSchema>, RString> {
    let inner = udwf.inner();

    let arg_fields = rresult_return!(rvec_wrapped_to_vec_datatype(&arg_types))
        .into_iter()
        .map(|dt| Field::new("f", dt, false))
        .map(Arc::new)
        .collect::<Vec<_>>();

    let return_fields = rresult_return!(fields_with_window_udf(&arg_fields, inner));
    let return_types = return_fields
        .into_iter()
        .map(|f| f.data_type().to_owned())
        .collect::<Vec<_>>();

    rresult!(vec_datatype_to_rvec_wrapped(&return_types))
}

unsafe extern "C" fn release_fn_wrapper(udwf: &mut FFI_WindowUDF) {
    let private_data = Box::from_raw(udwf.private_data as *mut WindowUDFPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(udwf: &FFI_WindowUDF) -> FFI_WindowUDF {
    // let private_data = udf.private_data as *const WindowUDFPrivateData;
    // let udf_data = &(*private_data);

    // let private_data = Box::new(WindowUDFPrivateData {
    //     udf: Arc::clone(&udf_data.udf),
    // });
    let private_data = Box::new(WindowUDFPrivateData {
        udf: Arc::clone(udwf.inner()),
    });

    FFI_WindowUDF {
        name: udwf.name.clone(),
        aliases: udwf.aliases.clone(),
        volatility: udwf.volatility.clone(),
        partition_evaluator: partition_evaluator_fn_wrapper,
        sort_options: udwf.sort_options.clone(),
        coerce_types: coerce_types_fn_wrapper,
        field: field_fn_wrapper,
        clone: clone_fn_wrapper,
        release: release_fn_wrapper,
        private_data: Box::into_raw(private_data) as *mut c_void,
    }
}

impl Clone for FFI_WindowUDF {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl From<Arc<WindowUDF>> for FFI_WindowUDF {
    fn from(udf: Arc<WindowUDF>) -> Self {
        let name = udf.name().into();
        let aliases = udf.aliases().iter().map(|a| a.to_owned().into()).collect();
        let volatility = udf.signature().volatility.into();
        let sort_options = udf.sort_options().map(|v| (&v).into()).into();

        let private_data = Box::new(WindowUDFPrivateData { udf });

        Self {
            name,
            aliases,
            volatility,
            partition_evaluator: partition_evaluator_fn_wrapper,
            sort_options,
            coerce_types: coerce_types_fn_wrapper,
            field: field_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }
}

impl Drop for FFI_WindowUDF {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

/// This struct is used to access an UDF provided by a foreign
/// library across a FFI boundary.
///
/// The ForeignWindowUDF is to be used by the caller of the UDF, so it has
/// no knowledge or access to the private data. All interaction with the UDF
/// must occur through the functions defined in FFI_WindowUDF.
#[derive(Debug)]
pub struct ForeignWindowUDF {
    name: String,
    aliases: Vec<String>,
    udf: FFI_WindowUDF,
    signature: Signature,
}

unsafe impl Send for ForeignWindowUDF {}
unsafe impl Sync for ForeignWindowUDF {}

impl TryFrom<&FFI_WindowUDF> for ForeignWindowUDF {
    type Error = DataFusionError;

    fn try_from(udf: &FFI_WindowUDF) -> Result<Self, Self::Error> {
        let name = udf.name.to_owned().into();
        let signature = Signature::user_defined((&udf.volatility).into());

        let aliases = udf.aliases.iter().map(|s| s.to_string()).collect();

        Ok(Self {
            name,
            udf: udf.clone(),
            aliases,
            signature,
        })
    }
}

impl WindowUDFImpl for ForeignWindowUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        unsafe {
            let arg_types = vec_datatype_to_rvec_wrapped(arg_types)?;
            let result_types = df_result!((self.udf.coerce_types)(&self.udf, arg_types))?;
            Ok(rvec_wrapped_to_vec_datatype(&result_types)?)
        }
    }

    fn partition_evaluator(
        &self,
        args: datafusion::logical_expr::function::PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let evaluator = unsafe {
            let args = FFI_PartitionEvaluatorArgs::try_from(args)?;
            (self.udf.partition_evaluator)(&self.udf, args)
        };

        df_result!(evaluator).map(|evaluator| {
            Box::new(ForeignPartitionEvaluator::from(evaluator))
                as Box<dyn PartitionEvaluator>
        })
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        unsafe {
            let input_types = vec_fieldref_to_rvec_wrapped(field_args.input_fields())?;
            let schema = df_result!((self.udf.field)(
                &self.udf,
                input_types,
                field_args.name().into()
            ))?;
            let schema: SchemaRef = schema.into();

            match schema.fields().is_empty() {
                true => Err(DataFusionError::Execution(
                    "Unable to retrieve field in WindowUDF via FFI".to_string(),
                )),
                false => Ok(schema.field(0).to_owned().into()),
            }
        }
    }

    fn sort_options(&self) -> Option<SortOptions> {
        let options: Option<&FFI_SortOptions> = self.udf.sort_options.as_ref().into();
        options.map(|s| s.into())
    }

    fn equals(&self, other: &dyn WindowUDFImpl) -> bool {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return false;
        };
        let Self {
            name,
            aliases,
            udf,
            signature,
        } = self;
        name == &other.name
            && aliases == &other.aliases
            && std::ptr::eq(udf, &other.udf)
            && signature == &other.signature
    }

    fn hash_value(&self) -> u64 {
        let Self {
            name,
            aliases,
            udf,
            signature,
        } = self;
        let mut hasher = DefaultHasher::new();
        std::any::type_name::<Self>().hash(&mut hasher);
        name.hash(&mut hasher);
        aliases.hash(&mut hasher);
        std::ptr::hash(udf, &mut hasher);
        signature.hash(&mut hasher);
        hasher.finish()
    }
}

#[repr(C)]
#[derive(Debug, StableAbi, Clone)]
#[allow(non_camel_case_types)]
pub struct FFI_SortOptions {
    pub descending: bool,
    pub nulls_first: bool,
}

impl From<&SortOptions> for FFI_SortOptions {
    fn from(value: &SortOptions) -> Self {
        Self {
            descending: value.descending,
            nulls_first: value.nulls_first,
        }
    }
}

impl From<&FFI_SortOptions> for SortOptions {
    fn from(value: &FFI_SortOptions) -> Self {
        Self {
            descending: value.descending,
            nulls_first: value.nulls_first,
        }
    }
}

#[cfg(test)]
#[cfg(feature = "integration-tests")]
mod tests {
    use crate::tests::create_record_batch;
    use crate::udwf::{FFI_WindowUDF, ForeignWindowUDF};
    use arrow::array::{create_array, ArrayRef};
    use datafusion::functions_window::lead_lag::{lag_udwf, WindowShift};
    use datafusion::logical_expr::expr::Sort;
    use datafusion::logical_expr::{col, ExprFunctionExt, WindowUDF, WindowUDFImpl};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    fn create_test_foreign_udwf(
        original_udwf: impl WindowUDFImpl + 'static,
    ) -> datafusion::common::Result<WindowUDF> {
        let original_udwf = Arc::new(WindowUDF::from(original_udwf));

        let local_udwf: FFI_WindowUDF = Arc::clone(&original_udwf).into();

        let foreign_udwf: ForeignWindowUDF = (&local_udwf).try_into()?;
        Ok(foreign_udwf.into())
    }

    #[test]
    fn test_round_trip_udwf() -> datafusion::common::Result<()> {
        let original_udwf = lag_udwf();
        let original_name = original_udwf.name().to_owned();

        // Convert to FFI format
        let local_udwf: FFI_WindowUDF = Arc::clone(&original_udwf).into();

        // Convert back to native format
        let foreign_udwf: ForeignWindowUDF = (&local_udwf).try_into()?;
        let foreign_udwf: WindowUDF = foreign_udwf.into();

        assert_eq!(original_name, foreign_udwf.name());
        Ok(())
    }

    #[tokio::test]
    async fn test_lag_udwf() -> datafusion::common::Result<()> {
        let udwf = create_test_foreign_udwf(WindowShift::lag())?;

        let ctx = SessionContext::default();
        let df = ctx.read_batch(create_record_batch(-5, 5))?;

        let df = df.select(vec![
            col("a"),
            udwf.call(vec![col("a")])
                .order_by(vec![Sort::new(col("a"), true, true)])
                .build()
                .unwrap()
                .alias("lag_a"),
        ])?;

        df.clone().show().await?;

        let result = df.collect().await?;
        let expected =
            create_array!(Int32, [None, Some(-5), Some(-4), Some(-3), Some(-2)])
                as ArrayRef;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].column(1), &expected);

        Ok(())
    }
}
