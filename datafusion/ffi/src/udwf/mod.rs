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

use std::ffi::c_void;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow_schema::{Field, FieldRef};
use datafusion_common::{Result, ToDFSchema, ffi_err};
use datafusion_expr::function::{WindowFunctionSimplification, WindowUDFFieldArgs};
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::type_coercion::functions::fields_with_udf;
use datafusion_expr::{
    Documentation, Expr, LimitEffect, PartitionEvaluator, ReversedUDWF, Signature,
    WindowUDF, WindowUDFImpl,
};
use datafusion_physical_expr::PhysicalExpr;
use expression_args::{FFI_ExpressionArgs, ForeignExpressionArgs};
use partition_evaluator::FFI_PartitionEvaluator;
use partition_evaluator_args::{
    FFI_PartitionEvaluatorArgs, ForeignPartitionEvaluatorArgs,
};

use stabby::string::String as SString;
use stabby::vec::Vec as SVec;

mod expression_args;
mod partition_evaluator;
mod partition_evaluator_args;
mod range;

use crate::arrow_wrappers::WrappedSchema;
use crate::physical_expr::FFI_PhysicalExpr;
use crate::util::{
    FFI_Option, FFI_Result, rvec_wrapped_to_vec_datatype, rvec_wrapped_to_vec_fieldref,
    vec_datatype_to_rvec_wrapped, vec_fieldref_to_rvec_wrapped,
};
use crate::volatility::FFI_Volatility;
use crate::{df_result, sresult, sresult_return};
use prost::Message;

/// A stable struct for sharing a [`WindowUDF`] across FFI boundaries.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_WindowUDF {
    /// FFI equivalent to the `name` of a [`WindowUDF`]
    pub name: SString,

    /// FFI equivalent to the `aliases` of a [`WindowUDF`]
    pub aliases: SVec<SString>,

    /// FFI equivalent to the `volatility` of a [`WindowUDF`]
    pub volatility: FFI_Volatility,

    pub partition_evaluator: unsafe extern "C" fn(
        udwf: &Self,
        args: FFI_PartitionEvaluatorArgs,
    )
        -> FFI_Result<FFI_PartitionEvaluator>,

    pub field: unsafe extern "C" fn(
        udwf: &Self,
        input_types: SVec<WrappedSchema>,
        display_name: SString,
    ) -> FFI_Result<WrappedSchema>,

    /// Pointer lifetime is tied to the inner Arc; null = None
    pub documentation: unsafe extern "C" fn(udwf: &Self) -> *const Documentation,

    /// Returns expressions in the same order as input_exprs
    pub expressions: unsafe extern "C" fn(
        udwf: &Self,
        args: FFI_ExpressionArgs,
    ) -> SVec<FFI_PhysicalExpr>,

    /// Serializes WindowFunction via DefaultLogicalExtensionCodec;
    /// returns None variant if no simplification; only called when has_simplify=true
    pub simplify: unsafe extern "C" fn(
        udwf: &Self,
        window_function: SVec<u8>,
        schema: WrappedSchema,
    ) -> FFI_Result<FFI_Option<SVec<u8>>>,

    /// Returns FFI_ReversedUDWF enum; Reversed variant contains a cloned FFI_WindowUDF
    pub reverse_expr: unsafe extern "C" fn(udwf: &Self) -> FFI_ReversedUDWF,

    pub coerce_types: unsafe extern "C" fn(
        udf: &Self,
        arg_types: SVec<WrappedSchema>,
    ) -> FFI_Result<SVec<WrappedSchema>>,

    pub sort_options: FFI_Option<FFI_SortOptions>,

    pub has_simplify: bool,

    /// Used to create a clone on the provider of the udf. This should
    /// only need to be called by the receiver of the udf.
    pub clone: unsafe extern "C" fn(udf: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(udf: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the udf.
    /// A [`ForeignWindowUDF`] should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`] and
    /// the crate's `README.md` for more information.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_WindowUDF {}
unsafe impl Sync for FFI_WindowUDF {}

pub struct WindowUDFPrivateData {
    pub udf: Arc<WindowUDF>,
}

impl FFI_WindowUDF {
    unsafe fn inner(&self) -> &Arc<WindowUDF> {
        unsafe {
            let private_data = self.private_data as *const WindowUDFPrivateData;
            &(*private_data).udf
        }
    }
}

unsafe extern "C" fn partition_evaluator_fn_wrapper(
    udwf: &FFI_WindowUDF,
    args: FFI_PartitionEvaluatorArgs,
) -> FFI_Result<FFI_PartitionEvaluator> {
    unsafe {
        let inner = udwf.inner();

        let args = sresult_return!(ForeignPartitionEvaluatorArgs::try_from(args));

        let evaluator =
            sresult_return!(inner.partition_evaluator_factory((&args).into()));

        FFI_Result::Ok(evaluator.into())
    }
}

unsafe extern "C" fn field_fn_wrapper(
    udwf: &FFI_WindowUDF,
    input_fields: SVec<WrappedSchema>,
    display_name: SString,
) -> FFI_Result<WrappedSchema> {
    unsafe {
        let inner = udwf.inner();

        let input_fields = sresult_return!(rvec_wrapped_to_vec_fieldref(&input_fields));

        let field = sresult_return!(inner.field(WindowUDFFieldArgs::new(
            &input_fields,
            display_name.as_str()
        )));

        let schema = Arc::new(Schema::new(vec![field]));

        FFI_Result::Ok(WrappedSchema::from(schema))
    }
}

unsafe extern "C" fn documentation_fn_wrapper(
    udwf: &FFI_WindowUDF,
) -> *const Documentation {
    unsafe {
        let inner = udwf.inner();
        match inner.documentation() {
            Some(doc) => doc as *const Documentation,
            None => std::ptr::null(),
        }
    }
}

unsafe extern "C" fn expressions_fn_wrapper(
    udwf: &FFI_WindowUDF,
    args: FFI_ExpressionArgs,
) -> SVec<FFI_PhysicalExpr> {
    unsafe {
        let inner = udwf.inner();
        let args = match ForeignExpressionArgs::try_from(args) {
            Ok(args) => args,
            Err(_) => return SVec::new(),
        };
        let expressions = inner.expressions((&args).into());
        expressions
            .into_iter()
            .map(FFI_PhysicalExpr::from)
            .collect()
    }
}

unsafe extern "C" fn simplify_fn_wrapper(
    udwf: &FFI_WindowUDF,
    window_function_bytes: SVec<u8>,
    schema: WrappedSchema,
) -> FFI_Result<FFI_Option<SVec<u8>>> {
    unsafe {
        let inner = udwf.inner();

        // 1. Deserialize bytes to Expr using Default codec
        let protobuf =
            sresult_return!(datafusion_proto::protobuf::LogicalExprNode::decode(
                window_function_bytes.as_ref()
            ));
        let mut ctx = datafusion_execution::TaskContext::default();
        // Register the wrapped UDWF so it can be resolved during deserialization
        sresult_return!(ctx.register_udwf(Arc::clone(inner)));
        let codec = datafusion_proto::logical_plan::DefaultLogicalExtensionCodec {};
        let expr =
            sresult_return!(datafusion_proto::logical_plan::from_proto::parse_expr(
                &protobuf, &ctx, &codec
            ));

        // 2. Extract WindowFunction from Expr
        let window_function = match expr {
            Expr::WindowFunction(wf) => wf,
            _ => return FFI_Result::Err("Expected WindowFunction Expr".into()),
        };

        // 3. Create dummy SimplifyContext
        let schema_ref: SchemaRef = schema.into();
        let df_schema = sresult_return!(schema_ref.to_dfschema_ref());
        let info = SimplifyContext::builder().with_schema(df_schema).build();

        // 4. Call inner.simplify()
        match inner.simplify() {
            Some(simplify_fn) => {
                let simplified_expr =
                    sresult_return!(simplify_fn(*window_function, &info));
                let protobuf = sresult_return!(
                    datafusion_proto::logical_plan::to_proto::serialize_expr(
                        &simplified_expr,
                        &codec
                    )
                );
                let mut buffer = Vec::new();
                sresult_return!(Message::encode(&protobuf, &mut buffer));
                FFI_Result::Ok(FFI_Option::Some(buffer.into_iter().collect()))
            }
            None => FFI_Result::Ok(FFI_Option::None),
        }
    }
}

unsafe extern "C" fn reverse_expr_fn_wrapper(udwf: &FFI_WindowUDF) -> FFI_ReversedUDWF {
    unsafe {
        let inner = udwf.inner();
        match inner.reverse_expr() {
            ReversedUDWF::Identical => FFI_ReversedUDWF::Identical,
            ReversedUDWF::NotSupported => FFI_ReversedUDWF::NotSupported,
            ReversedUDWF::Reversed(udf) => FFI_ReversedUDWF::Reversed(udf.into()),
        }
    }
}

unsafe extern "C" fn coerce_types_fn_wrapper(
    udwf: &FFI_WindowUDF,
    arg_types: SVec<WrappedSchema>,
) -> FFI_Result<SVec<WrappedSchema>> {
    unsafe {
        let inner = udwf.inner();

        let arg_fields = sresult_return!(rvec_wrapped_to_vec_datatype(&arg_types))
            .into_iter()
            .map(|dt| Field::new("f", dt, false))
            .map(Arc::new)
            .collect::<Vec<_>>();

        let return_fields = sresult_return!(fields_with_udf(&arg_fields, inner.as_ref()));
        let return_types = return_fields
            .into_iter()
            .map(|f| f.data_type().to_owned())
            .collect::<Vec<_>>();

        sresult!(vec_datatype_to_rvec_wrapped(&return_types))
    }
}

unsafe extern "C" fn release_fn_wrapper(udwf: &mut FFI_WindowUDF) {
    unsafe {
        debug_assert!(!udwf.private_data.is_null());
        let private_data = Box::from_raw(udwf.private_data as *mut WindowUDFPrivateData);
        drop(private_data);
        udwf.private_data = std::ptr::null_mut();
    }
}

unsafe extern "C" fn clone_fn_wrapper(udwf: &FFI_WindowUDF) -> FFI_WindowUDF {
    unsafe {
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
            has_simplify: udwf.has_simplify,
            coerce_types: coerce_types_fn_wrapper,
            field: field_fn_wrapper,
            documentation: documentation_fn_wrapper,
            expressions: expressions_fn_wrapper,
            simplify: simplify_fn_wrapper,
            reverse_expr: reverse_expr_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

impl Clone for FFI_WindowUDF {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl From<Arc<WindowUDF>> for FFI_WindowUDF {
    fn from(udf: Arc<WindowUDF>) -> Self {
        if let Some(udwf) = udf.inner().downcast_ref::<ForeignWindowUDF>() {
            return udwf.udf.clone();
        }

        let name = udf.name().into();
        let aliases = udf.aliases().iter().map(|a| a.to_owned().into()).collect();
        let volatility = udf.signature().volatility.into();
        let sort_options = udf.sort_options().map(|v| (&v).into()).into();
        let has_simplify = udf.inner().simplify().is_some();

        let private_data = Box::new(WindowUDFPrivateData { udf });

        Self {
            name,
            aliases,
            volatility,
            partition_evaluator: partition_evaluator_fn_wrapper,
            sort_options,
            has_simplify,
            coerce_types: coerce_types_fn_wrapper,
            field: field_fn_wrapper,
            documentation: documentation_fn_wrapper,
            expressions: expressions_fn_wrapper,
            simplify: simplify_fn_wrapper,
            reverse_expr: reverse_expr_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
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

impl PartialEq for ForeignWindowUDF {
    fn eq(&self, other: &Self) -> bool {
        // FFI_WindowUDF cannot be compared, so identity equality is the best we can do.
        std::ptr::eq(self, other)
    }
}
impl Eq for ForeignWindowUDF {}
impl Hash for ForeignWindowUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::ptr::hash(self, state)
    }
}

impl From<&FFI_WindowUDF> for Arc<dyn WindowUDFImpl> {
    fn from(udf: &FFI_WindowUDF) -> Self {
        if (udf.library_marker_id)() == crate::get_library_marker_id() {
            Arc::clone(unsafe { udf.inner().inner() })
        } else {
            let name = udf.name.to_owned().into();
            let signature = Signature::user_defined((&udf.volatility).into());

            let aliases = udf.aliases.iter().map(|s| s.to_string()).collect();

            Arc::new(ForeignWindowUDF {
                name,
                udf: udf.clone(),
                aliases,
                signature,
            })
        }
    }
}

impl WindowUDFImpl for ForeignWindowUDF {
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
        args: datafusion_expr::function::PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let evaluator = unsafe {
            let args = FFI_PartitionEvaluatorArgs::try_from(args)?;
            (self.udf.partition_evaluator)(&self.udf, args)
        };

        df_result!(evaluator).map(<Box<dyn PartitionEvaluator>>::from)
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
                true => ffi_err!(
                    "Unable to retrieve field in WindowUDF via FFI - schema has no fields"
                ),
                false => Ok(schema.field(0).to_owned().into()),
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        unsafe {
            let ptr = (self.udf.documentation)(&self.udf);
            ptr.as_ref()
        }
    }

    fn expressions(
        &self,
        expr_args: datafusion_expr::function::ExpressionArgs,
    ) -> Vec<Arc<dyn PhysicalExpr>> {
        unsafe {
            let fallback = expr_args.input_exprs().to_vec();
            let args = match FFI_ExpressionArgs::try_from(expr_args) {
                Ok(args) => args,
                Err(_) => return fallback,
            };
            (self.udf.expressions)(&self.udf, args)
                .into_iter()
                .map(|e| Arc::<dyn PhysicalExpr>::from(&e))
                .collect()
        }
    }

    fn simplify(&self) -> Option<WindowFunctionSimplification> {
        if !self.udf.has_simplify {
            return None;
        }

        let udf = self.udf.clone();
        Some(Box::new(move |wf, info| {
            let codec = datafusion_proto::logical_plan::DefaultLogicalExtensionCodec {};

            // To serialize the window function
            let expr = Expr::WindowFunction(Box::new(wf));
            let protobuf = datafusion_proto::logical_plan::to_proto::serialize_expr(
                &expr, &codec,
            )
            .map_err(|e| datafusion_common::DataFusionError::Plan(e.to_string()))?;

            let mut buffer = Vec::new();
            Message::encode(&protobuf, &mut buffer)
                .map_err(|e| datafusion_common::DataFusionError::Plan(e.to_string()))?;

            let schema_ref: SchemaRef = Arc::new(info.schema().as_arrow().clone());
            let schema = WrappedSchema::from(schema_ref);

            // Call the FFI function
            let result =
                unsafe { (udf.simplify)(&udf, buffer.into_iter().collect(), schema) };

            let result: Option<SVec<u8>> = crate::df_result!(result)?.into();

            match result {
                Some(bytes) => {
                    let protobuf = datafusion_proto::protobuf::LogicalExprNode::decode(
                        bytes.as_slice(),
                    )
                    .map_err(|e| {
                        datafusion_common::DataFusionError::Plan(e.to_string())
                    })?;
                    let ctx = datafusion_execution::TaskContext::default();
                    let simplified_expr =
                        datafusion_proto::logical_plan::from_proto::parse_expr(
                            &protobuf, &ctx, &codec,
                        )?;
                    Ok(simplified_expr)
                }
                None => Ok(expr),
            }
        }))
    }

    fn reverse_expr(&self) -> ReversedUDWF {
        unsafe { (self.udf.reverse_expr)(&self.udf).into() }
    }

    fn sort_options(&self) -> Option<SortOptions> {
        let options: Option<&FFI_SortOptions> = self.udf.sort_options.as_ref();
        options.map(|s| s.into())
    }

    fn limit_effect(&self, _args: &[Arc<dyn PhysicalExpr>]) -> LimitEffect {
        LimitEffect::Unknown
    }
}

#[repr(C)]
#[derive(Debug, Clone)]
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

#[repr(C, u8)]
#[derive(Debug, Clone)]
pub enum FFI_ReversedUDWF {
    Identical,
    NotSupported,
    Reversed(FFI_WindowUDF),
}

impl From<FFI_ReversedUDWF> for ReversedUDWF {
    fn from(value: FFI_ReversedUDWF) -> Self {
        match value {
            FFI_ReversedUDWF::Identical => ReversedUDWF::Identical,
            FFI_ReversedUDWF::NotSupported => ReversedUDWF::NotSupported,
            FFI_ReversedUDWF::Reversed(ffi_udf) => {
                let udf_impl: Arc<dyn WindowUDFImpl> = (&ffi_udf).into();
                ReversedUDWF::Reversed(Arc::new(WindowUDF::new_from_shared_impl(
                    udf_impl,
                )))
            }
        }
    }
}

#[cfg(test)]
#[cfg(feature = "integration-tests")]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, create_array};
    use arrow_schema::FieldRef;
    use datafusion::functions_window::lead_lag::{WindowShift, lag_udwf};
    use datafusion::logical_expr::expr::Sort;
    use datafusion::logical_expr::{ExprFunctionExt, WindowUDF, WindowUDFImpl, col};
    use datafusion::prelude::SessionContext;
    use datafusion_expr::function::WindowUDFFieldArgs;
    use datafusion_expr::{PartitionEvaluator, Signature};

    use crate::tests::create_record_batch;
    use crate::udwf::{FFI_WindowUDF, ForeignWindowUDF};

    fn create_test_foreign_udwf(
        original_udwf: impl WindowUDFImpl + 'static,
    ) -> datafusion::common::Result<WindowUDF> {
        let original_udwf = Arc::new(WindowUDF::from(original_udwf));

        let mut local_udwf: FFI_WindowUDF = Arc::clone(&original_udwf).into();
        local_udwf.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_udwf: Arc<dyn WindowUDFImpl> = (&local_udwf).into();
        Ok(WindowUDF::new_from_shared_impl(foreign_udwf))
    }

    #[test]
    fn test_round_trip_udwf() -> datafusion::common::Result<()> {
        let original_udwf = lag_udwf();
        let original_name = original_udwf.name().to_owned();

        // Convert to FFI format
        let mut local_udwf: FFI_WindowUDF = Arc::clone(&original_udwf).into();
        local_udwf.library_marker_id = crate::mock_foreign_marker_id;

        // Convert back to native format
        let foreign_udwf: Arc<dyn WindowUDFImpl> = (&local_udwf).into();
        let foreign_udwf = WindowUDF::new_from_shared_impl(foreign_udwf);

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

    #[test]
    fn test_ffi_udwf_local_bypass() -> datafusion_common::Result<()> {
        let original_udwf = Arc::new(WindowUDF::from(WindowShift::lag()));

        let mut ffi_udwf = FFI_WindowUDF::from(original_udwf);

        // Verify local libraries can be downcast to their original
        let foreign_udwf: Arc<dyn WindowUDFImpl> = (&ffi_udwf).into();
        assert!(foreign_udwf.is::<WindowShift>());

        // Verify different library markers generate foreign providers
        ffi_udwf.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_udwf: Arc<dyn WindowUDFImpl> = (&ffi_udwf).into();
        assert!(foreign_udwf.is::<ForeignWindowUDF>());

        Ok(())
    }

    #[test]
    fn test_ffi_udwf_documentation() -> datafusion_common::Result<()> {
        use datafusion_expr::{
            DocSection, Documentation, Volatility, function::PartitionEvaluatorArgs,
        };

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        struct MockUDWFWithDoc {
            signature: Signature,
            doc: Documentation,
        }

        impl WindowUDFImpl for MockUDWFWithDoc {
            fn name(&self) -> &str {
                "mock_doc"
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn partition_evaluator(
                &self,
                _: PartitionEvaluatorArgs,
            ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
                unimplemented!()
            }
            fn field(
                &self,
                _: WindowUDFFieldArgs,
            ) -> datafusion_common::Result<FieldRef> {
                unimplemented!()
            }
            fn documentation(&self) -> Option<&Documentation> {
                Some(&self.doc)
            }
        }

        let doc = Documentation::builder(DocSection::default(), "description", "syntax")
            .build();
        let original_udwf = Arc::new(WindowUDF::from(MockUDWFWithDoc {
            signature: Signature::any(0, Volatility::Immutable),
            doc: doc.clone(),
        }));

        let mut ffi_udwf = FFI_WindowUDF::from(Arc::clone(&original_udwf));
        ffi_udwf.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_udwf: Arc<dyn WindowUDFImpl> = (&ffi_udwf).into();
        assert_eq!(foreign_udwf.documentation(), Some(&doc));
        Ok(())
    }

    #[test]
    fn test_ffi_udwf_expressions() -> datafusion_common::Result<()> {
        use arrow::datatypes::DataType;
        use datafusion_expr::{
            Volatility,
            function::{ExpressionArgs, PartitionEvaluatorArgs},
        };
        use datafusion_physical_expr::PhysicalExpr;
        use datafusion_physical_expr::expressions::col;

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        struct MockUDWFWithExprs {
            signature: Signature,
        }

        impl WindowUDFImpl for MockUDWFWithExprs {
            fn name(&self) -> &str {
                "mock_exprs"
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn partition_evaluator(
                &self,
                _: PartitionEvaluatorArgs,
            ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
                unimplemented!()
            }
            fn field(
                &self,
                _: WindowUDFFieldArgs,
            ) -> datafusion_common::Result<FieldRef> {
                unimplemented!()
            }
            fn expressions(&self, args: ExpressionArgs) -> Vec<Arc<dyn PhysicalExpr>> {
                args.input_exprs().iter().rev().cloned().collect()
            }
        }

        let original_udwf = Arc::new(WindowUDF::from(MockUDWFWithExprs {
            signature: Signature::any(0, Volatility::Immutable),
        }));

        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", DataType::Int32, true),
            arrow::datatypes::Field::new("b", DataType::Int32, true),
        ]);
        let expr_a = col("a", &schema)?;
        let expr_b = col("b", &schema)?;
        let fields = vec![
            Arc::new(arrow::datatypes::Field::new("a", DataType::Int32, true)),
            Arc::new(arrow::datatypes::Field::new("b", DataType::Int32, true)),
        ];

        let mut ffi_udwf = FFI_WindowUDF::from(Arc::clone(&original_udwf));
        ffi_udwf.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_udwf: Arc<dyn WindowUDFImpl> = (&ffi_udwf).into();

        let input_exprs = [expr_a, expr_b];
        let args = ExpressionArgs::new(&input_exprs, &fields);
        let result = foreign_udwf.expressions(args);
        assert_eq!(result.len(), 2);
        assert_eq!(format!("{}", result[0]), "b@1");
        assert_eq!(format!("{}", result[1]), "a@0");
        Ok(())
    }

    #[test]
    fn test_ffi_udwf_simplify() -> datafusion_common::Result<()> {
        use datafusion_expr::{
            Volatility,
            function::{PartitionEvaluatorArgs, WindowFunctionSimplification},
            lit,
        };

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        struct MockUDWFSimplify {
            signature: Signature,
        }

        impl WindowUDFImpl for MockUDWFSimplify {
            fn name(&self) -> &str {
                "mock_simplify"
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn partition_evaluator(
                &self,
                _: PartitionEvaluatorArgs,
            ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
                unimplemented!()
            }
            fn field(
                &self,
                _: WindowUDFFieldArgs,
            ) -> datafusion_common::Result<FieldRef> {
                unimplemented!()
            }
            fn simplify(&self) -> Option<WindowFunctionSimplification> {
                Some(Box::new(|_, _| Ok(lit(1))))
            }
        }

        let original_udwf = Arc::new(WindowUDF::from(MockUDWFSimplify {
            signature: Signature::any(0, Volatility::Immutable),
        }));
        let mut ffi_udwf = FFI_WindowUDF::from(Arc::clone(&original_udwf));
        ffi_udwf.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_udwf: Arc<dyn WindowUDFImpl> = (&ffi_udwf).into();
        let simplify_fn = foreign_udwf.simplify().unwrap();

        let wf = datafusion_expr::expr::WindowFunction {
            fun: datafusion_expr::WindowFunctionDefinition::WindowUDF(original_udwf),
            params: datafusion_expr::expr::WindowFunctionParams {
                args: vec![],
                partition_by: vec![],
                order_by: vec![],
                window_frame: datafusion_expr::WindowFrame::new(None),
                filter: None,
                null_treatment: None,
                distinct: false,
            },
        };

        let schema = arrow::datatypes::Schema::empty();
        let df_schema = datafusion_common::DFSchema::try_from(schema).unwrap();
        let info = datafusion_expr::simplify::SimplifyContext::builder()
            .with_schema(Arc::new(df_schema))
            .build();

        let simplified_expr = simplify_fn(wf, &info).unwrap();
        assert_eq!(simplified_expr, lit(1));

        Ok(())
    }

    #[test]
    fn test_ffi_udwf_reverse_expr() -> datafusion_common::Result<()> {
        use datafusion_expr::{
            ReversedUDWF, Volatility, function::PartitionEvaluatorArgs,
        };

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        struct MockUDWFReverse {
            signature: Signature,
        }

        impl WindowUDFImpl for MockUDWFReverse {
            fn name(&self) -> &str {
                "mock_reverse"
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn partition_evaluator(
                &self,
                _: PartitionEvaluatorArgs,
            ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
                unimplemented!()
            }
            fn field(
                &self,
                _: WindowUDFFieldArgs,
            ) -> datafusion_common::Result<FieldRef> {
                unimplemented!()
            }
            fn reverse_expr(&self) -> ReversedUDWF {
                ReversedUDWF::Identical
            }
        }

        let original_udwf = Arc::new(WindowUDF::from(MockUDWFReverse {
            signature: Signature::any(0, Volatility::Immutable),
        }));

        let mut ffi_udwf = FFI_WindowUDF::from(Arc::clone(&original_udwf));
        ffi_udwf.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_udwf: Arc<dyn WindowUDFImpl> = (&ffi_udwf).into();
        assert!(matches!(
            foreign_udwf.reverse_expr(),
            ReversedUDWF::Identical
        ));

        Ok(())
    }

    #[test]
    fn test_ffi_udwf_reverse_expr_recursive() -> datafusion_common::Result<()> {
        use datafusion_expr::{
            ReversedUDWF, Volatility, function::PartitionEvaluatorArgs,
        };

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        struct MockUDWFRecursive {
            signature: Signature,
            reversed: Arc<WindowUDF>,
        }

        impl WindowUDFImpl for MockUDWFRecursive {
            fn name(&self) -> &str {
                "mock_recursive"
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn partition_evaluator(
                &self,
                _: PartitionEvaluatorArgs,
            ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
                unimplemented!()
            }
            fn field(
                &self,
                _: WindowUDFFieldArgs,
            ) -> datafusion_common::Result<FieldRef> {
                unimplemented!()
            }
            fn reverse_expr(&self) -> ReversedUDWF {
                ReversedUDWF::Reversed(Arc::clone(&self.reversed))
            }
        }

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        struct MockUDWFSimple {
            signature: Signature,
        }
        impl WindowUDFImpl for MockUDWFSimple {
            fn name(&self) -> &str {
                "mock_simple"
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn partition_evaluator(
                &self,
                _: PartitionEvaluatorArgs,
            ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
                unimplemented!()
            }
            fn field(
                &self,
                _: WindowUDFFieldArgs,
            ) -> datafusion_common::Result<FieldRef> {
                unimplemented!()
            }
        }

        let reversed = Arc::new(WindowUDF::from(MockUDWFSimple {
            signature: Signature::any(0, Volatility::Immutable),
        }));
        let original_udwf = Arc::new(WindowUDF::from(MockUDWFRecursive {
            signature: Signature::any(0, Volatility::Immutable),
            reversed: Arc::clone(&reversed),
        }));

        // Forced foreign path
        let mut ffi_udwf = FFI_WindowUDF::from(Arc::clone(&original_udwf));
        ffi_udwf.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_udwf: Arc<dyn WindowUDFImpl> = (&ffi_udwf).into();

        let result = foreign_udwf.reverse_expr();
        if let ReversedUDWF::Reversed(res_udf) = result {
            assert_eq!(res_udf.name(), "mock_simple");
        } else {
            panic!("Expected Reversed variant");
        }

        Ok(())
    }
}
