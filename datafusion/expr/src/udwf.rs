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

//! [`WindowUDF`]: User Defined Window Functions

use arrow::compute::SortOptions;
use std::cmp::Ordering;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::{
    any::Any,
    fmt::{self, Debug, Display, Formatter},
    sync::Arc,
};

use arrow::datatypes::{DataType, Field};

use crate::expr::WindowFunction;
use crate::{
    function::WindowFunctionSimplification, Documentation, Expr, PartitionEvaluator,
    Signature,
};
use datafusion_common::{not_impl_err, Result};
use datafusion_functions_window_common::expr::ExpressionArgs;
use datafusion_functions_window_common::field::WindowUDFFieldArgs;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

/// Logical representation of a user-defined window function (UDWF)
/// A UDWF is different from a UDF in that it is stateful across batches.
///
/// See the documentation on [`PartitionEvaluator`] for more details
///
/// 1. For simple use cases, use [`create_udwf`] (examples in
///    [`simple_udwf.rs`]).
///
/// 2. For advanced use cases, use [`WindowUDFImpl`] which provides full API
///    access (examples in [`advanced_udwf.rs`]).
///
/// # API Note
/// This is a separate struct from `WindowUDFImpl` to maintain backwards
/// compatibility with the older API.
///
/// [`PartitionEvaluator`]: crate::PartitionEvaluator
/// [`create_udwf`]: crate::expr_fn::create_udwf
/// [`simple_udwf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/simple_udwf.rs
/// [`advanced_udwf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/advanced_udwf.rs
#[derive(Debug, Clone, PartialOrd)]
pub struct WindowUDF {
    inner: Arc<dyn WindowUDFImpl>,
}

/// Defines how the WindowUDF is shown to users
impl Display for WindowUDF {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl PartialEq for WindowUDF {
    fn eq(&self, other: &Self) -> bool {
        self.inner.equals(other.inner.as_ref())
    }
}

impl Eq for WindowUDF {}

impl Hash for WindowUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash_value().hash(state)
    }
}

impl WindowUDF {
    /// Create a new `WindowUDF` from a `[WindowUDFImpl]` trait object
    ///
    /// Note this is the same as using the `From` impl (`WindowUDF::from`)
    pub fn new_from_impl<F>(fun: F) -> WindowUDF
    where
        F: WindowUDFImpl + 'static,
    {
        Self {
            inner: Arc::new(fun),
        }
    }

    /// Return the underlying [`WindowUDFImpl`] trait object for this function
    pub fn inner(&self) -> &Arc<dyn WindowUDFImpl> {
        &self.inner
    }

    /// Adds additional names that can be used to invoke this function, in
    /// addition to `name`
    ///
    /// If you implement [`WindowUDFImpl`] directly you should return aliases directly.
    pub fn with_aliases(self, aliases: impl IntoIterator<Item = &'static str>) -> Self {
        Self::new_from_impl(AliasedWindowUDFImpl::new(Arc::clone(&self.inner), aliases))
    }

    /// creates a [`Expr`] that calls the window function with default
    /// values for `order_by`, `partition_by`, `window_frame`.
    ///
    /// See [`ExprFunctionExt`] for details on setting these values.
    ///
    /// This utility allows using a user defined window function without
    /// requiring access to the registry, such as with the DataFrame API.
    ///
    /// [`ExprFunctionExt`]: crate::expr_fn::ExprFunctionExt
    pub fn call(&self, args: Vec<Expr>) -> Expr {
        let fun = crate::WindowFunctionDefinition::WindowUDF(Arc::new(self.clone()));

        Expr::WindowFunction(WindowFunction::new(fun, args))
    }

    /// Returns this function's name
    ///
    /// See [`WindowUDFImpl::name`] for more details.
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// Returns the aliases for this function.
    pub fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    /// Returns this function's signature (what input types are accepted)
    ///
    /// See [`WindowUDFImpl::signature`] for more details.
    pub fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    /// Do the function rewrite
    ///
    /// See [`WindowUDFImpl::simplify`] for more details.
    pub fn simplify(&self) -> Option<WindowFunctionSimplification> {
        self.inner.simplify()
    }

    /// Expressions that are passed to the [`PartitionEvaluator`].
    ///
    /// See [`WindowUDFImpl::expressions`] for more details.
    pub fn expressions(&self, expr_args: ExpressionArgs) -> Vec<Arc<dyn PhysicalExpr>> {
        self.inner.expressions(expr_args)
    }
    /// Return a `PartitionEvaluator` for evaluating this window function
    pub fn partition_evaluator_factory(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        self.inner.partition_evaluator(partition_evaluator_args)
    }

    /// Returns the field of the final result of evaluating this window function.
    ///
    /// See [`WindowUDFImpl::field`] for more details.
    pub fn field(&self, field_args: WindowUDFFieldArgs) -> Result<Field> {
        self.inner.field(field_args)
    }

    /// Returns custom result ordering introduced by this window function
    /// which is used to update ordering equivalences.
    ///
    /// See [`WindowUDFImpl::sort_options`] for more details.
    pub fn sort_options(&self) -> Option<SortOptions> {
        self.inner.sort_options()
    }

    /// See [`WindowUDFImpl::coerce_types`] for more details.
    pub fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    /// Returns the reversed user-defined window function when the
    /// order of evaluation is reversed.
    ///
    /// See [`WindowUDFImpl::reverse_expr`] for more details.
    pub fn reverse_expr(&self) -> ReversedUDWF {
        self.inner.reverse_expr()
    }

    /// Returns the documentation for this Window UDF.
    ///
    /// Documentation can be accessed programmatically as well as
    /// generating publicly facing documentation.
    pub fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

impl<F> From<F> for WindowUDF
where
    F: WindowUDFImpl + Send + Sync + 'static,
{
    fn from(fun: F) -> Self {
        Self::new_from_impl(fun)
    }
}

/// Trait for implementing [`WindowUDF`].
///
/// This trait exposes the full API for implementing user defined window functions and
/// can be used to implement any function.
///
/// See [`advanced_udwf.rs`] for a full example with complete implementation and
/// [`WindowUDF`] for other available options.
///
///
/// [`advanced_udwf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/advanced_udwf.rs
/// # Basic Example
/// ```
/// # use std::any::Any;
/// # use std::sync::OnceLock;
/// # use arrow::datatypes::{DataType, Field};
/// # use datafusion_common::{DataFusionError, plan_err, Result};
/// # use datafusion_expr::{col, Signature, Volatility, PartitionEvaluator, WindowFrame, ExprFunctionExt, Documentation};
/// # use datafusion_expr::{WindowUDFImpl, WindowUDF};
/// # use datafusion_functions_window_common::field::WindowUDFFieldArgs;
/// # use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
/// # use datafusion_expr::window_doc_sections::DOC_SECTION_ANALYTICAL;
///
/// #[derive(Debug, Clone)]
/// struct SmoothIt {
///   signature: Signature,
/// }
///
/// impl SmoothIt {
///   fn new() -> Self {
///     Self {
///       signature: Signature::uniform(1, vec![DataType::Int32], Volatility::Immutable),
///      }
///   }
/// }
///
/// static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();
///
/// fn get_doc() -> &'static Documentation {
///     DOCUMENTATION.get_or_init(|| {
///         Documentation::builder()
///             .with_doc_section(DOC_SECTION_ANALYTICAL)
///             .with_description("smooths the windows")
///             .with_syntax_example("smooth_it(2)")
///             .with_argument("arg1", "The int32 number to smooth by")
///             .build()
///             .unwrap()
///     })
/// }
///
/// /// Implement the WindowUDFImpl trait for SmoothIt
/// impl WindowUDFImpl for SmoothIt {
///    fn as_any(&self) -> &dyn Any { self }
///    fn name(&self) -> &str { "smooth_it" }
///    fn signature(&self) -> &Signature { &self.signature }
///    // The actual implementation would smooth the window
///    fn partition_evaluator(
///        &self,
///        _partition_evaluator_args: PartitionEvaluatorArgs,
///    ) -> Result<Box<dyn PartitionEvaluator>> {
///        unimplemented!()
///    }
///    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<Field> {
///      if let Some(DataType::Int32) = field_args.get_input_type(0) {
///        Ok(Field::new(field_args.name(), DataType::Int32, false))
///      } else {
///        plan_err!("smooth_it only accepts Int32 arguments")
///      }
///    }
///    fn documentation(&self) -> Option<&Documentation> {
///      Some(get_doc())
///    }
/// }
///
/// // Create a new WindowUDF from the implementation
/// let smooth_it = WindowUDF::from(SmoothIt::new());
///
/// // Call the function `add_one(col)`
/// // smooth_it(speed) OVER (PARTITION BY car ORDER BY time ASC)
/// let expr = smooth_it.call(vec![col("speed")])
///     .partition_by(vec![col("car")])
///     .order_by(vec![col("time").sort(true, true)])
///     .window_frame(WindowFrame::new(None))
///     .build()
///     .unwrap();
/// ```
pub trait WindowUDFImpl: Debug + Send + Sync {
    // Note: When adding any methods (with default implementations), remember to add them also
    // into the AliasedWindowUDFImpl below!

    /// Returns this object as an [`Any`] trait object
    fn as_any(&self) -> &dyn Any;

    /// Returns this function's name
    fn name(&self) -> &str;

    /// Returns the function's [`Signature`] for information about what input
    /// types are accepted and the function's Volatility.
    fn signature(&self) -> &Signature;

    /// Returns the expressions that are passed to the [`PartitionEvaluator`].
    fn expressions(&self, expr_args: ExpressionArgs) -> Vec<Arc<dyn PhysicalExpr>> {
        expr_args
            .input_exprs()
            .first()
            .map_or(vec![], |expr| vec![Arc::clone(expr)])
    }

    /// Invoke the function, returning the [`PartitionEvaluator`] instance
    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>>;

    /// Returns any aliases (alternate names) for this function.
    ///
    /// Note: `aliases` should only include names other than [`Self::name`].
    /// Defaults to `[]` (no aliases)
    fn aliases(&self) -> &[String] {
        &[]
    }

    /// Optionally apply per-UDWF simplification / rewrite rules.
    ///
    /// This can be used to apply function specific simplification rules during
    /// optimization. The default implementation does nothing.
    ///
    /// Note that DataFusion handles simplifying arguments and  "constant
    /// folding" (replacing a function call with constant arguments such as
    /// `my_add(1,2) --> 3` ). Thus, there is no need to implement such
    /// optimizations manually for specific UDFs.
    ///
    /// Example:
    /// [`simplify_udwf_expression.rs`]: <https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/simplify_udwf_expression.rs>
    ///
    /// # Returns
    /// [None] if simplify is not defined or,
    ///
    /// Or, a closure with two arguments:
    /// * 'window_function': [crate::expr::WindowFunction] for which simplified has been invoked
    /// * 'info': [crate::simplify::SimplifyInfo]
    fn simplify(&self) -> Option<WindowFunctionSimplification> {
        None
    }

    /// Return true if this window UDF is equal to the other.
    ///
    /// Allows customizing the equality of window UDFs.
    /// Must be consistent with [`Self::hash_value`] and follow the same rules as [`Eq`]:
    ///
    /// - reflexive: `a.equals(a)`;
    /// - symmetric: `a.equals(b)` implies `b.equals(a)`;
    /// - transitive: `a.equals(b)` and `b.equals(c)` implies `a.equals(c)`.
    ///
    /// By default, compares [`Self::name`] and [`Self::signature`].
    fn equals(&self, other: &dyn WindowUDFImpl) -> bool {
        self.name() == other.name() && self.signature() == other.signature()
    }

    /// Returns a hash value for this window UDF.
    ///
    /// Allows customizing the hash code of window UDFs. Similarly to [`Hash`] and [`Eq`],
    /// if [`Self::equals`] returns true for two UDFs, their `hash_value`s must be the same.
    ///
    /// By default, hashes [`Self::name`] and [`Self::signature`].
    fn hash_value(&self) -> u64 {
        let hasher = &mut DefaultHasher::new();
        self.name().hash(hasher);
        self.signature().hash(hasher);
        hasher.finish()
    }

    /// The [`Field`] of the final result of evaluating this window function.
    ///
    /// Call `field_args.name()` to get the fully qualified name for defining
    /// the [`Field`]. For a complete example see the implementation in the
    /// [Basic Example](WindowUDFImpl#basic-example) section.
    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<Field>;

    /// Allows the window UDF to define a custom result ordering.
    ///
    /// By default, a window UDF doesn't introduce an ordering.
    /// But when specified by a window UDF this is used to update
    /// ordering equivalences.
    fn sort_options(&self) -> Option<SortOptions> {
        None
    }

    /// Coerce arguments of a function call to types that the function can evaluate.
    ///
    /// This function is only called if [`WindowUDFImpl::signature`] returns [`crate::TypeSignature::UserDefined`]. Most
    /// UDWFs should return one of the other variants of `TypeSignature` which handle common
    /// cases
    ///
    /// See the [type coercion module](crate::type_coercion)
    /// documentation for more details on type coercion
    ///
    /// For example, if your function requires a floating point arguments, but the user calls
    /// it like `my_func(1::int)` (aka with `1` as an integer), coerce_types could return `[DataType::Float64]`
    /// to ensure the argument was cast to `1::double`
    ///
    /// # Parameters
    /// * `arg_types`: The argument types of the arguments  this function with
    ///
    /// # Return value
    /// A Vec the same length as `arg_types`. DataFusion will `CAST` the function call
    /// arguments to these specific types.
    fn coerce_types(&self, _arg_types: &[DataType]) -> Result<Vec<DataType>> {
        not_impl_err!("Function {} does not implement coerce_types", self.name())
    }

    /// Allows customizing the behavior of the user-defined window
    /// function when it is evaluated in reverse order.
    fn reverse_expr(&self) -> ReversedUDWF {
        ReversedUDWF::NotSupported
    }

    /// Returns the documentation for this Window UDF.
    ///
    /// Documentation can be accessed programmatically as well as
    /// generating publicly facing documentation.
    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

pub enum ReversedUDWF {
    /// The result of evaluating the user-defined window function
    /// remains identical when reversed.
    Identical,
    /// A window function which does not support evaluating the result
    /// in reverse order.
    NotSupported,
    /// Customize the user-defined window function for evaluating the
    /// result in reverse order.
    Reversed(Arc<WindowUDF>),
}

impl PartialEq for dyn WindowUDFImpl {
    fn eq(&self, other: &Self) -> bool {
        self.equals(other)
    }
}

impl PartialOrd for dyn WindowUDFImpl {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.name().partial_cmp(other.name()) {
            Some(Ordering::Equal) => self.signature().partial_cmp(other.signature()),
            cmp => cmp,
        }
    }
}

/// WindowUDF that adds an alias to the underlying function. It is better to
/// implement [`WindowUDFImpl`], which supports aliases, directly if possible.
#[derive(Debug)]
struct AliasedWindowUDFImpl {
    inner: Arc<dyn WindowUDFImpl>,
    aliases: Vec<String>,
}

impl AliasedWindowUDFImpl {
    pub fn new(
        inner: Arc<dyn WindowUDFImpl>,
        new_aliases: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        let mut aliases = inner.aliases().to_vec();
        aliases.extend(new_aliases.into_iter().map(|s| s.to_string()));

        Self { inner, aliases }
    }
}

impl WindowUDFImpl for AliasedWindowUDFImpl {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn expressions(&self, expr_args: ExpressionArgs) -> Vec<Arc<dyn PhysicalExpr>> {
        expr_args
            .input_exprs()
            .first()
            .map_or(vec![], |expr| vec![Arc::clone(expr)])
    }

    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        self.inner.partition_evaluator(partition_evaluator_args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn simplify(&self) -> Option<WindowFunctionSimplification> {
        self.inner.simplify()
    }

    fn equals(&self, other: &dyn WindowUDFImpl) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<AliasedWindowUDFImpl>() {
            self.inner.equals(other.inner.as_ref()) && self.aliases == other.aliases
        } else {
            false
        }
    }

    fn hash_value(&self) -> u64 {
        let hasher = &mut DefaultHasher::new();
        self.inner.hash_value().hash(hasher);
        self.aliases.hash(hasher);
        hasher.finish()
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<Field> {
        self.inner.field(field_args)
    }

    fn sort_options(&self) -> Option<SortOptions> {
        self.inner.sort_options()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

// Window UDF doc sections for use in public documentation
pub mod window_doc_sections {
    use crate::DocSection;

    pub fn doc_sections() -> Vec<DocSection> {
        vec![
            DOC_SECTION_AGGREGATE,
            DOC_SECTION_RANKING,
            DOC_SECTION_ANALYTICAL,
        ]
    }

    pub const DOC_SECTION_AGGREGATE: DocSection = DocSection {
        include: true,
        label: "Aggregate Functions",
        description: Some("All aggregate functions can be used as window functions."),
    };

    pub const DOC_SECTION_RANKING: DocSection = DocSection {
        include: true,
        label: "Ranking Functions",
        description: None,
    };

    pub const DOC_SECTION_ANALYTICAL: DocSection = DocSection {
        include: true,
        label: "Analytical Functions",
        description: None,
    };
}

#[cfg(test)]
mod test {
    use crate::{PartitionEvaluator, WindowUDF, WindowUDFImpl};
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::Result;
    use datafusion_expr_common::signature::{Signature, Volatility};
    use datafusion_functions_window_common::field::WindowUDFFieldArgs;
    use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
    use std::any::Any;
    use std::cmp::Ordering;

    #[derive(Debug, Clone)]
    struct AWindowUDF {
        signature: Signature,
    }

    impl AWindowUDF {
        fn new() -> Self {
            Self {
                signature: Signature::uniform(
                    1,
                    vec![DataType::Int32],
                    Volatility::Immutable,
                ),
            }
        }
    }

    /// Implement the WindowUDFImpl trait for AddOne
    impl WindowUDFImpl for AWindowUDF {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn name(&self) -> &str {
            "a"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn partition_evaluator(
            &self,
            _partition_evaluator_args: PartitionEvaluatorArgs,
        ) -> Result<Box<dyn PartitionEvaluator>> {
            unimplemented!()
        }
        fn field(&self, _field_args: WindowUDFFieldArgs) -> Result<Field> {
            unimplemented!()
        }
    }

    #[derive(Debug, Clone)]
    struct BWindowUDF {
        signature: Signature,
    }

    impl BWindowUDF {
        fn new() -> Self {
            Self {
                signature: Signature::uniform(
                    1,
                    vec![DataType::Int32],
                    Volatility::Immutable,
                ),
            }
        }
    }

    /// Implement the WindowUDFImpl trait for AddOne
    impl WindowUDFImpl for BWindowUDF {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn name(&self) -> &str {
            "b"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn partition_evaluator(
            &self,
            _partition_evaluator_args: PartitionEvaluatorArgs,
        ) -> Result<Box<dyn PartitionEvaluator>> {
            unimplemented!()
        }
        fn field(&self, _field_args: WindowUDFFieldArgs) -> Result<Field> {
            unimplemented!()
        }
    }

    #[test]
    fn test_partial_ord() {
        let a1 = WindowUDF::from(AWindowUDF::new());
        let a2 = WindowUDF::from(AWindowUDF::new());
        assert_eq!(a1.partial_cmp(&a2), Some(Ordering::Equal));

        let b1 = WindowUDF::from(BWindowUDF::new());
        assert!(a1 < b1);
        assert!(!(a1 == b1));
    }
}
