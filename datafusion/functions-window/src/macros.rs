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

//! Convenience macros for defining a user-defined window function
//! and associated expression API (fluent style).
//!
//! See [`define_udwf_and_expr!`] for usage examples.
//!
//! [`define_udwf_and_expr!`]: crate::define_udwf_and_expr!

/// Lazily initializes a user-defined window function exactly once
/// when called concurrently. Repeated calls return a reference to the
/// same instance.
///
/// # Parameters
///
/// * `$UDWF`: The struct which defines the [`Signature`](datafusion_expr::Signature)
///     of the user-defined window function.
/// * `$OUT_FN_NAME`: The basename to generate a unique function name like
///     `$OUT_FN_NAME_udwf`.
/// * `$DOC`: Doc comments for UDWF.
/// * (optional) `$CTOR`: Pass a custom constructor. When omitted it
///     automatically resolves to `$UDWF::default()`.
///
/// # Example
///
/// ```
/// # use std::any::Any;
/// # use datafusion_common::arrow::datatypes::{DataType, Field};
/// # use datafusion_expr::{PartitionEvaluator, Signature, Volatility, WindowUDFImpl};
/// #
/// # use datafusion_functions_window_common::field::WindowUDFFieldArgs;
/// # use datafusion_functions_window::get_or_init_udwf;
/// #
/// /// Defines the `simple_udwf()` user-defined window function.
/// get_or_init_udwf!(
///     SimpleUDWF,
///     simple,
///     "Simple user-defined window function doc comment."
/// );
/// #
/// # assert_eq!(simple_udwf().name(), "simple_user_defined_window_function");
/// #
/// #  #[derive(Debug)]
/// #  struct SimpleUDWF {
/// #      signature: Signature,
/// #  }
/// #
/// #  impl Default for SimpleUDWF {
/// #      fn default() -> Self {
/// #          Self {
/// #             signature: Signature::any(0, Volatility::Immutable),
/// #          }
/// #      }
/// #  }
/// #
/// #  impl WindowUDFImpl for SimpleUDWF {
/// #      fn as_any(&self) -> &dyn Any {
/// #          self
/// #      }
/// #      fn name(&self) -> &str {
/// #          "simple_user_defined_window_function"
/// #      }
/// #      fn signature(&self) -> &Signature {
/// #          &self.signature
/// #      }
/// #      fn partition_evaluator(
/// #          &self,
/// #      ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
/// #          unimplemented!()
/// #      }
/// #      fn field(&self, field_args: WindowUDFFieldArgs) -> datafusion_common::Result<Field> {
/// #          Ok(Field::new(field_args.name(), DataType::Int64, false))
/// #      }
/// #  }
/// #
/// ```
#[macro_export]
macro_rules! get_or_init_udwf {
    ($UDWF:ident, $OUT_FN_NAME:ident, $DOC:expr) => {
        get_or_init_udwf!($UDWF, $OUT_FN_NAME, $DOC, $UDWF::default);
    };

    ($UDWF:ident, $OUT_FN_NAME:ident, $DOC:expr, $CTOR:path) => {
        paste::paste! {
            #[doc = concat!(" Singleton instance of [`", stringify!($OUT_FN_NAME), "`], ensures the user-defined")]
            #[doc = concat!(" window function is only created once.")]
            #[allow(non_upper_case_globals)]
            static [<STATIC_ $UDWF>]: std::sync::OnceLock<std::sync::Arc<datafusion_expr::WindowUDF>> =
                std::sync::OnceLock::new();

            #[doc = concat!(" Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`", stringify!($OUT_FN_NAME), "`].")]
            #[doc = ""]
            #[doc = concat!(" ", $DOC)]
            pub fn [<$OUT_FN_NAME _udwf>]() -> std::sync::Arc<datafusion_expr::WindowUDF> {
                [<STATIC_ $UDWF>]
                    .get_or_init(|| {
                        std::sync::Arc::new(datafusion_expr::WindowUDF::from($CTOR()))
                    })
                    .clone()
            }
        }
    };
}

/// Create a [`WindowFunction`] expression that exposes a fluent API
/// which you can use to build more complex expressions.
///
/// [`WindowFunction`]: datafusion_expr::Expr::WindowFunction
///
/// # Parameters
///
/// * `$UDWF`: The struct which defines the [`Signature`] of the
///     user-defined window function.
/// * `$OUT_FN_NAME`: The basename to generate a unique function name like
///     `$OUT_FN_NAME_udwf`.
/// * `$DOC`: Doc comments for UDWF.
/// * (optional) `[$($PARAM:ident),+]`: An array of 1 or more parameters
///     for the generated function. The type of parameters is [`Expr`].
///     When omitted this creates a function with zero parameters.
///
/// [`Signature`]: datafusion_expr::Signature
/// [`Expr`]: datafusion_expr::Expr
///
/// # Example
///
/// 1. With Zero Parameters
/// ```
/// # use std::any::Any;
/// # use datafusion_common::arrow::datatypes::{DataType, Field};
/// # use datafusion_expr::{PartitionEvaluator, Signature, Volatility, WindowUDFImpl};
/// # use datafusion_functions_window::{create_udwf_expr, get_or_init_udwf};
/// # use datafusion_functions_window_common::field::WindowUDFFieldArgs;
/// # get_or_init_udwf!(
/// #     RowNumber,
/// #     row_number,
/// #     "Returns a unique row number for each row in window partition beginning at 1."
/// # );
/// /// Creates `row_number()` API which has zero parameters:
/// ///
/// ///     ```
/// ///     /// Returns a unique row number for each row in window partition
/// ///     /// beginning at 1.
/// ///     pub fn row_number() -> datafusion_expr::Expr {
/// ///        row_number_udwf().call(vec![])
/// ///     }
/// ///     ```
/// create_udwf_expr!(
///     RowNumber,
///     row_number,
///     "Returns a unique row number for each row in window partition beginning at 1."
/// );
/// #
/// # assert_eq!(
/// #     row_number().name_for_alias().unwrap(),
/// #     "row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
/// # );
/// #
/// # #[derive(Debug)]
/// # struct RowNumber {
/// #     signature: Signature,
/// # }
/// # impl Default for RowNumber {
/// #     fn default() -> Self {
/// #         Self {
/// #             signature: Signature::any(0, Volatility::Immutable),
/// #         }
/// #     }
/// # }
/// # impl WindowUDFImpl for RowNumber {
/// #     fn as_any(&self) -> &dyn Any {
/// #         self
/// #     }
/// #     fn name(&self) -> &str {
/// #         "row_number"
/// #     }
/// #     fn signature(&self) -> &Signature {
/// #         &self.signature
/// #     }
/// #     fn partition_evaluator(
/// #         &self,
/// #     ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
/// #         unimplemented!()
/// #     }
/// #     fn field(&self, field_args: WindowUDFFieldArgs) -> datafusion_common::Result<Field> {
/// #         Ok(Field::new(field_args.name(), DataType::UInt64, false))
/// #     }
/// # }
/// ```
///
/// 2. With Multiple Parameters
/// ```
/// # use std::any::Any;
/// #
/// # use datafusion_expr::{
/// #     PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDFImpl,
/// # };
/// #
/// # use datafusion_functions_window::{create_udwf_expr, get_or_init_udwf};
/// # use datafusion_functions_window_common::field::WindowUDFFieldArgs;
/// #
/// # use datafusion_common::arrow::datatypes::Field;
/// # use datafusion_common::ScalarValue;
/// # use datafusion_expr::{col, lit};
/// #
/// # get_or_init_udwf!(Lead, lead, "user-defined window function");
/// #
/// /// Creates `lead(expr, offset, default)` with 3 parameters:
/// ///
/// ///     ```
/// ///     /// Returns a value evaluated at the row that is offset rows
/// ///     /// after the current row within the partition.
/// ///     pub fn lead(
/// ///         expr: datafusion_expr::Expr,
/// ///         offset: datafusion_expr::Expr,
/// ///         default: datafusion_expr::Expr,
/// ///     ) -> datafusion_expr::Expr {
/// ///         lead_udwf().call(vec![expr, offset, default])
/// ///     }
/// ///     ```
/// create_udwf_expr!(
///     Lead,
///     lead,
///     [expr, offset, default],
///     "Returns a value evaluated at the row that is offset rows after the current row within the partition."
/// );
/// #
/// # assert_eq!(
/// #     lead(col("a"), lit(1i64), lit(ScalarValue::Null))
/// #         .name_for_alias()
/// #         .unwrap(),
/// #     "lead(a,Int64(1),NULL) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
/// # );
/// #
/// # #[derive(Debug)]
/// # struct Lead {
/// #     signature: Signature,
/// # }
/// #
/// # impl Default for Lead {
/// #     fn default() -> Self {
/// #         Self {
/// #             signature: Signature::one_of(
/// #                 vec![
/// #                     TypeSignature::Any(1),
/// #                     TypeSignature::Any(2),
/// #                     TypeSignature::Any(3),
/// #                 ],
/// #                 Volatility::Immutable,
/// #             ),
/// #         }
/// #     }
/// # }
/// #
/// # impl WindowUDFImpl for Lead {
/// #     fn as_any(&self) -> &dyn Any {
/// #         self
/// #     }
/// #     fn name(&self) -> &str {
/// #         "lead"
/// #     }
/// #     fn signature(&self) -> &Signature {
/// #         &self.signature
/// #     }
/// #     fn partition_evaluator(
/// #         &self,
/// #     ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
/// #         unimplemented!()
/// #     }
/// #     fn field(&self, field_args: WindowUDFFieldArgs) -> datafusion_common::Result<Field> {
/// #         Ok(Field::new(
/// #             field_args.name(),
/// #             field_args.get_input_type(0).unwrap(),
/// #             false,
/// #         ))
/// #     }
/// # }
/// ```
#[macro_export]
macro_rules! create_udwf_expr {
    // zero arguments
    ($UDWF:ident, $OUT_FN_NAME:ident, $DOC:expr) => {
        paste::paste! {
            #[doc = " Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for"]
            #[doc = concat!(" [`", stringify!($UDWF), "`] user-defined window function.")]
            #[doc = ""]
            #[doc = concat!(" ", $DOC)]
            pub fn $OUT_FN_NAME() -> datafusion_expr::Expr {
                [<$OUT_FN_NAME _udwf>]().call(vec![])
            }
       }
    };

    // 1 or more arguments
    ($UDWF:ident, $OUT_FN_NAME:ident, [$($PARAM:ident),+], $DOC:expr) => {
        paste::paste! {
            #[doc = " Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for"]
            #[doc = concat!(" [`", stringify!($UDWF), "`] user-defined window function.")]
            #[doc = ""]
            #[doc = concat!(" ", $DOC)]
            pub fn $OUT_FN_NAME(
                $($PARAM: datafusion_expr::Expr),+
            ) -> datafusion_expr::Expr {
                [<$OUT_FN_NAME _udwf>]()
                    .call(vec![$($PARAM),+])
            }
       }
    };
}

/// Defines a user-defined window function.
///
/// Combines [`get_or_init_udwf!`] and [`create_udwf_expr!`] into a
/// single macro for convenience.
///
/// # Arguments
///
/// * `$UDWF`: The struct which defines the [`Signature`] of the
///     user-defined window function.
/// * `$OUT_FN_NAME`: The basename to generate a unique function name like
///     `$OUT_FN_NAME_udwf`.
/// * (optional) `[$($PARAM:ident),+]`: An array of 1 or more parameters
///     for the generated function. The type of parameters is [`Expr`].
///     When omitted this creates a function with zero parameters.
/// * `$DOC`: Doc comments for UDWF.
/// * (optional) `$CTOR`: Pass a custom constructor. When omitted it
///     automatically resolves to `$UDWF::default()`.
///
/// [`Signature`]: datafusion_expr::Signature
/// [`Expr`]: datafusion_expr::Expr
///
/// # Usage
///
/// ## Expression API With Zero parameters
/// 1. Uses default constructor for UDWF.
///
/// ```
/// # use std::any::Any;
/// # use datafusion_common::arrow::datatypes::{DataType, Field};
/// # use datafusion_expr::{PartitionEvaluator, Signature, Volatility, WindowUDFImpl};
/// #
/// # use datafusion_functions_window_common::field::WindowUDFFieldArgs;
/// # use datafusion_functions_window::{define_udwf_and_expr, get_or_init_udwf, create_udwf_expr};
/// #
/// /// 1. Defines the `simple_udwf()` user-defined window function.
/// ///
/// /// 2. Defines the expression API:
/// ///     ```
/// ///     pub fn simple() -> datafusion_expr::Expr {
/// ///         simple_udwf().call(vec![])
/// ///     }
/// ///     ```
/// define_udwf_and_expr!(
///     SimpleUDWF,
///     simple,
///     "a simple user-defined window function"
/// );
/// #
/// # assert_eq!(simple_udwf().name(), "simple_user_defined_window_function");
/// #
/// #  #[derive(Debug)]
/// #  struct SimpleUDWF {
/// #      signature: Signature,
/// #  }
/// #
/// #  impl Default for SimpleUDWF {
/// #      fn default() -> Self {
/// #          Self {
/// #             signature: Signature::any(0, Volatility::Immutable),
/// #          }
/// #      }
/// #  }
/// #
/// #  impl WindowUDFImpl for SimpleUDWF {
/// #      fn as_any(&self) -> &dyn Any {
/// #          self
/// #      }
/// #      fn name(&self) -> &str {
/// #          "simple_user_defined_window_function"
/// #      }
/// #      fn signature(&self) -> &Signature {
/// #          &self.signature
/// #      }
/// #      fn partition_evaluator(
/// #          &self,
/// #      ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
/// #          unimplemented!()
/// #      }
/// #      fn field(&self, field_args: WindowUDFFieldArgs) -> datafusion_common::Result<Field> {
/// #          Ok(Field::new(field_args.name(), DataType::Int64, false))
/// #      }
/// #  }
/// #
/// ```
///
/// 2. Uses a custom constructor for UDWF.
///
/// ```
/// # use std::any::Any;
/// # use datafusion_common::arrow::datatypes::{DataType, Field};
/// # use datafusion_expr::{PartitionEvaluator, Signature, Volatility, WindowUDFImpl};
/// # use datafusion_functions_window::{create_udwf_expr, define_udwf_and_expr, get_or_init_udwf};
/// # use datafusion_functions_window_common::field::WindowUDFFieldArgs;
/// #
/// /// 1. Defines the `row_number_udwf()` user-defined window function.
/// ///
/// /// 2. Defines the expression API:
/// ///     ```
/// ///     pub fn row_number() -> datafusion_expr::Expr {
/// ///         row_number_udwf().call(vec![])
/// ///     }
/// ///     ```
/// define_udwf_and_expr!(
///     RowNumber,
///     row_number,
///     "Returns a unique row number for each row in window partition beginning at 1.",
///     RowNumber::new // <-- custom constructor
/// );
/// #
/// # assert_eq!(
/// #     row_number().name_for_alias().unwrap(),
/// #     "row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
/// # );
/// #
/// # #[derive(Debug)]
/// # struct RowNumber {
/// #     signature: Signature,
/// # }
/// # impl RowNumber {
/// #     fn new() -> Self {
/// #         Self {
/// #             signature: Signature::any(0, Volatility::Immutable),
/// #         }
/// #     }
/// # }
/// # impl WindowUDFImpl for RowNumber {
/// #     fn as_any(&self) -> &dyn Any {
/// #         self
/// #     }
/// #     fn name(&self) -> &str {
/// #         "row_number"
/// #     }
/// #     fn signature(&self) -> &Signature {
/// #         &self.signature
/// #     }
/// #     fn partition_evaluator(
/// #         &self,
/// #     ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
/// #         unimplemented!()
/// #     }
/// #     fn field(&self, field_args: WindowUDFFieldArgs) -> datafusion_common::Result<Field> {
/// #         Ok(Field::new(field_args.name(), DataType::UInt64, false))
/// #     }
/// # }
/// ```
///
/// ## Expression API With Multiple Parameters
/// 3. Uses default constructor for UDWF
///
/// ```
/// # use std::any::Any;
/// #
/// # use datafusion_expr::{
/// #     PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDFImpl,
/// # };
/// #
/// # use datafusion_functions_window::{create_udwf_expr, define_udwf_and_expr, get_or_init_udwf};
/// # use datafusion_functions_window_common::field::WindowUDFFieldArgs;
/// #
/// # use datafusion_common::arrow::datatypes::Field;
/// # use datafusion_common::ScalarValue;
/// # use datafusion_expr::{col, lit};
/// #
/// /// 1. Defines the `lead_udwf()` user-defined window function.
/// ///
/// /// 2. Defines the expression API:
/// ///     ```
/// ///     pub fn lead(
/// ///         expr: datafusion_expr::Expr,
/// ///         offset: datafusion_expr::Expr,
/// ///         default: datafusion_expr::Expr,
/// ///     ) -> datafusion_expr::Expr {
/// ///         lead_udwf().call(vec![expr, offset, default])
/// ///     }
/// ///     ```
/// define_udwf_and_expr!(
///     Lead,
///     lead,
///     [expr, offset, default],        // <- 3 parameters
///     "user-defined window function"
/// );
/// #
/// # assert_eq!(
/// #     lead(col("a"), lit(1i64), lit(ScalarValue::Null))
/// #         .name_for_alias()
/// #         .unwrap(),
/// #     "lead(a,Int64(1),NULL) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
/// # );
/// #
/// # #[derive(Debug)]
/// # struct Lead {
/// #     signature: Signature,
/// # }
/// #
/// # impl Default for Lead {
/// #     fn default() -> Self {
/// #         Self {
/// #             signature: Signature::one_of(
/// #                 vec![
/// #                     TypeSignature::Any(1),
/// #                     TypeSignature::Any(2),
/// #                     TypeSignature::Any(3),
/// #                 ],
/// #                 Volatility::Immutable,
/// #             ),
/// #         }
/// #     }
/// # }
/// #
/// # impl WindowUDFImpl for Lead {
/// #     fn as_any(&self) -> &dyn Any {
/// #         self
/// #     }
/// #     fn name(&self) -> &str {
/// #         "lead"
/// #     }
/// #     fn signature(&self) -> &Signature {
/// #         &self.signature
/// #     }
/// #     fn partition_evaluator(
/// #         &self,
/// #     ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
/// #         unimplemented!()
/// #     }
/// #     fn field(&self, field_args: WindowUDFFieldArgs) -> datafusion_common::Result<Field> {
/// #         Ok(Field::new(
/// #             field_args.name(),
/// #             field_args.get_input_type(0).unwrap(),
/// #             false,
/// #         ))
/// #     }
/// # }
/// ```
/// 4. Uses custom constructor for UDWF
///
/// ```
/// # use std::any::Any;
/// #
/// # use datafusion_expr::{
/// #     PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDFImpl,
/// # };
/// #
/// # use datafusion_functions_window::{create_udwf_expr, define_udwf_and_expr, get_or_init_udwf};
/// # use datafusion_functions_window_common::field::WindowUDFFieldArgs;
/// #
/// # use datafusion_common::arrow::datatypes::Field;
/// # use datafusion_common::ScalarValue;
/// # use datafusion_expr::{col, lit};
/// #
/// /// 1. Defines the `lead_udwf()` user-defined window function.
/// ///
/// /// 2. Defines the expression API:
/// ///     ```
/// ///     pub fn lead(
/// ///         expr: datafusion_expr::Expr,
/// ///         offset: datafusion_expr::Expr,
/// ///         default: datafusion_expr::Expr,
/// ///     ) -> datafusion_expr::Expr {
/// ///         lead_udwf().call(vec![expr, offset, default])
/// ///     }
/// ///     ```
/// define_udwf_and_expr!(
///     Lead,
///     lead,
///     [expr, offset, default],        // <- 3 parameters
///     "user-defined window function",
///     Lead::new                       // <- Custom constructor
/// );
/// #
/// # assert_eq!(
/// #     lead(col("a"), lit(1i64), lit(ScalarValue::Null))
/// #         .name_for_alias()
/// #         .unwrap(),
/// #     "lead(a,Int64(1),NULL) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
/// # );
/// #
/// # #[derive(Debug)]
/// # struct Lead {
/// #     signature: Signature,
/// # }
/// #
/// # impl Lead {
/// #     fn new() -> Self {
/// #         Self {
/// #             signature: Signature::one_of(
/// #                 vec![
/// #                     TypeSignature::Any(1),
/// #                     TypeSignature::Any(2),
/// #                     TypeSignature::Any(3),
/// #                 ],
/// #                 Volatility::Immutable,
/// #             ),
/// #         }
/// #     }
/// # }
/// #
/// # impl WindowUDFImpl for Lead {
/// #     fn as_any(&self) -> &dyn Any {
/// #         self
/// #     }
/// #     fn name(&self) -> &str {
/// #         "lead"
/// #     }
/// #     fn signature(&self) -> &Signature {
/// #         &self.signature
/// #     }
/// #     fn partition_evaluator(
/// #         &self,
/// #     ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
/// #         unimplemented!()
/// #     }
/// #     fn field(&self, field_args: WindowUDFFieldArgs) -> datafusion_common::Result<Field> {
/// #         Ok(Field::new(
/// #             field_args.name(),
/// #             field_args.get_input_type(0).unwrap(),
/// #             false,
/// #         ))
/// #     }
/// # }
/// ```
#[macro_export]
macro_rules! define_udwf_and_expr {
    // Defines UDWF with default constructor
    // Defines expression API with zero parameters
    ($UDWF:ident, $OUT_FN_NAME:ident, $DOC:expr) => {
        get_or_init_udwf!($UDWF, $OUT_FN_NAME, $DOC);
        create_udwf_expr!($UDWF, $OUT_FN_NAME, $DOC);
    };

    // Defines UDWF by passing a custom constructor
    // Defines expression API with zero parameters
    ($UDWF:ident, $OUT_FN_NAME:ident, $DOC:expr, $CTOR:path) => {
        get_or_init_udwf!($UDWF, $OUT_FN_NAME, $DOC, $CTOR);
        create_udwf_expr!($UDWF, $OUT_FN_NAME, $DOC);
    };

    // Defines UDWF with default constructor
    // Defines expression API with multiple parameters
    ($UDWF:ident, $OUT_FN_NAME:ident, [$($PARAM:ident),+], $DOC:expr) => {
        get_or_init_udwf!($UDWF, $OUT_FN_NAME, $DOC);
        create_udwf_expr!($UDWF, $OUT_FN_NAME, [$($PARAM),+], $DOC);
    };

    // Defines UDWF by passing a custom constructor
    // Defines expression API with multiple parameters
    ($UDWF:ident, $OUT_FN_NAME:ident, [$($PARAM:ident),+], $DOC:expr, $CTOR:path) => {
        get_or_init_udwf!($UDWF, $OUT_FN_NAME, $DOC, $CTOR);
        create_udwf_expr!($UDWF, $OUT_FN_NAME, [$($PARAM),+], $DOC);
    };
}
