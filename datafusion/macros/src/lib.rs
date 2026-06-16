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

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

extern crate proc_macro;

mod metrics_doc_impl;
mod user_doc_impl;

use proc_macro::TokenStream;

/// Attribute macro that generates an inherent `fn doc(&self)` on the
/// annotated UDF struct, returning `Some(&'static Documentation)`.
///
/// See [`user_doc_impl::user_doc`] for documentation and examples.
#[proc_macro_attribute]
pub fn user_doc(args: TokenStream, input: TokenStream) -> TokenStream {
    user_doc_impl::user_doc(args, input)
}

/// Attribute macro that generates an inherent `fn metrics_doc()` on the
/// annotated struct (typically an `ExecutionPlan`), returning a
/// `&'static MetricsDocumentation`.
///
/// # Arguments
///
/// - `position = "common" | "operator"` — defaults to `"operator"`.
/// - `description = "..."` — optional markdown description.
/// - `metric(name = "...", description = "...")` — repeatable, one per metric.
///
/// # Example
///
/// ```ignore
/// use datafusion_macros::metrics_doc;
///
/// #[metrics_doc(
///     position = "operator",
///     metric(
///         name = "selectivity",
///         description = "Selectivity of the filter, calculated as output_rows / input_rows",
///     ),
/// )]
/// pub struct FilterExec { /* ... */ }
/// ```
///
/// generates roughly:
///
/// ```ignore
/// pub struct FilterExec { /* ... */ }
///
/// impl FilterExec {
///     pub fn metrics_doc() -> &'static datafusion_doc::MetricsDocumentation {
///         static DOC: std::sync::LazyLock<datafusion_doc::MetricsDocumentation> =
///             std::sync::LazyLock::new(|| {
///                 datafusion_doc::MetricsDocumentation::builder(
///                     "FilterExec",
///                     datafusion_doc::MetricPosition::Operator,
///                 )
///                 .with_metric(
///                     "selectivity",
///                     "Selectivity of the filter, calculated as output_rows / input_rows",
///                 )
///                 .build()
///             });
///         &DOC
///     }
/// }
/// ```
///
/// The trait method `ExecutionPlan::metrics_documentation` is *not*
/// generated — operators write it by hand, delegating to
/// `Self::metrics_doc()` (mirroring how `ScalarUDFImpl::documentation`
/// delegates to `self.doc()`).
#[proc_macro_attribute]
pub fn metrics_doc(args: TokenStream, input: TokenStream) -> TokenStream {
    metrics_doc_impl::metrics_doc(args, input)
}
