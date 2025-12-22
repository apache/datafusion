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
#![deny(clippy::allow_attributes)]

extern crate proc_macro;

mod metric_doc_impl;
mod user_doc_impl;

use proc_macro::TokenStream;

/// This procedural macro is intended to parse a rust custom attribute and create user documentation
/// from it by constructing a `DocumentBuilder()` automatically. The `Documentation` can be
/// retrieved from the `documentation()` method
/// declared on `AggregateUDF`, `WindowUDFImpl`, `ScalarUDFImpl` traits.
/// For `doc_section`, this macro will try to find corresponding predefined `DocSection` by label field
/// Predefined `DocSection` can be found in datafusion/expr/src/udf.rs
/// Example:
/// ```ignore
/// #[user_doc(
///     doc_section(label = "Time and Date Functions"),
///     description = r"Converts a value to a date (`YYYY-MM-DD`).",
///     syntax_example = "to_date('2017-05-31', '%Y-%m-%d')",
///     sql_example = r#"```sql
/// > select to_date('2023-01-31');
/// +-----------------------------+
/// | to_date(Utf8(\"2023-01-31\")) |
/// +-----------------------------+
/// | 2023-01-31                  |
/// +-----------------------------+
/// ```"#,
///     standard_argument(name = "expression", prefix = "String"),
///     argument(
///         name = "format_n",
///         description = r"Optional [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) strings to use to parse the expression. Formats will be tried in the order
///   they appear with the first successful one being returned. If none of the formats successfully parse the expression
///   an error will be returned."
///    )
/// )]
/// #[derive(Debug)]
/// pub struct ToDateFunc {
///     signature: Signature,
/// }
/// ```
/// will generate the following code
/// ```ignore
/// pub struct ToDateFunc {
///     signature: Signature,
/// }
/// impl ToDateFunc {
///     fn doc(&self) -> Option<&datafusion_doc::Documentation> {
///         static DOCUMENTATION: std::sync::LazyLock<
///             datafusion_doc::Documentation,
///         > = std::sync::LazyLock::new(|| {
///             datafusion_doc::Documentation::builder(
///                     datafusion_doc::DocSection {
///                         include: true,
///                         label: "Time and Date Functions",
///                         description: None,
///                     },
///                     r"Converts a value to a date (`YYYY-MM-DD`).".to_string(),
///                     "to_date('2017-05-31', '%Y-%m-%d')".to_string(),
///                 )
///                 .with_sql_example(
///                     r#"```sql
/// > select to_date('2023-01-31');
/// +-----------------------------+
/// | to_date(Utf8(\"2023-01-31\")) |
/// +-----------------------------+
/// | 2023-01-31                  |
/// +-----------------------------+
/// ```"#,
///                 )
///                 .with_standard_argument("expression", "String".into())
///                 .with_argument(
///                     "format_n",
///                     r"Optional [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) strings to use to parse the expression. Formats will be tried in the order
/// they appear with the first successful one being returned. If none of the formats successfully parse the expression
/// an error will be returned.",
///                 )
///                 .build()
///         });
///         Some(&DOCUMENTATION)
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn user_doc(args: TokenStream, input: TokenStream) -> TokenStream {
    user_doc_impl::user_doc(args, input)
}

/// Helper attribute to register metrics structs or execs for documentation generation.
///
/// # Usage
///
/// ## On Metrics Structs
///
/// Use `#[metric_doc]` (no arguments) for operator-specific metrics:
/// ```ignore
/// #[metric_doc]
/// struct FilterExecMetrics {
///     /// Common metrics for most operators
///     baseline_metrics: BaselineMetrics,
///     /// Selectivity of the filter
///     selectivity: RatioMetrics,
/// }
/// ```
///
/// Use `#[metric_doc(common)]` for reusable metrics shared across operators:
/// ```ignore
/// #[metric_doc(common)]
/// pub struct BaselineMetrics {
///     /// Amount of time the operator was actively using the CPU
///     elapsed_compute: Time,
///     /// Total output rows
///     output_rows: Count,
/// }
/// ```
///
/// ## On Exec Structs
///
/// Reference a single metrics struct:
/// ```ignore
/// #[metric_doc(FilterExecMetrics)]
/// pub struct FilterExec { /* ... */ }
/// ```
///
/// Reference multiple metrics structs (for operators with multiple metric groups):
/// ```ignore
/// #[metric_doc(MetricsGroupA, MetricsGroupB)]
/// pub struct UserDefinedExec { /* ... */ }
/// ```
///
/// ## Cross-File Usage
///
/// Metrics structs can be defined in different files/modules from the exec.
/// The macro resolves references at compile time, so metrics can live anywhere
/// as long as they are in scope where the exec is defined:
/// ```ignore
/// // In metrics/groups.rs
/// #[metric_doc]
/// pub struct MetricsGroupA {
///     /// Time spent in phase A
///     phase_a_time: Time,
/// }
///
/// #[metric_doc]
/// pub struct MetricsGroupB {
///     /// Time spent in phase B
///     phase_b_time: Time,
/// }
///
/// // In exec/user_defined.rs
/// use crate::metrics::groups::{MetricsGroupA, MetricsGroupB};
///
/// #[metric_doc(MetricsGroupA, MetricsGroupB)]
/// pub struct UserDefinedExec { /* ... */ }
/// ```
#[proc_macro_attribute]
pub fn metric_doc(args: TokenStream, input: TokenStream) -> TokenStream {
    metric_doc_impl::metric_doc(&args, input)
}

/// Derive macro used on `*Metrics` structs to expose doc comments for documentation generation.
#[proc_macro_derive(DocumentedMetrics, attributes(metric_doc, metric_doc_attr))]
pub fn derive_documented_metrics(input: TokenStream) -> TokenStream {
    metric_doc_impl::derive_documented_metrics(input)
}

/// Derive macro used on `*Exec` structs to link them to the metrics they expose.
#[proc_macro_derive(DocumentedExec, attributes(metric_doc, metric_doc_attr))]
pub fn derive_documented_exec(input: TokenStream) -> TokenStream {
    metric_doc_impl::derive_documented_exec(input)
}
