// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

#![cfg_attr(test, allow(clippy::needless_pass_by_value))]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![cfg_attr(not(test), deny(clippy::clone_on_ref_ptr))]

pub mod generate_series;

use datafusion_catalog::TableFunction;
use std::sync::Arc;

/// Returns all default table functions
pub fn all_default_table_functions() -> Vec<Arc<TableFunction>> {
    vec![generate_series(), range()]
}

/// Creates a singleton instance of a table function
/// - `$module`: A struct implementing `TableFunctionImpl` to create the function from
/// - `$name`: The name to give to the created function
/// - `$func_name`: The name of the function to be called
///   This is used to ensure creating the list of `TableFunction` only happens once.
#[macro_export]
macro_rules! create_udtf_function {
    ($module:expr, $func_name:ident, $name:expr) => {
        pub fn $func_name() -> Arc<TableFunction> {
            static INSTANCE: std::sync::LazyLock<Arc<TableFunction>> =
                std::sync::LazyLock::new(|| {
                    std::sync::Arc::new(TableFunction::new(
                        $name.to_string(),
                        Arc::new($module),
                    ))
                });
            std::sync::Arc::clone(&INSTANCE)
        }
    };
}

create_udtf_function!(
    generate_series::GenerateSeriesFunc {},
    generate_series,
    "generate_series"
);
create_udtf_function!(generate_series::RangeFunc {}, range, "range");
