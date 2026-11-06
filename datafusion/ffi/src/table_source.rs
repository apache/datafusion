// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_expr::{TableProviderFilterPushDown, TableType};

/// FFI safe version of [`TableProviderFilterPushDown`].
#[expect(non_camel_case_types)]
#[repr(u8)]
pub enum FFI_TableProviderFilterPushDown {
    Unsupported,
    Inexact,
    Exact,
}

impl From<&FFI_TableProviderFilterPushDown> for TableProviderFilterPushDown {
    fn from(value: &FFI_TableProviderFilterPushDown) -> Self {
        match value {
            FFI_TableProviderFilterPushDown::Unsupported => {
                TableProviderFilterPushDown::Unsupported
            }
            FFI_TableProviderFilterPushDown::Inexact => {
                TableProviderFilterPushDown::Inexact
            }
            FFI_TableProviderFilterPushDown::Exact => TableProviderFilterPushDown::Exact,
        }
    }
}

impl From<&TableProviderFilterPushDown> for FFI_TableProviderFilterPushDown {
    fn from(value: &TableProviderFilterPushDown) -> Self {
        match value {
            TableProviderFilterPushDown::Unsupported => {
                FFI_TableProviderFilterPushDown::Unsupported
            }
            TableProviderFilterPushDown::Inexact => {
                FFI_TableProviderFilterPushDown::Inexact
            }
            TableProviderFilterPushDown::Exact => FFI_TableProviderFilterPushDown::Exact,
        }
    }
}

/// FFI safe version of [`TableType`].
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FFI_TableType {
    Base,
    View,
    Temporary,
}

impl From<FFI_TableType> for TableType {
    fn from(value: FFI_TableType) -> Self {
        match value {
            FFI_TableType::Base => TableType::Base,
            FFI_TableType::View => TableType::View,
            FFI_TableType::Temporary => TableType::Temporary,
        }
    }
}

impl From<TableType> for FFI_TableType {
    fn from(value: TableType) -> Self {
        match value {
            TableType::Base => FFI_TableType::Base,
            TableType::View => FFI_TableType::View,
            TableType::Temporary => FFI_TableType::Temporary,
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::error::Result;

    use super::*;

    fn round_trip_filter_pushdown(pushdown: TableProviderFilterPushDown) -> Result<()> {
        let ffi_pushdown: FFI_TableProviderFilterPushDown = (&pushdown).into();
        let round_trip: TableProviderFilterPushDown = (&ffi_pushdown).into();

        assert_eq!(pushdown, round_trip);
        Ok(())
    }

    #[test]
    fn round_trip_all_filter_pushdowns() -> Result<()> {
        round_trip_filter_pushdown(TableProviderFilterPushDown::Exact)?;
        round_trip_filter_pushdown(TableProviderFilterPushDown::Inexact)?;
        round_trip_filter_pushdown(TableProviderFilterPushDown::Unsupported)?;

        Ok(())
    }

    fn round_trip_table_type(table_type: TableType) -> Result<()> {
        let ffi_type: FFI_TableType = table_type.into();
        let round_trip_type: TableType = ffi_type.into();

        assert_eq!(table_type, round_trip_type);
        Ok(())
    }

    #[test]
    fn test_round_all_trip_table_type() -> Result<()> {
        round_trip_table_type(TableType::Base)?;
        round_trip_table_type(TableType::Temporary)?;
        round_trip_table_type(TableType::View)?;

        Ok(())
    }
}
