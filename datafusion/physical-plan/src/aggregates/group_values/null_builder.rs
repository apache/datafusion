// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::NullBufferBuilder;
use arrow::buffer::NullBuffer;
use datafusion_common::Result;
use datafusion_expr::GroupSelection;

/// Helper methods for NullBufferBuilder that are used in Group By columns
pub(crate) trait NullBufferBuilderExt {
    fn empty() -> Self;

    /// Return true if the row at index `row` is null
    fn is_null(&self, row: usize) -> bool;

    /// Returns a null buffer for `selection` without changing this builder.
    fn build_preserving(
        &self,
        selection: GroupSelection<'_>,
    ) -> Result<Option<NullBuffer>>;

    /// Returns a NullBuffer representing the first `n` rows accumulated so far
    /// shifting any remaining down by `n`
    fn take_n(&mut self, n: usize) -> Option<NullBuffer>;

    /// Returns true if this builder might have any nulls
    ///
    /// This is guaranteed to be true if there are nulls
    /// but may be true even if there are no nulls
    fn might_have_nulls(&self) -> bool;
}

impl NullBufferBuilderExt for NullBufferBuilder {
    fn empty() -> Self {
        Self::new(0)
    }

    fn is_null(&self, row: usize) -> bool {
        !self.is_valid(row)
    }

    fn build_preserving(
        &self,
        selection: GroupSelection<'_>,
    ) -> Result<Option<NullBuffer>> {
        selection.validate_num_groups(self.len())?;
        if self.as_slice().is_none() {
            return Ok(None);
        }

        let mut selected = NullBufferBuilder::new(selection.len());
        for index in selection.iter() {
            selected.append(self.is_valid(index));
        }
        Ok(selected.finish())
    }

    fn take_n(&mut self, n: usize) -> Option<NullBuffer> {
        // Copy over the values at  n..len-1 values to the start of a
        // new builder and leave it in self
        //
        // TODO: it would be great to use something like `set_bits` from arrow here.
        let mut new_builder = NullBufferBuilder::new(self.len());
        for i in n..self.len() {
            new_builder.append(self.is_valid(i));
        }
        std::mem::swap(&mut new_builder, self);

        // take only first n values from the original builder
        new_builder.truncate(n);
        new_builder.finish()
    }

    fn might_have_nulls(&self) -> bool {
        self.as_slice().is_some()
    }
}
