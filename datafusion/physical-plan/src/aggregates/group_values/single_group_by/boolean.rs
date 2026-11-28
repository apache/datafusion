// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::aggregates::group_values::{GroupValues, MISSING_GROUP};

use arrow::array::{
    ArrayRef, AsArray as _, BooleanArray, BooleanBufferBuilder, NullBufferBuilder,
};
use datafusion_common::Result;
use datafusion_expr::{EmitTo, GroupSelection};
use std::{mem::size_of, sync::Arc};

#[derive(Debug)]
pub struct GroupValuesBoolean {
    false_group: Option<usize>,
    true_group: Option<usize>,
    null_group: Option<usize>,
}

impl GroupValuesBoolean {
    pub fn new() -> Self {
        Self {
            false_group: None,
            true_group: None,
            null_group: None,
        }
    }

    /// [`GroupValues::intern`] if `INSERT`, else [`GroupValues::lookup`].
    fn probe<const INSERT: bool>(
        &mut self,
        cols: &[ArrayRef],
        groups: &mut Vec<usize>,
    ) -> Result<()> {
        let array = cols[0].as_boolean();
        groups.clear();

        for value in array.iter() {
            let index = match value {
                Some(false) => {
                    if let Some(index) = self.false_group {
                        index
                    } else if !INSERT {
                        MISSING_GROUP
                    } else {
                        let index = self.len();
                        self.false_group = Some(index);
                        index
                    }
                }
                Some(true) => {
                    if let Some(index) = self.true_group {
                        index
                    } else if !INSERT {
                        MISSING_GROUP
                    } else {
                        let index = self.len();
                        self.true_group = Some(index);
                        index
                    }
                }
                None => {
                    if let Some(index) = self.null_group {
                        index
                    } else if !INSERT {
                        MISSING_GROUP
                    } else {
                        let index = self.len();
                        self.null_group = Some(index);
                        index
                    }
                }
            };

            groups.push(index);
        }

        Ok(())
    }
}

impl GroupValues for GroupValuesBoolean {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        self.probe::<true>(cols, groups)
    }

    fn lookup(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        self.probe::<false>(cols, groups)
    }

    fn supports_lookup(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        size_of::<Self>()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.false_group.is_some() as usize
            + self.true_group.is_some() as usize
            + self.null_group.is_some() as usize
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let len = self.len();
        let mut builder = BooleanBufferBuilder::new(len);
        let emit_count = match emit_to {
            EmitTo::All => len,
            EmitTo::First(n) => n,
        };
        builder.append_n(emit_count, false);
        if let Some(idx) = self.true_group.as_mut() {
            if *idx < emit_count {
                builder.set_bit(*idx, true);
                self.true_group = None;
            } else {
                *idx -= emit_count;
            }
        }

        if let Some(idx) = self.false_group.as_mut() {
            if *idx < emit_count {
                // already false, no need to set
                self.false_group = None;
            } else {
                *idx -= emit_count;
            }
        }

        let values = builder.finish();

        let nulls = if let Some(idx) = self.null_group.as_mut() {
            if *idx < emit_count {
                let mut buffer = NullBufferBuilder::new(len);
                buffer.append_n_non_nulls(*idx);
                buffer.append_null();
                buffer.append_n_non_nulls(emit_count - *idx - 1);

                self.null_group = None;
                Some(buffer.finish().unwrap())
            } else {
                *idx -= emit_count;
                None
            }
        } else {
            None
        };

        Ok(vec![Arc::new(BooleanArray::new(values, nulls)) as _])
    }

    fn values_preserving(
        &mut self,
        selection: GroupSelection<'_>,
    ) -> Result<Vec<ArrayRef>> {
        let num_groups = self.len();
        selection.validate_num_groups(num_groups)?;
        let mut values = BooleanBufferBuilder::new(selection.len());
        let mut nulls = NullBufferBuilder::new(selection.len());
        for index in selection.iter() {
            if self.null_group == Some(index) {
                values.append(false);
                nulls.append_null();
            } else {
                debug_assert!(
                    self.false_group == Some(index) || self.true_group == Some(index)
                );
                values.append(self.true_group == Some(index));
                nulls.append_non_null();
            }
        }
        Ok(vec![Arc::new(BooleanArray::new(
            values.finish(),
            nulls.finish(),
        ))])
    }

    fn supports_values_preserving(&self) -> bool {
        true
    }

    fn clear_shrink(&mut self, _num_rows: usize) {
        self.false_group = None;
        self.true_group = None;
        self.null_group = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_finds_stored_groups_only() -> Result<()> {
        let mut group_values = GroupValuesBoolean::new();
        assert!(group_values.supports_lookup());
        let mut groups = vec![];
        let probe: ArrayRef =
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)]));
        group_values.lookup(&[Arc::clone(&probe)], &mut groups)?;
        assert_eq!(groups, vec![MISSING_GROUP; 3]);
        assert!(group_values.is_empty());

        let input: ArrayRef =
            Arc::new(BooleanArray::from(vec![Some(true), Some(true), None]));
        group_values.intern(&[input], &mut groups)?;
        assert_eq!(groups, vec![0, 0, 1]);
        group_values.lookup(&[probe], &mut groups)?;
        assert_eq!(groups, vec![0, 1, MISSING_GROUP]);
        assert_eq!(group_values.len(), 2);
        Ok(())
    }
}
