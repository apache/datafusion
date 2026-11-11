// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Schema};
use arrow::downcast_dictionary_array;
use arrow::record_batch::RecordBatch;
use datafusion_common::utils::compact_dictionary;
use datafusion_common::{Result, internal_err};
use datafusion_expr::ColumnarValue;

use crate::PhysicalExpr;

/// Rebuilds a dictionary with only the values referenced by the input rows.
#[derive(Debug, Eq)]
pub struct CompactDictionaryExpr {
    expr: Arc<dyn PhysicalExpr>,
}

impl PartialEq for CompactDictionaryExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
    }
}

impl Hash for CompactDictionaryExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
    }
}

impl CompactDictionaryExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl fmt::Display for CompactDictionaryExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "compact_dictionary({})", self.expr)
    }
}

impl PhysicalExpr for CompactDictionaryExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.expr.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self.expr.evaluate(batch)? {
            ColumnarValue::Array(array) => {
                let array_ref = array.as_ref();
                let should_compact = downcast_dictionary_array! {
                    array_ref => array_ref.values().len() > array_ref.len(),
                    dt => return internal_err!("expected dictionary array, got {dt}"),
                };
                if !should_compact {
                    return Ok(ColumnarValue::Array(array));
                }
                let compacted: ArrayRef = downcast_dictionary_array! {
                    array_ref => Arc::new(compact_dictionary(array_ref)?) as ArrayRef,
                    dt => return internal_err!("expected dictionary array, got {dt}"),
                };
                Ok(ColumnarValue::Array(compacted))
            }
            value @ ColumnarValue::Scalar(_) => Ok(value),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::new(Arc::clone(&children[0]))))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.expr.fmt_sql(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray, DictionaryArray, Int32Array, StringArray};
    use arrow::datatypes::{Field, Int32Type};
    use datafusion_expr::ColumnarValue;

    use crate::expressions::Column;

    #[test]
    fn compacts_unreferenced_dictionary_values() -> Result<()> {
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![2, 0, 2]),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
        )?;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            dictionary.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(dictionary)])?;
        let expr = CompactDictionaryExpr::new(Arc::new(Column::new("key", 0)));

        let ColumnarValue::Array(array) = expr.evaluate(&batch)? else {
            unreachable!()
        };
        let compacted = array.as_dictionary::<Int32Type>();
        assert_eq!(compacted.keys(), &Int32Array::from(vec![0, 1, 0]));
        assert_eq!(
            compacted.values().as_string::<i32>(),
            &StringArray::from(vec!["c", "a"])
        );
        Ok(())
    }
}
