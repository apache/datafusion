use serde::Serialize;

/// A node in a memory-usage tree, suitable for pretty-printing or JSON
/// serialization. `MemoryUsage` values can be nested, allowing callers to
/// inspect how memory is distributed across sub components.
#[derive(Debug, Serialize)]
pub struct MemoryUsage {
    /// Identifier (e.g. operator name or field)
    pub name: String,
    /// Approximate total bytes used by this node
    pub bytes: usize,
    /// Breakdown of sub-components
    pub children: Vec<MemoryUsage>,
}

/// Trait for types that can report their approximate memory consumption.
///
/// Implementors should provide a hierarchical [`MemoryUsage`] describing all
/// relevant allocations. The provided [`size`](MemoryExplain::size) method
/// simply returns the top level number of bytes.
///
/// The [`bytes`](MemoryUsage::bytes) field of the value returned by
/// [`explain_memory`](MemoryExplain::explain_memory) must match the value
/// returned by [`size`](MemoryExplain::size).
pub trait MemoryExplain {
    /// Returns the total bytes used by `self`.
    fn size(&self) -> usize {
        self.explain_memory().bytes
    }

    /// Returns a breakdown of memory usage for `self`.
    fn explain_memory(&self) -> MemoryUsage;
}

use crate::accumulator::Accumulator;
use crate::groups_accumulator::GroupsAccumulator;

impl MemoryExplain for dyn Accumulator {
    fn explain_memory(&self) -> MemoryUsage {
        MemoryUsage {
            name: std::any::type_name_of_val(self).to_string(),
            bytes: self.size(),
            children: vec![],
        }
    }
}

impl MemoryExplain for dyn GroupsAccumulator {
    fn explain_memory(&self) -> MemoryUsage {
        MemoryUsage {
            name: std::any::type_name_of_val(self).to_string(),
            bytes: self.size(),
            children: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accumulator::Accumulator;
    use crate::groups_accumulator::{EmitTo, GroupsAccumulator};
    use arrow::array::{ArrayRef, BooleanArray};
    use datafusion_common::{Result, ScalarValue};

    #[derive(Debug)]
    struct MockAcc {
        buf: Vec<u8>,
    }

    impl Default for MockAcc {
        fn default() -> Self {
            Self {
                buf: Vec::with_capacity(4),
            }
        }
    }

    impl Accumulator for MockAcc {
        fn update_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
            Ok(())
        }
        fn evaluate(&mut self) -> Result<ScalarValue> {
            Ok(ScalarValue::from(0u64))
        }
        fn state(&mut self) -> Result<Vec<ScalarValue>> {
            Ok(vec![])
        }
        fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
            Ok(())
        }
        fn size(&self) -> usize {
            self.buf.capacity()
        }
        fn supports_retract_batch(&self) -> bool {
            false
        }
        fn retract_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_accumulator_memory() {
        let acc = MockAcc::default();
        let usage = (&acc as &dyn Accumulator).explain_memory();
        assert_eq!(usage.bytes, 4);
        // Name should be the trait object type name
        assert_eq!(
            usage.name,
            std::any::type_name::<dyn Accumulator>().to_string()
        );
    }

    #[derive(Debug)]
    struct MockGroupsAcc {
        size: usize,
    }

    impl GroupsAccumulator for MockGroupsAcc {
        fn update_batch(
            &mut self,
            _values: &[ArrayRef],
            _groups: &[usize],
            _filter: Option<&BooleanArray>,
            _n: usize,
        ) -> Result<()> {
            Ok(())
        }
        fn evaluate(&mut self, _emit_to: EmitTo) -> Result<ArrayRef> {
            Err(datafusion_common::DataFusionError::Internal(
                "not used".into(),
            ))
        }
        fn state(&mut self, _emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
            Ok(vec![])
        }
        fn merge_batch(
            &mut self,
            _values: &[ArrayRef],
            _groups: &[usize],
            _filter: Option<&BooleanArray>,
            _n: usize,
        ) -> Result<()> {
            Ok(())
        }
        fn convert_to_state(
            &self,
            _values: &[ArrayRef],
            _filter: Option<&BooleanArray>,
        ) -> Result<Vec<ArrayRef>> {
            Ok(vec![])
        }
        fn supports_convert_to_state(&self) -> bool {
            false
        }
        fn size(&self) -> usize {
            self.size
        }
    }

    #[test]
    fn test_groups_acc_memory() {
        let acc = MockGroupsAcc { size: 8 };
        let usage = (&acc as &dyn GroupsAccumulator).explain_memory();
        assert_eq!(usage.bytes, 8);
        // Name should be the trait object type name
        assert_eq!(
            usage.name,
            std::any::type_name::<dyn GroupsAccumulator>().to_string()
        );
    }
}
