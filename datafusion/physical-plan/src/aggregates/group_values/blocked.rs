use super::single_group_by::{
    blocked_boolean::BlockedGroupValuesBoolean,
    blocked_primitive::BlockedGroupValuesPrimitive,
};
use crate::aggregates::group_values::new_group_values;
use crate::aggregates::order::GroupOrdering;
use arrow::array::{ArrayRef, downcast_primitive};
use arrow_schema::{DataType, SchemaRef};
use datafusion_common::{
    assert_eq_or_internal_err, assert_ne_or_internal_err, assert_or_internal_err,
    not_impl_err, unwrap_or_internal_err,
};
use datafusion_expr_common::blocked_groups_accumulator::{
    BlockedEmitTo, BlockedGroupSelection, BlocksIndex,
};
use datafusion_expr_common::groups_accumulator::EmitTo;

/// Stores the group values during hash aggregation.
///
/// # Background
///
/// In a query such as `SELECT a, b, count(*) FROM t GROUP BY a, b`, the group values
/// identify each group, and correspond to all the distinct values of `(a,b)`.
///
/// ```sql
/// -- Input has 4 rows with 3 distinct combinations of (a,b) ("groups")
/// create table t(a int, b varchar)
/// as values (1, 'a'), (2, 'b'), (1, 'a'), (3, 'c');
///
/// select a, b, count(*) from t group by a, b;
/// ----
/// 1 a 2
/// 2 b 1
/// 3 c 1
/// ```
///
/// # Design
///
/// Managing group values is a performance critical operation in hash
/// aggregation. The major operations are:
///
/// 1. Intern: Quickly finding existing and adding new group values
/// 2. Emit: Returning the group values as an array
///
/// There are multiple specialized implementations of this trait optimized for
/// different data types and number of columns, optimized for these operations.
/// See [`crate::aggregates::group_values::new_group_values`] for details.
///
/// # Group Ids
///
/// Each distinct group in a hash aggregation is identified by a unique group id
/// (usize) which is assigned by instances of this trait. Group ids are
/// continuous without gaps, starting from 0.
pub trait BlockedGroupValues: Send {
    fn block_size(&self) -> usize;

    /// Calculates the group id for each input row of `cols`, assigning new
    /// group ids as necessary.
    ///
    /// When the function returns, `groups`  must contain the group id for each
    /// row in `cols`.
    ///
    /// If a row has the same value as a previous row, the same group id is
    /// assigned. If a row has a new value, the next available group id is
    /// assigned.
    fn intern(
        &mut self,
        cols: &[ArrayRef],
        groups: &mut Vec<BlocksIndex>,
    ) -> datafusion_common::Result<()>;

    /// Returns the number of bytes of memory used by this [`BlockedGroupValues`].
    ///
    /// May be expensive; check the implementation before calling on hot paths.
    fn size(&self) -> usize;

    /// Returns true if this [`BlockedGroupValues`] is empty
    fn is_empty(&self) -> bool;

    /// The number of values (distinct group values) stored in this [`BlockedGroupValues`]
    fn len(&self) -> usize;

    /// Materializes selected group values without changing the stored values or
    /// their group indices.
    ///
    /// Rows are returned in the order specified by `selection`. An empty
    /// selection returns one correctly typed empty array per group-value column.
    ///
    /// This method requires exclusive access because implementations may mutate
    /// internal caches or builders, even though stored values are unchanged.
    fn values_preserving(
        &mut self,
        _selection: BlockedGroupSelection<'_>,
    ) -> datafusion_common::Result<Vec<ArrayRef>> {
        not_impl_err!("Preserving group values are not implemented")
    }

    /// Returns `true` if [`Self::values_preserving`] is implemented.
    fn supports_values_preserving(&self) -> bool {
        false
    }

    /// Emits the group values
    fn emit(
        &mut self,
        emit_to: BlockedEmitTo,
    ) -> datafusion_common::Result<Vec<Vec<ArrayRef>>> {
        match emit_to {
            BlockedEmitTo::All => self.emit_all(),
            BlockedEmitTo::NextBlock => {
                let len = self.len();
                let block_size = self.block_size();

                if len == 0 {
                    return Ok(vec![]);
                }

                if len <= block_size {
                    return self.emit_all();
                }

                let block = self.emit_block()?;
                let block = unwrap_or_internal_err!(block);

                // Assert that all arrays length equal block size since length is greater than block size
                for arr in &block {
                    assert_eq_or_internal_err!(arr.len(), block_size);
                }

                Ok(vec![block])
            }
            BlockedEmitTo::First(n) => {
                assert_ne_or_internal_err!(n, 0);
                assert_or_internal_err!(
                    n <= self.len(),
                    "n ({n}) must be less than or equal current length ({})",
                    self.len()
                );
                assert_or_internal_err!(
                    n < self.block_size(),
                    "n ({n}) must be less than current block size ({})",
                    self.block_size()
                );

                if n == self.len() {
                    self.emit_all()
                } else {
                    self.emit_first_n(n).map(|first_n| vec![first_n])
                }
            }
        }
    }

    /// Emit all group values
    fn emit_all(&mut self) -> datafusion_common::Result<Vec<Vec<ArrayRef>>>;

    /// Emit the next block
    /// returns Ok(None) when there are no blocks
    fn emit_block(&mut self) -> datafusion_common::Result<Option<Vec<ArrayRef>>>;

    /// Emit first `n` values and shift all values to fit into blocks
    ///
    /// `n` must be smaller than [`Self::block_size`] and larger than `0`
    /// `n` must be smaller or equal to [`Self::len`]
    fn emit_first_n(&mut self, n: usize) -> datafusion_common::Result<Vec<ArrayRef>>;

    // TODO - add into iterator which move the entry state into an iterator that will output ready batches
    //        this is so it won't need to update the underlying hash map whenever calling emit block
    //        this is for the case when we need to emit all, but we don't want to materialize all right away
    //        but we want to skip the internal hash map updates, so the iterator will avoid that while clearing memory
    //        the iterator should expose `allocated_size()` function

    /// Clear the contents and shrink the capacity to the size of the batch (free up memory usage)
    fn clear_shrink(&mut self, num_rows: usize);
}

// This is just an adapter until all is implemented
pub struct BlockedGroupValuesAdapter {
    block_size: usize,
    inner: Box<dyn crate::aggregates::group_values::GroupValues>,
}

impl BlockedGroupValuesAdapter {
    pub fn new(
        block_size: usize,
        inner: Box<dyn crate::aggregates::group_values::GroupValues>,
    ) -> Self {
        Self { block_size, inner }
    }
}

impl BlockedGroupValues for BlockedGroupValuesAdapter {
    fn block_size(&self) -> usize {
        self.block_size
    }

    fn intern(
        &mut self,
        cols: &[ArrayRef],
        groups: &mut Vec<BlocksIndex>,
    ) -> datafusion_common::Result<()> {
        let block_size = self.block_size;
        let mut group_indices_flattened = groups
            .iter()
            .map(|i| i.into_index_in_fixed_block_size(block_size))
            .collect::<Vec<_>>();
        self.inner.intern(cols, &mut group_indices_flattened)?;
        *groups = group_indices_flattened
            .iter()
            .map(|index| BlocksIndex::from_index_in_fixed_block_size(*index, block_size))
            .collect::<Vec<_>>();

        Ok(())
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn emit_all(&mut self) -> datafusion_common::Result<Vec<Vec<ArrayRef>>> {
        let mut blocks = vec![];

        while self.len() > self.block_size {
            blocks.push(self.inner.emit(EmitTo::First(self.block_size))?);
        }

        if self.len() > 0 {
            blocks.push(self.inner.emit(EmitTo::All)?);
        }

        Ok(blocks)
    }

    fn emit_block(&mut self) -> datafusion_common::Result<Option<Vec<ArrayRef>>> {
        if self.len() == 0 {
            return Ok(None);
        }

        let output = if self.len() <= self.block_size {
            self.inner.emit(EmitTo::All)
        } else {
            self.inner.emit(EmitTo::First(self.block_size))
        };

        Ok(Some(output?))
    }

    fn emit_first_n(&mut self, n: usize) -> datafusion_common::Result<Vec<ArrayRef>> {
        assert_ne_or_internal_err!(n, 0);
        assert_or_internal_err!(
            n <= self.len(),
            "n ({n}) must be less than or equal current length ({})",
            self.len()
        );
        assert_or_internal_err!(
            n < self.block_size(),
            "n ({n}) must be less than current block size ({})",
            self.block_size()
        );

        let output = if self.len() == n {
            self.inner.emit(EmitTo::All)
        } else {
            self.inner.emit(EmitTo::First(n))
        };

        Ok(output?)
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        self.inner.clear_shrink(num_rows)
    }
}

/// Return a specialized implementation of [`BlockedGroupValues`] for the given schema.
///
/// [`BlockedGroupValues`] implementations choosing logic:
///
///   - If group by single column, and type of this column has
///     the specific [`BlockedGroupValues`] implementation, such implementation
///     will be chosen.
///
///   - If group by multiple columns, and all column types have the specific
///     `GroupColumn` implementations, `GroupValuesColumn` will be chosen.
///
///   - Otherwise, the general implementation `GroupValuesRows` will be chosen.
///
/// `GroupColumn`:  crate::aggregates_blocked::group_values::multi_group_by::GroupColumn
/// `GroupValuesColumn`: crate::aggregates_blocked::group_values::multi_group_by::GroupValuesColumn
/// `GroupValuesRows`: crate::aggregates_blocked::group_values::GroupValuesRows
pub fn new_blocked_group_values(
    schema: SchemaRef,
    group_ordering: &GroupOrdering,
    block_size: usize,
) -> datafusion_common::Result<Box<dyn BlockedGroupValues>> {
    if schema.fields.len() == 1 {
        let d = schema.fields[0].data_type();

        macro_rules! downcast_helper {
            ($t:ty, $d:ident) => {
                return Ok(Box::new(BlockedGroupValuesPrimitive::<$t>::new(
                    $d.clone(),
                    block_size,
                )))
            };
        }

        downcast_primitive! {
            d => (downcast_helper, d),
            DataType::Boolean => {
                return Ok(Box::new(BlockedGroupValuesBoolean::new(block_size)));
            },
            _ => {}
        }
    }

    let group_values = new_group_values(schema, group_ordering)?;

    Ok(Box::new(BlockedGroupValuesAdapter::new(
        block_size,
        group_values,
    )))
}
