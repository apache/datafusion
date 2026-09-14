use super::blocked_custom_input_builder_with_lifetime::{
    BlockWithLifetime, BlockWithLifetimeProvider, BlockedCustomInputBuilderWithLifetime,
};
use arrow::row::{RowConverter, Rows};
use std::ops::{Deref, DerefMut};

#[derive(Debug)]
pub struct BlockedRowsBuilder<const FIXED_BLOCK_SIZING: bool>(
    BlockedCustomInputBuilderWithLifetime<FIXED_BLOCK_SIZING, RowsBlockProvider>,
);

impl<const FIXED_BLOCK_SIZING: bool> BlockedRowsBuilder<FIXED_BLOCK_SIZING> {
    pub fn new(block_size: usize, row_converter: RowConverter) -> Self {
        let block_provider = RowsBlockProvider { row_converter };

        Self(BlockedCustomInputBuilderWithLifetime::new(
            block_size,
            block_provider,
        ))
    }

    pub fn row_converter(&self) -> &RowConverter {
        self.0.provider().row_converter()
    }
}

impl<const FIXED_BLOCK_SIZING: bool> Deref for BlockedRowsBuilder<FIXED_BLOCK_SIZING> {
    type Target =
        BlockedCustomInputBuilderWithLifetime<FIXED_BLOCK_SIZING, RowsBlockProvider>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const FIXED_BLOCK_SIZING: bool> DerefMut for BlockedRowsBuilder<FIXED_BLOCK_SIZING> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
pub struct RowsBlockProvider {
    row_converter: RowConverter,
}

impl RowsBlockProvider {
    pub fn new(row_converter: RowConverter) -> Self {
        Self { row_converter }
    }

    pub fn row_converter(&self) -> &RowConverter {
        &self.row_converter
    }
}

impl BlockWithLifetimeProvider for RowsBlockProvider {
    type Block = Rows;

    fn new_block(&self) -> Self::Block {
        self.row_converter.empty_rows(0, 0)
    }

    fn allocated_size(&self) -> usize {
        self.row_converter.size()
    }
}

impl BlockWithLifetime for Rows {
    type Item<'a> = arrow::row::Row<'a>;

    fn allocated_size(&self) -> usize {
        self.size()
    }

    fn push(&mut self, item: Self::Item<'_>) {
        self.push(item)
    }

    fn extend<'a>(&mut self, iter: impl Iterator<Item = Self::Item<'a>>) {
        for item in iter {
            self.push(item)
        }
    }

    fn len(&self) -> usize {
        self.num_rows()
    }

    fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    fn index(&self, index: usize) -> Self::Item<'_> {
        self.row(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::groups_accumulator::BlocksIndex;
    use arrow::array::{ArrayRef, AsArray, Int32Array};
    use arrow::datatypes::{DataType, Int32Type};
    use arrow::row::SortField;
    use std::sync::Arc;

    fn converter() -> RowConverter {
        RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap()
    }

    fn rows_of(converter: &RowConverter, values: &[i32]) -> Rows {
        converter
            .convert_columns(&[Arc::new(Int32Array::from(values.to_vec())) as ArrayRef])
            .unwrap()
    }

    fn values_of(converter: &RowConverter, rows: &Rows) -> Vec<i32> {
        let columns = converter.convert_rows(rows).unwrap();
        columns[0].as_primitive::<Int32Type>().values().to_vec()
    }

    fn drain(builder: &mut BlockedRowsBuilder<true>) -> Vec<Vec<i32>> {
        let mut out = vec![];
        while let Some(block) = builder.take_block() {
            out.push(values_of(builder.row_converter(), &block));
        }
        assert_eq!(builder.len(), 0);
        assert_eq!(builder.num_blocks(), 1);
        out
    }

    #[test]
    fn new_is_empty() {
        let mut builder = BlockedRowsBuilder::<true>::new(3, converter());
        assert_eq!(builder.len(), 0);
        assert_eq!(builder.num_blocks(), 1);
        assert_eq!(builder.current_block_len(), 0);
        assert!(builder.take_block().is_none());
        assert!(builder.take_all().is_empty());
    }

    #[test]
    fn push_spans_blocks_and_values_are_readable() {
        let mut builder = BlockedRowsBuilder::<true>::new(3, converter());
        let source = rows_of(builder.row_converter(), &(0..8).collect::<Vec<_>>());

        let finished: Vec<bool> = source.iter().map(|row| builder.push(row)).collect();
        assert_eq!(
            finished,
            [false, false, true, false, false, true, false, false]
        );
        assert_eq!(builder.len(), 8);
        assert_eq!(builder.num_blocks(), 3);
        assert_eq!(builder.current_block_index(), 2);
        assert_eq!(builder.current_block_len(), 2);

        for i in 0..8 {
            let index = BlocksIndex::from_index_in_fixed_block_size(i, 3);
            assert_eq!(builder.value(index), source.row(i));
        }

        assert_eq!(
            drain(&mut builder),
            vec![vec![0, 1, 2], vec![3, 4, 5], vec![6, 7]]
        );
    }

    #[test]
    fn extend_in_block_and_take_all() {
        let mut builder = BlockedRowsBuilder::<true>::new(4, converter());
        let source = rows_of(builder.row_converter(), &[5, 6, 7, 8]);
        assert!(builder.extend_in_block(source.iter()));
        assert_eq!(builder.num_blocks(), 2);

        let source = rows_of(builder.row_converter(), &[9]);
        assert!(!builder.extend_in_block(source.iter()));

        let blocks: Vec<Vec<i32>> = builder
            .take_all()
            .iter()
            .map(|block| values_of(builder.row_converter(), block))
            .collect();
        assert_eq!(blocks, vec![vec![5, 6, 7, 8], vec![9]]);
        assert_eq!(builder.len(), 0);
    }

    #[test]
    fn take_block_then_push_continues_layout() {
        let mut builder = BlockedRowsBuilder::<true>::new(2, converter());
        let source = rows_of(builder.row_converter(), &[1, 2, 3]);
        for row in source.iter() {
            builder.push(row);
        }
        let block = builder.take_block().unwrap();
        assert_eq!(values_of(builder.row_converter(), &block), [1, 2]);
        assert_eq!(builder.len(), 1);
        assert_eq!(builder.current_block_index(), 0);

        let source = rows_of(builder.row_converter(), &[4, 5]);
        for row in source.iter() {
            builder.push(row);
        }
        assert_eq!(drain(&mut builder), vec![vec![3, 4], vec![5]]);
    }

    #[test]
    fn reset_clears_everything() {
        let mut builder = BlockedRowsBuilder::<true>::new(2, converter());
        let source = rows_of(builder.row_converter(), &[1, 2, 3]);
        for row in source.iter() {
            builder.push(row);
        }
        builder.reset();
        assert_eq!(builder.len(), 0);
        assert_eq!(builder.num_blocks(), 1);
        assert!(builder.take_block().is_none());
    }

    #[test]
    fn manual_start_new_block() {
        let mut builder = BlockedRowsBuilder::<false>::new(0, converter());
        let source = rows_of(builder.row_converter(), &[1, 2, 3, 4, 5]);
        for row in source.iter().take(3) {
            assert!(!builder.push(row));
        }
        builder.start_new_block();
        for row in source.iter().skip(3) {
            assert!(!builder.push(row));
        }
        assert_eq!(builder.num_blocks(), 2);
        assert_eq!(builder.len(), 5);
        assert_eq!(builder.value(BlocksIndex::new(1, 1)), source.row(4));

        let blocks: Vec<Vec<i32>> = builder
            .take_all()
            .iter()
            .map(|block| values_of(builder.row_converter(), block))
            .collect();
        assert_eq!(blocks, vec![vec![1, 2, 3], vec![4, 5]]);
    }
}
