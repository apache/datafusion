# DataFusion Physical Expression Adapter

This crate provides utilities for adapting physical expressions to different schemas in DataFusion.

It handles schema differences in file scans by rewriting expressions to match the physical schema,
including type casting, missing columns, and partition values.

For detailed documentation, see the [`PhysicalExprAdapter`] trait documentation.
