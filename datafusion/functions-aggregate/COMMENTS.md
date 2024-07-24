# Nullable Setting For List Items

When the accumulator state is a list of items and the type of the item
is either equivalent to the first-argument/returned-value, then
we set `nullable` argument of list item as `true` regardless of the
how `nullable` is defined in the schema of either the first argument or
the returned value.

For example, in `NthValueAgg` we do this:
```rust
fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
    let mut fields = vec![Field::new_list(
        format_state_name(self.name(), "nth_value"),
        Field::new("item", args.input_type.clone(), true), // always true
        false,
    )];
    // ... rest of the function code
}
```

By setting `nullable` to be always `true` like this we ensure that the
aggregate computation works even when nulls are present.

The advantage of doing it this way is that it eliminates the need for
additional code and special treatment of nulls in the accumulator state.

# Nullable Setting For List

This depends on the aggregate implementation.

In `ArrayAgg` the list is nullable, so it is set to `true`.
```rust
fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
    if args.is_distinct {
        return Ok(vec![Field::new_list(
            format_state_name(args.name, "distinct_array_agg"),
            Field::new("item", args.input_type.clone(), true),
            true, // list maybe null
        )]);
    }

    let mut fields = vec![Field::new_list(
        format_state_name(args.name, "array_agg"),
        Field::new("item", args.input_type.clone(), true),
        true, // list maybe null
    )];
    // ... rest of the function code
}
```

Alternatively in `Sum` the list is not nullable, so it is set to `false`.
```rust
fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
    if args.is_distinct {
        Ok(vec![Field::new_list(
            format_state_name(args.name, "sum distinct"),
            Field::new("item", args.return_type.clone(), true),
            false, // impossible for list to be null
        )])
    } else {
        // .. rest of the function code
    }
}
```