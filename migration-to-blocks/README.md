# Migration to Blocks process

The process I did to implement blocks

## Steps

### 1. copy the entire `aggregates` module in `physical-plan` into new `aggregates_blocked` and update imports

this is important so we start from clean state while not breaking and so every subsequent step can be tested
and we can also use git diff and see what changed in each commit

we need to copy the entire module and then update all references to `aggregates` module imports to `aggregates_blocked`

### 2. Reuse functions from `physical-plan` crate `aggregates` module in `aggregate_blocked` that are not going to be changed for certain

this is to reduce the amount of code exists in the duplication, but only do that for code that we are sure that we are not going to use

otherwise when we need it later we would add it back and we would think that this is our struct and not the struct from `aggregates` module that was just modified

and it will be harder to understand from which commit what actual changes were made

### 3. Rename `AggregateExec` in `aggregate_blocked` to `BlockedAggregateExec`
just to avoid confusion, don't change the exec name so tests will still pass

### 4. Change `try_new` in `AggregateExec` to return `BlockedAggregateExec` and not `AggregateExec` and have `actual_try_new` with the old code and change tests in `aggregate` module to call `actual_try_new`

this is so all tests in the codebase will use the blocked aggregate already and so any bugs will surface.
keeping the tests in `aggregate` module call `actual_try_new` is ok since those tests are also exists in the blocked version where it is against the new exec

### 5. Add `fallback_agg` to `BlockedAggregateExec` so when trying to migrate and some are still unsupported we can use that

make sure that when `BlockedAggregateExec` the underlying `fallback_agg` also updates

this is so the next steps of migrating streams one by one will be easier

### 6. Rename `GroupValues` to `BlockedGroupValues` and `GroupColumn` to `BlockedGroupColumn`

to avoid confusion

### 7. Add `BlocksIndex` and `BlockedEmitTo`

this is not dependent on anything and it is a building block for everything else

### 8. Change in group indices to be stored internally as `BlocksIndex` and not `usize`

this is to prepare for next where we change the `BlockedGroupValues` and `BlockedGroupColumn` to work with `BlocksIndex`

### 9. Remove all `BlockedGroupValues` impl and replace with adapter
this should have been done earlier maybe

this is so we can migrate one by one while adding required functions to the `BlockedGroupValues` trait

this requires to expose some functions from aggregate for now

### 10. Add `batch_size` to `BlockedGroupValues` for later emit change
this is needed for later emit change to return blocked, so we need to know the block size
