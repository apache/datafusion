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

