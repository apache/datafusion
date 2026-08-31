# Migration to Blocks process

The process I did to implement blocks

## Steps

### 1. copy the entire `aggregates` module in `physical-plan` into new `aggregates_blocked` and update imports

this is important so we start from clean state while not breaking and so every subsequent step can be tested
and we can also use git diff and see what changed in each commit

we need to copy the entire module and then update all references to `aggregates` module imports to `aggregates_blocked`


