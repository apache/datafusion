---
name: Apache DataFusion Contribution Guidelines
description: Instructions and context for AI agents working in the Apache DataFusion repository to ensure high quality, CI-compliant PRs.
---

# Apache DataFusion Guidelines

When working in the Apache DataFusion repository, you MUST follow these guidelines to ensure the user's PR passes CI checks and Codecov.

## 1. Local CI Pre-checks
Before telling the user a task is complete, always ensure you have verified the following or advise the user to run them:
1. **Clippy:** DataFusion has strict clippy rules. Run `cargo clippy --all-targets --workspace -- -D warnings`.
   - *Note:* DataFusion uses `#[expect(...)]` instead of `#[allow(...)]` for lint allowances to prevent stale attributes.
2. **Formatting:** Always run `cargo fmt --all`.
3. **Tests:** Ensure core tests pass. Run `cargo test -p datafusion --workspace`.

## 2. Code Coverage (Codecov)
DataFusion strictly enforces Codecov.
- Never leave dead or unreachable code. If you add a guard/check that can never be hit, remove it, as Codecov will flag the untested `false` branch.
- When changing core logic, always add or update a test in the corresponding `tests/` module to cover the newly added logic.
- Pay attention to `unwrap()`, `panic!()`, or early returns, as they often require explicit test coverage.

## 3. Architecture & Style
- DataFusion makes heavy use of `Arc<dyn ExecutionPlan>`, `Arc<dyn PhysicalExpr>`, and standard Arrow types.
- Be careful with `as _` type coercions; prefer explicit typing like `let exec: Arc<dyn ExecutionPlan> = ...` to avoid type inference errors in tests.
- When working on the physical optimizer, keep in mind that rules are applied sequentially and can mutate or swap plans. Verify invariants early in the rules (e.g., presence of dynamic filters).

## 4. Git Etiquette
- Keep commits focused and provide clear, descriptive commit messages.
- If you push changes directly for the user using their PAT, always verify tests compile first to avoid polluting the PR with broken commits.
