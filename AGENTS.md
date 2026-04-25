# Agent Guidelines for Apache DataFusion

## Developer Documentation

- [Quick Start Setup](docs/source/contributor-guide/development_environment.md#quick-start)
- [Testing Quick Start](docs/source/contributor-guide/testing.md#testing-quick-start)
- [Before Submitting a PR](docs/source/contributor-guide/index.md#before-submitting-a-pr)
- [Contributor Guide](docs/source/contributor-guide/index.md)
- [Architecture Guide](docs/source/contributor-guide/architecture.md)

## Before Committing

Before committing any changes, you MUST follow the instructions in
[Before Submitting a PR](docs/source/contributor-guide/index.md#before-submitting-a-pr)
and ensure the required checks listed there pass. Do not commit code that
fails any of those checks.

At a minimum, you MUST run and fix any errors from these commands before
committing:

```bash
# Format code
cargo fmt --all

# Lint (must pass with no warnings)
cargo clippy --all-targets --all-features -- -D warnings
```

You can also run the full lint suite used by CI:

```bash
./dev/rust_lint.sh
# or auto-fix: ./dev/rust_lint.sh --write --allow-dirty
```

When creating a PR, you MUST follow the [PR template](.github/pull_request_template.md).

## Testing

See the [Testing Quick Start](docs/source/contributor-guide/testing.md#testing-quick-start)
for the recommended pre-PR test commands.
