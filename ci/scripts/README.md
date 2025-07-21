# CI Scripts

This directory contains scripts used in the DataFusion CI/CD pipeline.

## Breaking Changes Detection

### `detect-breaking-changes.sh`

Automatically detects potential breaking changes in DataFusion pull requests using git-based analysis.

**Features:**
- Detects changes to LogicalPlan enums and variants
- Monitors DataFrame API modifications
- Checks SQL parser keyword changes
- Generates detailed reports with diff snippets
- Integrates with GitHub to post PR comments
- Lightweight git-based analysis (no compilation required)

**Usage:**
```bash
# Run from repository root
./ci/scripts/detect-breaking-changes.sh

# With custom base ref
GITHUB_BASE_REF=v42.0.0 ./ci/scripts/detect-breaking-changes.sh
```

**Environment Variables:**
- `GITHUB_BASE_REF`: Base branch/tag to compare against (default: `main`)
- `GITHUB_HEAD_REF`: Head ref being tested (default: `HEAD`)
- `GITHUB_TOKEN`: Required for posting PR comments
- `GITHUB_ENV`: Set by GitHub Actions for CI integration

**Output:**
- Exit code 0: No breaking changes detected
- Exit code 1: Breaking changes detected
- Generates `breaking-changes-report.md` when issues found

### `breaking-changes-report.md`

Template/example of the report generated when breaking changes are detected. Contains:
- Summary of detected changes
- Links to DataFusion API stability guidelines
- Required actions for PR authors
- Approval requirements

## Integration

The breaking changes detection is integrated into the GitHub Actions workflow (`.github/workflows/dev.yml`) and runs automatically on all pull requests.