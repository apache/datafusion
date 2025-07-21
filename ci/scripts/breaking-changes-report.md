# 🚨 Breaking Changes Report

## Summary
Breaking changes detected in this PR that require the `api-change` label.

## DataFusion API Stability Guidelines
Per the [API Health Policy](https://datafusion.apache.org/contributor-guide/specification/api-health-policy.html):

### Git-Based Analysis:
- ⚠️  Breaking changes detected but specific patterns not identified
- Review the modified files below for potential API changes

### Source Files Modified:
- datafusion/core/src/dataframe/mod.rs

### All Files Modified:
- .github/workflows/dev.yml
- ci/scripts/detect-breaking-changes.sh
- datafusion/core/src/dataframe/mod.rs

## Required Actions:
1. Add the `api-change` label to this PR
2. Update CHANGELOG.md with breaking change details
3. Consider adding deprecation warnings before removal
4. Update migration guide if needed

## Approval Requirements:
- Breaking changes require approval from a DataFusion maintainer
- Consider if this change is necessary or if a deprecation path exists

## Note:
This analysis uses git-based pattern detection to identify common breaking change patterns.
Review the specific changes carefully to determine if they truly constitute breaking changes.