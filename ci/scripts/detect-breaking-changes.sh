#!/bin/bash

set -e

BASE_REF="${GITHUB_BASE_REF:-main}"
HEAD_REF="${GITHUB_HEAD_REF:-HEAD}"

echo "🔍 DataFusion Breaking Changes Detection"
echo "Comparing against: $BASE_REF"
echo "Current ref: $HEAD_REF"

# Skip installation - we're only using git-based analysis
echo "📋 Using git-based analysis for breaking change detection..."

detect_datafusion_breaking_changes() {
    local breaking_changes_found=1  # 1 = false in bash

    echo "📋 Checking DataFusion-specific API rules..."

    # Skip semver checks for now - too resource intensive
    echo "⚠️  Skipping cargo-semver-checks (too resource intensive for CI)"
    echo "Using git-based analysis only..."

    # Only run git-based checks if we have a valid base ref
    if [ -n "$BASE_REF" ]; then
        echo "Checking for Rust source file changes..."
        if git diff "$BASE_REF" --name-only 2>/dev/null | grep -qE "\.rs$"; then
            echo "Rust files modified - running detailed checks..."

            echo "Checking LogicalPlan stability..."
            if check_logical_plan_changes; then
                echo "❌ Breaking changes detected in LogicalPlan"
                breaking_changes_found=0
            else
                echo "✅ No LogicalPlan breaking changes detected"
            fi

            echo "Checking DataFrame API..."
            if check_dataframe_api_changes; then
                echo "❌ Breaking changes detected in DataFrame API"
                breaking_changes_found=0
            else
                echo "✅ No DataFrame API breaking changes detected"
            fi

            echo "Checking SQL parser compatibility..."
            if check_sql_parser_changes; then
                echo "❌ Breaking changes detected in SQL parser"
                breaking_changes_found=0
            else
                echo "✅ No SQL parser breaking changes detected"
            fi
        else
            echo "✅ No Rust source files modified - no breaking changes possible"
        fi
    else
        echo "⚠️  Skipping git-based checks (no base ref available)"
    fi

    return $breaking_changes_found
}

check_logical_plan_changes() {
    # Check for changes in LogicalPlan files using git diff
    if git diff "$BASE_REF"..HEAD --name-only 2>/dev/null | grep -qE "(logical_plan|logical.*expr)"; then
        echo "    LogicalPlan-related files modified"

        # Check for enum variant removals (potential breaking change)
        if git diff "$BASE_REF"..HEAD 2>/dev/null | grep -qE "^-.*enum.*LogicalPlan|^-.*pub.*enum"; then
            echo "    Enum definitions modified"
            return 0  # Breaking change detected
        fi

        # Check for removed enum variants
        if git diff "$BASE_REF"..HEAD 2>/dev/null | grep -qE "^-\s*[A-Z][a-zA-Z]*\s*[\(\{,]"; then
            echo "    Enum variants potentially removed"
            return 0  # Breaking change detected
        fi
    fi

    return 1  # No breaking changes
}

check_dataframe_api_changes() {
    # Check if DataFrame public methods were changed
    if git diff "$BASE_REF" --name-only 2>/dev/null | grep -qE "dataframe"; then
        echo "    DataFrame files modified, checking for API changes..."

        # Check for ANY changes to public methods (additions, removals, modifications)
        if git diff "$BASE_REF" 2>/dev/null | grep -qE "^[-+].*pub fn"; then
            echo "    Public DataFrame API methods modified"
            return 0  # Breaking change detected
        fi
    fi

    return 1  # No breaking changes
}

check_sql_parser_changes() {
    # Check for SQL keyword removals
    if git diff "$BASE_REF"..HEAD --name-only 2>/dev/null | grep -qE "keywords"; then
        if git diff "$BASE_REF"..HEAD 2>/dev/null | grep -qE "^-.*,"; then
            echo "    SQL keywords potentially removed"
            return 0  # Breaking change detected
        fi
    fi

    return 1  # No breaking changes
}

generate_breaking_changes_report() {
    local output_file="breaking-changes-report.md"

    cat > "$output_file" << EOF
# 🚨 Breaking Changes Report

## Summary
Breaking changes detected in this PR that require the \`api-change\` label.

## DataFusion API Stability Guidelines
Per the [API Health Policy](https://datafusion.apache.org/contributor-guide/specification/api-health-policy.html):

EOF

    echo "### Git-Based Analysis:" >> "$output_file"

    # Only add git-based analysis if we have a base ref
    if [ -n "$BASE_REF" ]; then
        local changes_found=false

        # Check for DataFrame API changes with details - EXCLUDE script files
        if git diff "$BASE_REF" --name-only 2>/dev/null | grep -qE "dataframe.*\.rs$"; then
            # Only look at .rs files, exclude ci/scripts
            local dataframe_diff=$(git diff "$BASE_REF" 2>/dev/null -- "datafusion/*/src/**/*.rs" | grep -E "^[-+].*pub fn")
            if [ -n "$dataframe_diff" ]; then
                echo "- ⚠️  **DataFrame API changes detected:**" >> "$output_file"
                echo "\`\`\`diff" >> "$output_file"
                echo "$dataframe_diff" | head -10 >> "$output_file"
                echo "\`\`\`" >> "$output_file"
                echo "" >> "$output_file"
                changes_found=true
            fi
        fi

        # Check for LogicalPlan changes with details - EXCLUDE script files
        local logical_plan_files=$(git diff "$BASE_REF" --name-only 2>/dev/null | grep -E "(logical_plan|logical.*expr).*\.rs$")
        if [ -n "$logical_plan_files" ]; then
            echo "- ⚠️  **LogicalPlan changes detected in:**" >> "$output_file"
            echo "$logical_plan_files" | sed 's/^/  - /' >> "$output_file"
            echo "" >> "$output_file"
            changes_found=true
        fi

        # Show general public API changes - ONLY in .rs files, EXCLUDE ci/scripts
        local api_changes=$(git diff "$BASE_REF" 2>/dev/null -- "datafusion/" "ballista/" | grep -E "^[-+].*pub (fn|struct|enum)")
        if [ -n "$api_changes" ]; then
            echo "- ⚠️  **Public API signature changes:**" >> "$output_file"
            echo "\`\`\`diff" >> "$output_file"
            echo "$api_changes" | head -10 >> "$output_file"
            echo "\`\`\`" >> "$output_file"
            echo "" >> "$output_file"
            changes_found=true
        fi

        if [ "$changes_found" = false ]; then
            echo "- ⚠️  Breaking changes detected but specific patterns not identified" >> "$output_file"
            echo "- Run \`cargo semver-checks\` locally for detailed analysis" >> "$output_file"
        fi

        # Add list of changed SOURCE files only
        echo "" >> "$output_file"
        echo "### Source Files Modified:" >> "$output_file"
        git diff "$BASE_REF" --name-only 2>/dev/null | grep -E "\.rs$" | head -20 | sed 's/^/- /' >> "$output_file" || {
            echo "- No Rust source files modified" >> "$output_file"
        }

        echo "" >> "$output_file"
        echo "### All Files Modified:" >> "$output_file"
        git diff "$BASE_REF" --name-only 2>/dev/null | head -20 | sed 's/^/- /' >> "$output_file"
    else
        echo "- ⚠️  Git-based analysis skipped (no base ref available)" >> "$output_file"
    fi

    cat >> "$output_file" << EOF

## Required Actions:
1. Add the \`api-change\` label to this PR
2. Update CHANGELOG.md with breaking change details
3. Consider adding deprecation warnings before removal
4. Update migration guide if needed

## Approval Requirements:
- Breaking changes require approval from a DataFusion maintainer
- Consider if this change is necessary or if a deprecation path exists

## Note:
This analysis used git-based pattern detection. For comprehensive semver analysis,
run \`cargo semver-checks check-release --baseline-rev main\` locally.
EOF

    echo "📋 Report generated: $output_file"
}

main() {
    if detect_datafusion_breaking_changes; then
        echo ""
        echo "🚨 FINAL RESULT: Breaking changes detected!"
        # Only set GITHUB_ENV if it exists (in CI)
        if [ -n "$GITHUB_ENV" ]; then
            echo "BREAKING_CHANGES_DETECTED=true" >> "$GITHUB_ENV"
        fi

        generate_breaking_changes_report

        # Only try to comment if we have GitHub CLI and are in a PR context
        if command -v gh >/dev/null 2>&1 && [ -n "$GITHUB_TOKEN" ] && [ -n "$GITHUB_REPOSITORY" ] && [ "$GITHUB_EVENT_NAME" = "pull_request" ]; then
            echo "💬 Adding PR comment..."
            gh pr comment --body-file breaking-changes-report.md || {
                echo "⚠️  Could not add PR comment, but report was generated"
            }
        else
            echo "⚠️  Skipping PR comment (not in PR context or missing GitHub CLI)"
        fi

        exit 1
    else
        echo ""
        echo "🎉 FINAL RESULT: No breaking changes detected"
        # Only set GITHUB_ENV if it exists (in CI)
        if [ -n "$GITHUB_ENV" ]; then
            echo "BREAKING_CHANGES_DETECTED=false" >> "$GITHUB_ENV"
        fi
    fi
}

main "$@"