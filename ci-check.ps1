param (
    [switch]$Help
)

if ($Help) {
    Write-Host "Usage: .\ci-check.ps1"
    Write-Host "Runs formatting, clippy, and cargo tests to ensure the PR is ready for GitHub."
    exit
}

Write-Host "Running DataFusion CI Checks locally..." -ForegroundColor Cyan

Write-Host "`n[1/3] Running formatting (cargo fmt)..." -ForegroundColor Yellow
cargo fmt --all
if ($LASTEXITCODE -ne 0) {
    Write-Host "Formatting failed!" -ForegroundColor Red
    exit 1
}

Write-Host "`n[2/3] Running clippy (cargo clippy)..." -ForegroundColor Yellow
cargo clippy --all-targets --workspace -- -D warnings
if ($LASTEXITCODE -ne 0) {
    Write-Host "Clippy found warnings/errors!" -ForegroundColor Red
    exit 1
}

Write-Host "`n[3/3] Running tests (cargo test)..." -ForegroundColor Yellow
cargo test --workspace
if ($LASTEXITCODE -ne 0) {
    Write-Host "Tests failed!" -ForegroundColor Red
    exit 1
}

Write-Host "`nAll checks passed! You are ready to push to GitHub." -ForegroundColor Green
