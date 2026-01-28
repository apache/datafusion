#!/usr/bin/env python3
"""Quick test to verify OwnedCastOptions compilation"""
import subprocess
import sys

def run_command(cmd):
    """Run a command and return exit code"""
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, cwd="/Users/kosiew/GitHub/df-temp", capture_output=False)
    return result.returncode

# Test 1: Check datafusion-common format module
print("\n=== Testing datafusion-common ===")
exit_code = run_command("cargo check -p datafusion-common --lib")
if exit_code != 0:
    print(f"FAILED: cargo check -p datafusion-common returned {exit_code}")
    sys.exit(1)
print("PASSED: datafusion-common checks out")

# Test 2: Check physical-expr (which uses CastExpr)
print("\n=== Testing datafusion-physical-expr (CastExpr) ===")
exit_code = run_command("cargo check -p datafusion-physical-expr --lib")
if exit_code != 0:
    print(f"FAILED: cargo check -p datafusion-physical-expr returned {exit_code}")
    sys.exit(1)
print("PASSED: datafusion-physical-expr checks out")

# Test 3: Check proto crate (which uses from_proto)
print("\n=== Testing datafusion-proto (deserialization) ===")
exit_code = run_command("cargo check -p datafusion-proto --lib")
if exit_code != 0:
    print(f"FAILED: cargo check -p datafusion-proto returned {exit_code}")
    sys.exit(1)
print("PASSED: datafusion-proto checks out")

print("\n=== ALL CHECKS PASSED ===")
