#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This script is meant for developers of DataFusion -- it is runnable
# from the standard DataFusion development environment and uses cargo,
# etc and orchestrates gathering data and run the benchmark binary in
# different configurations.

# Exit on error
set -e

# Get the current script directory
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

# Default path configurations
TPCDS_REPO="https://github.com/databricks/tpcds-kit"  # TPC-DS repository URL
TPCDS_BASE_DIR="${SCRIPT_DIR}/data"                  # Base directory for cloning and data
TPCDS_DIR="${TPCDS_BASE_DIR}/tpcds-kit"              # Directory for TPC-DS repository
TPCDS_TOOL_DIR="${TPCDS_DIR}/tools"                  # Directory for TPC-DS tools
TPCDS_DATA_DIR="${TPCDS_BASE_DIR}/scale_${SCALE_FACTOR}"  # Directory for generated TPC-DS data (includes scale factor)
DATAFUSION_DIR=$(cd -- "${DATAFUSION_DIR:-$SCRIPT_DIR/..}" && pwd)
QUERY_DIR="${DATAFUSION_DIR}/datafusion/core/tests/tpc-ds"  # Directory for query SQL files
RESULT_DIR="${SCRIPT_DIR}/results"                   # Directory for query results
CARGO_COMMAND="cargo run --release --bin tpcds"                # Command to run Rust binary

# Default arguments
SCALE_FACTOR=""
QUERY_NUMBER=""
QUERY_ALL=false
DIR=""

# Usage information
usage() {
    echo "
Usage:
verify.sh --scale <scale_factor> [--query <query_number> | --query_all] [--dir <path_to_data_folder>]

Options:
  --scale <scale_factor>   Specify TPC-DS data scale factor (e.g., 1, 10, 100, etc.)
  --query <query_number>   Specify the query number to execute (e.g., 1, 2, 3, etc.)
                           Cannot be used together with --query_all.
  --query_all              Execute all queries sequentially.
  --dir <path>             Specify a folder containing existing TPC-DS data (.dat files)
                           If provided, the script will skip data generation and convert .dat files to .parquet.

Examples:
# Clone the TPC-DS repository, build tools, generate 10GB data, convert to parquet, and execute all queries
./verify.sh --scale 10 --query_all

# Use existing data folder, convert to parquet, and execute all queries
./verify.sh --dir /path/to/existing/data --scale 10 --query_all

# Generate 10GB data, convert to parquet, and execute query 1
./verify.sh --scale 10 --query 1
"
    exit 1
}

# Parse command-line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --scale)
                SCALE_FACTOR=$2
                shift 2
                ;;
            --query)
                QUERY_NUMBER=$2
                shift 2
                ;;
            --query_all)
                QUERY_ALL=true
                shift 1
                ;;
            --dir)
                DIR=$2
                shift 2
                ;;
            *)
                usage
                ;;
        esac
    done

    if [[ -z "${SCALE_FACTOR}" && -z "${DIR}" ]]; then
        echo "Error: Either --scale or --dir is required."
        usage
    fi

    if [[ -n "${QUERY_NUMBER}" && "${QUERY_ALL}" == true ]]; then
        echo "Error: --query and --query_all cannot be used together."
        usage
    fi

    # Update TPCDS_DATA_DIR with the scale factor if provided
    if [[ -n "${SCALE_FACTOR}" ]]; then
        TPCDS_DATA_DIR="${TPCDS_BASE_DIR}/scale_${SCALE_FACTOR}"
    fi
}

# Clone the TPC-DS repository
clone_tpcds_repo() {
    if [ ! -d "${TPCDS_DIR}" ]; then
        echo "Cloning TPC-DS repository from ${TPCDS_REPO} into ${TPCDS_DIR}..."
        mkdir -p "${TPCDS_BASE_DIR}"
        git clone "${TPCDS_REPO}" "${TPCDS_DIR}"
        echo "TPC-DS repository cloned successfully."
    else
        echo "TPC-DS repository already exists at ${TPCDS_DIR}."
    fi
}

# Build TPC-DS tools
build_tpcds_tools() {
    echo "Building TPC-DS tools in ${TPCDS_TOOL_DIR}..."
    pushd "${TPCDS_TOOL_DIR}" > /dev/null
    make OS=MACOS CFLAGS="-D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE -DYYDEBUG -DMACOS -g -Wall -fcommon -std=c99 -Wno-implicit-int -Wno-error"
    popd > /dev/null
    echo "TPC-DS tools built successfully."
}

# Generate TPC-DS data
generate_data() {
    echo "Preparing TPC-DS data directory at ${TPCDS_DATA_DIR}..."
    
    # Check if the data directory exists for the given scale
    if [ -d "${TPCDS_DATA_DIR}" ]; then
        local dat_files_count=$(find "${TPCDS_DATA_DIR}" -type f -name "*.dat" | wc -l)
        if [ "${dat_files_count}" -ge 1 ]; then
            echo "TPC-DS data directory already contains .dat files. Skipping data generation."
            return
        else
            echo "TPC-DS data directory exists but contains no .dat files. Removing it..."
            rm -rf "${TPCDS_DATA_DIR}"
        fi
    fi

    mkdir -p "${TPCDS_DATA_DIR}"

    local relative_data_dir="../data"
    pushd "${TPCDS_TOOL_DIR}" > /dev/null
    ./dsdgen -dir "${TPCDS_DATA_DIR}" \
             -scale ${SCALE_FACTOR} \
             -FORCE
    popd > /dev/null
    DIR="${TPCDS_DATA_DIR}"
    convert_dat_to_parquet
}

# Convert .dat files to .parquet using the Python script
convert_dat_to_parquet() {
    python "${SCRIPT_DIR}/transfer_dat_parquet.py" --dir "${DIR}"
}

# Execute a single query
run_single_query() {
    QUERY_NUMBER=$1
    QUERY_FILE="${QUERY_DIR}/${QUERY_NUMBER}.sql"
    
    if [ ! -f "${QUERY_FILE}" ]; then
        echo "Error: Query file '${QUERY_FILE}' does not exist."
        exit 1
    fi

    echo "Executing query: ${QUERY_NUMBER}.sql on scale factor ${SCALE_FACTOR}..."

    pushd "${SCRIPT_DIR}" > /dev/null
    ${CARGO_COMMAND} -- run --data-dir "$TPCDS_DATA_DIR" --query "${QUERY_NUMBER}"
    popd > /dev/null
}

# Execute all queries
run_all_queries() {
    echo "Executing all TPC-DS queries sequentially on scale factor ${SCALE_FACTOR}..."
    # Trigger all queries directly using the Rust binary
    pushd "${SCRIPT_DIR}" > /dev/null
    ${CARGO_COMMAND} -- query-all --data-dir "${TPCDS_DATA_DIR}" 
    popd > /dev/null

    echo "All queries executed successfully."
}
# Main function
main() {
    parse_args "$@"
   
    if [ -z "${DIR}" ]; then
        clone_tpcds_repo
        build_tpcds_tools
        generate_data
    fi

    if [ "${QUERY_ALL}" == true ]; then
        run_all_queries
    elif [ -n "${QUERY_NUMBER}" ]; then
        run_single_query "${QUERY_NUMBER}"
    else
        echo "Error: Either --query or --query_all must be specified."
        usage
    fi
}

main "$@"