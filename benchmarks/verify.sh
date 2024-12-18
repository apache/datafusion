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
TPCDS_DATA_DIR="${TPCDS_DIR}/data"             # Directory for generated TPC-DS data
QUERY_DIR="${SCRIPT_DIR}/queries"                    # Directory for query SQL files
RESULT_DIR="${SCRIPT_DIR}/results"                   # Directory for query results
CARGO_COMMAND="cargo run --release --bin tpcds"      # Command to run Rust binary

# Default arguments
SCALE_FACTOR=""
QUERY_NUMBER=""
DIR=""

# Usage information
usage() {
    echo "
Usage:
verify.sh --scale <scale_factor> [--query <query_number>] [--dir <path_to_data_folder>]

Options:
  --scale <scale_factor>   Specify TPC-DS data scale factor (e.g., 1, 10, 100, etc.)
  --query <query_number>   Specify the query number to execute (e.g., 1, 2, 3, etc.)
                           If not provided, all queries will be executed.
  --dir <path>             Specify a folder containing existing TPC-DS data (.dat files)
                           If provided, the script will skip data generation and convert .dat files to .parquet.

Examples:
# Clone the TPC-DS repository, build tools, generate 10GB data, convert to parquet, and execute all queries
./verify.sh --scale 10

# Use existing data folder, convert to parquet, and execute all queries
./verify.sh --dir /path/to/existing/data --scale 10

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
    
    if [ -d "${TPCDS_DATA_DIR}" ]; then
        echo "TPC-DS data directory already exists. Removing it..."
        rm -rf "${TPCDS_DATA_DIR}"
    fi

    mkdir -p "${TPCDS_DATA_DIR}"

    echo "Generating TPC-DS data at scale factor ${SCALE_FACTOR} in ${TPCDS_DATA_DIR}..."

    local relative_data_dir="../data"

    echo "TPCDS_TOOL_DIR: ${TPCDS_TOOL_DIR}"

    local dsdgen_command="./dsdgen -dir ${relative_data_dir} -scale ${SCALE_FACTOR} -FORCE"
    echo "Running dsdgen command: ${dsdgen_command}"

    pushd "${TPCDS_TOOL_DIR}" > /dev/null
    ./dsdgen -dir "${relative_data_dir}" \
             -scale ${SCALE_FACTOR} \
             -FORCE
    popd > /dev/null

    echo "TPC-DS data generation completed."

    DIR="${TPCDS_DATA_DIR}"
}

# Convert .dat files to .parquet using the Python script
convert_dat_to_parquet() {
    echo "Converting .dat files in ${DIR} to .parquet format using transfer_dat_parquet.py..."
    python "${SCRIPT_DIR}/transfer_dat_parquet.py" --dir "${DIR}"
    echo "Conversion completed for .dat files in ${DIR}."
}

# Execute a single query
run_single_query() {
    QUERY_NUMBER=$1
    QUERY_FILE="${QUERY_DIR}/query${QUERY_NUMBER}.sql"

    if [ ! -f "${QUERY_FILE}" ]; then
        echo "Error: Query file '${QUERY_FILE}' does not exist."
        exit 1
    fi

    RESULT_FILE="${RESULT_DIR}/query${QUERY_NUMBER}_sf${SCALE_FACTOR}.out"

    echo "Running query: query${QUERY_NUMBER} on scale factor ${SCALE_FACTOR}..."
    mkdir -p "${RESULT_DIR}" # Ensure the result directory exists

    # Execute the query and save the result
    ${CARGO_COMMAND} -- query --query "${QUERY_FILE}" --path "${DIR}" --format parquet > "${RESULT_FILE}"

    echo "Query query${QUERY_NUMBER} completed. Result saved to ${RESULT_FILE}."
}

# Execute all queries
run_all_queries() {
    echo "Running all TPC-DS queries on scale factor ${SCALE_FACTOR}..."

    mkdir -p "${RESULT_DIR}" # Ensure the result directory exists

    # Iterate through all query files and execute them
    for query_file in "${QUERY_DIR}"/*.sql; do
        if [ -f "${query_file}" ]; then
            QUERY_NAME=$(basename "${query_file}" .sql)
            QUERY_NUMBER=${QUERY_NAME#query}
            run_single_query "${QUERY_NUMBER}"
        fi
    done

    echo "All queries completed. Results are in ${RESULT_DIR}."
}

setup_venv() {
    python3 -m venv "$VIRTUAL_ENV"
    PATH=$VIRTUAL_ENV/bin:$PATH python3 -m pip install -r requirements.txt
}


# Main function
main() {
    # Parse command-line arguments
    parse_args "$@"

    # If DIR is not specified, generate data
    if [ -z "${DIR}" ]; then
        # Clone TPC-DS repository
        clone_tpcds_repo

        # Build TPC-DS tools
        build_tpcds_tools

        # Generate data
        generate_data
    fi

    # Convert .dat files to .parquet
    convert_dat_to_parquet

    # Execute queries
    if [ -n "${QUERY_NUMBER}" ]; then
        run_single_query "${QUERY_NUMBER}"
    else
        run_all_queries
    fi
}

main "$@"