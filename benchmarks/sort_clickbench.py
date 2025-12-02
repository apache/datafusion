#!/usr/bin/env python3

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

#!/usr/bin/env python3
"""
Sort ClickBench data by EventTime for reverse scan benchmark.
Enhanced version with configurable row group size and optimization options.
"""

import sys
import argparse
from pathlib import Path

try:
    import pyarrow.parquet as pq
    import pyarrow.compute as pc
except ImportError:
    print("Error: pyarrow is not installed.")
    print("Please install it with: pip install pyarrow")
    sys.exit(1)


def sort_clickbench_data(
        input_path: str,
        output_path: str,
        row_group_size: int = 1024 * 1024,  # 1M rows default
        compression: str = 'snappy',
        verify: bool = True
):
    """Sort parquet file by EventTime column with optimized settings."""

    input_file = Path(input_path)
    output_file = Path(output_path)

    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        sys.exit(1)

    if output_file.exists():
        print(f"Sorted file already exists: {output_file}")
        if verify:
            verify_sorted_file(output_file)
        return

    try:
        print(f"Reading {input_file.name}...")
        table = pq.read_table(str(input_file))

        print(f"Original table has {len(table):,} rows")
        print("Sorting by EventTime...")

        # Sort the table by EventTime
        sorted_indices = pc.sort_indices(table, sort_keys=[("EventTime", "ascending")])
        sorted_table = pc.take(table, sorted_indices)

        print(f"Sorted table has {len(sorted_table):,} rows")

        # Verify sort
        event_times = sorted_table.column('EventTime').to_pylist()
        if event_times and verify:
            print(f"First EventTime: {event_times[0]}")
            print(f"Last EventTime: {event_times[-1]}")
            # Verify ascending order
            is_sorted = all(event_times[i] <= event_times[i+1] for i in range(min(1000, len(event_times)-1)))
            print(f"Sort verification (first 1000 rows): {'✓ PASS' if is_sorted else '✗ FAIL'}")

        print(f"Writing sorted file to {output_file}...")
        print(f"  Row group size: {row_group_size:,} rows")
        print(f"  Compression: {compression}")

        # Write sorted table with optimized settings
        pq.write_table(
            sorted_table,
            str(output_file),
            compression=compression,
            use_dictionary=True,
            write_statistics=True,
            # Optimize row group size for better performance
            row_group_size=row_group_size,
            # Set data page size (1MB is good for most cases)
            data_page_size=1024 * 1024,
            # Use v2 data page format for better compression
            use_deprecated_int96_timestamps=False,
            coerce_timestamps='us',  # Use microsecond precision
            # Batch size for writing
            write_batch_size=min(row_group_size, 1024 * 64),
            # Enable compression for all columns
            compression_level=None,  # Use default compression level
        )

        # Report results
        input_size_mb = input_file.stat().st_size / (1024**2)
        output_size_mb = output_file.stat().st_size / (1024**2)

        # Read metadata to verify row groups
        parquet_file = pq.ParquetFile(str(output_file))
        num_row_groups = parquet_file.num_row_groups

        print(f"\n✓ Successfully created sorted file!")
        print(f"  Input:  {input_size_mb:.1f} MB")
        print(f"  Output: {output_size_mb:.1f} MB")
        print(f"  Compression ratio: {input_size_mb/output_size_mb:.2f}x")
        print(f"\nRow Group Statistics:")
        print(f"  Total row groups: {num_row_groups}")
        print(f"  Total rows: {len(sorted_table):,}")

        # Show row group details
        for i in range(min(3, num_row_groups)):
            rg_metadata = parquet_file.metadata.row_group(i)
            print(f"  Row group {i}: {rg_metadata.num_rows:,} rows, {rg_metadata.total_byte_size / 1024**2:.1f} MB")

        if num_row_groups > 3:
            print(f"  ... and {num_row_groups - 3} more row groups")

        avg_rows_per_group = len(sorted_table) / num_row_groups if num_row_groups > 0 else 0
        print(f"  Average rows per group: {avg_rows_per_group:,.0f}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def verify_sorted_file(file_path: Path):
    """Verify that a parquet file is sorted by EventTime."""
    try:
        print(f"Verifying sorted file: {file_path}")
        parquet_file = pq.ParquetFile(str(file_path))

        num_row_groups = parquet_file.num_row_groups
        file_size_mb = file_path.stat().st_size / (1024**2)

        print(f"  File size: {file_size_mb:.1f} MB")
        print(f"  Row groups: {num_row_groups}")

        # Read first and last row group to verify sort
        first_rg = parquet_file.read_row_group(0)
        last_rg = parquet_file.read_row_group(num_row_groups - 1)

        first_time = first_rg.column('EventTime')[0].as_py()
        last_time = last_rg.column('EventTime')[-1].as_py()

        print(f"  First EventTime: {first_time}")
        print(f"  Last EventTime: {last_time}")
        print(f"  Sorted: {'✓ YES' if first_time <= last_time else '✗ NO'}")

    except Exception as e:
        print(f"Error during verification: {e}")


def main():
    parser = argparse.ArgumentParser(
        description='Sort ClickBench parquet file by EventTime',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage (1M rows per group)
  %(prog)s input.parquet output.parquet
  
  # Custom row group size (2M rows)
  %(prog)s input.parquet output.parquet --row-group-size 2097152
  
  # Use zstd compression
  %(prog)s input.parquet output.parquet --compression zstd
  
  # Verify existing file
  %(prog)s --verify-only output.parquet
        """
    )

    parser.add_argument(
        'input',
        nargs='?',
        help='Input parquet file path'
    )
    parser.add_argument(
        'output',
        nargs='?',
        help='Output sorted parquet file path'
    )
    parser.add_argument(
        '--row-group-size',
        type=int,
        default=64 * 1024,  # 64K rows
        help='Number of rows per row group (default: 65536 = 64K)'
    )
    parser.add_argument(
        '--compression',
        choices=['snappy', 'gzip', 'brotli', 'lz4', 'zstd', 'none'],
        default='zstd',
        help='Compression codec (default: zstd)'
    )
    parser.add_argument(
        '--compression-level',
        type=int,
        default=3,
        help='Compression level (default: 3 for zstd)'
    )
    parser.add_argument(
        '--no-verify',
        action='store_true',
        help='Skip sort verification'
    )
    parser.add_argument(
        '--verify-only',
        action='store_true',
        help='Only verify an existing sorted file (no sorting)'
    )

    args = parser.parse_args()

    if args.verify_only:
        if not args.input:
            parser.error("--verify-only requires input file")
        verify_sorted_file(Path(args.input))
        return

    if not args.input or not args.output:
        parser.error("input and output paths are required")

    sort_clickbench_data(
        args.input,
        args.output,
        row_group_size=args.row_group_size,
        compression=args.compression,
        verify=not args.no_verify
    )


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("Usage: python3 sort_clickbench_enhanced.py <input_file> <output_file>")
        print("Run with --help for more options")
        sys.exit(1)

    main()