"""
Benchmark test to measure Snowflake query performance across different schemas.
"""

import csv
import os
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from axolotl.snowflake_connection import SnowflakeConn, ColumnInfo
from axolotl.config import AxolotlConfig, SnowflakeOptions, load_config


def benchmark_column(conn: SnowflakeConn, column_info: ColumnInfo) -> List[Dict[str, Any]]:
    """
    Benchmark all queries for a single column. Returns a list of dicts containing
    benchmark results for each query

    - only runs simple queries, not histogram or percentile
    - Make sure the conn is configured not to exclude expensive queries
    """

    (fq_table_name, column_name, data_type, _, _) = column_info
    parts = fq_table_name.split('.')
    table_schema = parts[1] if len(parts) >= 2 else None
    table_name = parts[2] if len(parts) >= 3 else parts[-1]

    queries = conn._simple_queries(column_info)

    # Use the existing _query_values_benchmark method
    timings = conn._query_values_benchmark(queries, column_info)

    results = []
    for metric_name, timing in timings.items():
        metric_query = queries[metric_name]
        results.append({
            'table_schema': table_schema,
            'table_name': table_name,
            'column': column_name,
            'data_type': data_type,
            'metric_name': metric_name,
            'metric_query': metric_query,
            'timing_seconds': timing,
        })

    return results


def benchmark_schema(config: AxolotlConfig, connection_name: str, schema_name: str) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Benchmark all columns in a specific schema.

    Returns:
        Tuple of (detailed_results, summary_dict)
    """
    print(f"\n{'='*60}")
    print(f"Benchmarking schema: {schema_name}")
    print(f"{'='*60}\n")

    # Create a modified config for this specific schema
    schema_config = AxolotlConfig(
        connections={
            connection_name: SnowflakeOptions(
                **{
                    **config.connections[connection_name].model_dump(),
                    'include_schemas': [schema_name],
                }
            )
        },
        default_metrics_config=config.default_metrics_config,
    )

    detailed_results = []
    summary_by_table: Dict[str, Dict[str, Any]] = {}

    with SnowflakeConn(schema_config, connection_name, run_id=int(time.time())) as conn:
        all_columns = conn.list_columns()
        print(f"Found {len(all_columns)} columns to benchmark in {schema_name}")

        for i, column_info in enumerate(all_columns, 1):
            (fq_table_name, column_name, data_type, _) = column_info

            print(f"[{i}/{len(all_columns)}] Benchmarking {fq_table_name}.{column_name} ({data_type})...")

            try:
                column_results = benchmark_column(conn, column_info)
                detailed_results.extend(column_results)

                # Update summary information
                table_key = f"{column_results[0]['table_schema']}.{column_results[0]['table_name']}"
                if table_key not in summary_by_table:
                    summary_by_table[table_key] = {
                        'fq_table_name': column_info.fq_table_name,
                        'data_types': set(),
                        'row_count': 'N/A',  # We'll need to query this separately
                        'column_count': 0,
                    }

                summary_by_table[table_key]['data_types'].add(data_type)
                summary_by_table[table_key]['column_count'] += 1

            except Exception as e:
                print(f"  Error benchmarking {fq_table_name}.{column_name}: {e}")
                continue

        # Grab the row count
        print("\nGetting row counts for summary...")
        for table_key, summary in summary_by_table.items():
            try:
                fq_table = f"{conn.database}.{summary['table_schema']}.{summary['table_name']}"
                with conn.conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {fq_table}")
                    row_count = cur.fetchone()[0]
                    summary['row_count'] = row_count
            except Exception as e:
                print(f"  Error getting row count for {table_key}: {e}")
                summary['row_count'] = 'ERROR'

    # Convert summary dict to list
    summary_list = []
    for table_key, summary in summary_by_table.items():
        summary_list.append({
            'fq_table_name': summary['fq_table_name'],
            'data_types': ', '.join(sorted(summary['data_types'])),
            'row_count': summary['row_count'],
            'column_count': summary['column_count'],
        })

    return detailed_results, summary_list


def run_benchmark(schemas: List[str], config: AxolotlConfig, connection_name: str = 'default'):
    """
    Run benchmarks across multiple schemas and save results to CSV files.

    Args:
        schemas: List of schema names to benchmark (e.g., ["TPCH_SF10", "TPCH_SF1"])
        config: Axolotl configuration object
        connection_name: Name of the connection to use from config
    """
    # Create output directory
    output_dir = Path(__file__).parent / 'benchmark'
    output_dir.mkdir(exist_ok=True)

    all_summaries = []

    print(f"\n{'='*60}")
    print(f"Starting benchmark at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Schemas to benchmark: {', '.join(schemas)}")
    print(f"Output directory: {output_dir}")
    print(f"{'='*60}\n")

    for schema in schemas:
        try:
            # Run benchmark for this schema
            detailed_results, summary = benchmark_schema(config, connection_name, schema)

            # Write detailed results to schema-specific CSV
            output_file = output_dir / f"{schema}.csv"
            with open(output_file, 'w', newline='') as f:
                if detailed_results:
                    writer = csv.DictWriter(f, fieldnames=[
                        'table_schema', 'table_name', 'column', 'data_type',
                        'metric_name', 'metric_query', 'timing_seconds'
                    ])
                    writer.writeheader()
                    writer.writerows(detailed_results)

            print(f"\nWrote detailed results to {output_file}")
            print(f"Total queries benchmarked: {len(detailed_results)}")

            # Add schema name to summary entries
            for entry in summary:
                entry['schema'] = schema
            all_summaries.extend(summary)

        except Exception as e:
            print(f"\nError benchmarking schema {schema}: {e}")
            import traceback
            traceback.print_exc()
            continue

    # Write combined summary CSV
    summary_file = output_dir / 'summary.csv'
    with open(summary_file, 'w', newline='') as f:
        if all_summaries:
            writer = csv.DictWriter(f, fieldnames=[
                'schema', 'table_schema', 'table_name', 'data_types', 'row_count', 'column_count'
            ])
            writer.writeheader()
            writer.writerows(all_summaries)

    print(f"\n{'='*60}")
    print(f"Benchmark complete!")
    print(f"Summary written to {summary_file}")
    print(f"{'='*60}\n")


if __name__ == "__main__":

    config = load_config()
    schemas_to_benchmark = ["TPCH_SF1000"]
    run_benchmark(schemas_to_benchmark, config, 'sample')

