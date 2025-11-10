from math import ceil
from .bq_utils import (
    store_shard, store_batch, get_column_values, count_num_rows,
    get_table_num_rows, store_all, export_shard, export_batch, export_all
)
from .constants import BATCH_PLACEHOLDER


def determine_batches(client, config):
    """Determine batches based on configuration."""
    if config.batch_column is not None:
        # A batch column is specified, use distinct values from it as batches
        return get_column_values(client, config.with_statement, config.in_table_or_view, config.batch_column)
    else:
        # Determine number of rows
        if config.do_count:
            # Counting rows using a query
            num_rows = count_num_rows(client, config.with_statement, config.in_table_or_view)
        else:
            # Getting the number of rows directly from table metadata
            num_rows = get_table_num_rows(client, config.in_table_or_view)

        # Determine number of shards required
        num_shards = ceil(num_rows / config.max_batch_size)
        return list(range(num_shards))


def export_to_df(client, config, batches, output):
    """Export data to dataframes using steputil output."""
    if config.do_batching:
        sum_len = 0
        max_len = 0
        for batch in batches:
            filename = config.filename_pattern.replace(BATCH_PLACEHOLDER, str(batch))
            if config.do_sharding:
                df = store_shard(
                    client, config.with_statement, config.limit, config.in_table_or_view,
                    len(batches), batch, config.temp_table is not None,
                    config.hash_columns, output, filename, config.convert_to_str
                )
            else:
                df = store_batch(
                    client, config.with_statement, config.limit, config.in_table_or_view,
                    batch, config.batch_column, output, filename, config.convert_to_str
                )
            sum_len += len(df)
            max_len = max(max_len, len(df))
        print(f"Extracted {len(batches)} batches (max {max_len} rows per batch, {sum_len} rows total)")
    else:
        store_all(
            client, config.with_statement, config.limit, config.in_table_or_view,
            output, config.filename_pattern, config.convert_to_str
        )


def export_to_gs(client, config, batches):
    """Export data directly to Google Cloud Storage."""
    if config.do_batching:
        for batch in batches:
            uri = config.gs_uri_prefix + config.filename_pattern.replace(BATCH_PLACEHOLDER, str(batch))
            if config.do_sharding:
                export_shard(
                    client, config.with_statement, config.limit, config.in_table_or_view,
                    len(batches), batch, config.temp_table is not None,
                    config.hash_columns, uri
                )
            else:
                export_batch(
                    client, config.with_statement, config.limit, config.in_table_or_view,
                    batch, config.batch_column, uri
                )
        print(f"Extracted {len(batches)} batches to cloud storage")
    else:
        uri = config.gs_uri_prefix + config.filename_pattern
        export_all(client, config.with_statement, config.limit, config.in_table_or_view, uri)
