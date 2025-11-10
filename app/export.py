from math import ceil
from bq_utils import store_shard, store_batch, get_column_values, count_num_rows, get_table_num_rows, store_all, \
    export_shard, export_batch, export_all
from constants import BATCH_PLACEHOLDER


def determine_batches(config):
    if config.batch_column is not None:
        # a batch column is specified, use distinct values from it as batches
        return get_column_values(config.client, config.with_statement, config.table_or_view, config.batch_column)
    else:
        # determine number of rows
        if config.do_count:
            # counting rows using a query
            num_rows = count_num_rows(config.client, config.with_statement, config.table_or_view)
        else:
            # getting the number of rows directly from table metadata
            num_rows = get_table_num_rows(config.client, config.table_or_view)
        # determine number of shards required
        num_shards = ceil(num_rows / config.max_batch_size)
        return list(range(num_shards))


def export_to_df(config, batches):
    if config.do_batching:
        sum_len = 0
        max_len = 0
        for batch in batches:
            filename = config.output_path + config.filename_pattern.replace(BATCH_PLACEHOLDER, str(batch))
            if config.do_sharding:
                df = store_shard(config.client, config.with_statement, config.limit, config.table_or_view, len(batches), batch, config.temp_table is not None, config.hash_columns, filename, config.convert_to_str)
            else:
                df = store_batch(config.client, config.with_statement, config.limit, config.table_or_view, batch, config.batch_column, filename, config.convert_to_str)
            sum_len += len(df)
            max_len = max(max_len, len(df))
        print(f"Done extracting {len(batches)} dataframes (maximally {max_len} rows per batch, {sum_len} rows in total)")
    else:
        store_all(config.client, config.with_statement, config.limit, config.table_or_view, config.output_path + config.filename_pattern, config.convert_to_str)


def export_to_gs(config, batches):
    if config.do_batching:
        for batch in batches:
            uri = config.gs_uri_prefix + config.filename_pattern.replace(BATCH_PLACEHOLDER, str(batch))
            if config.do_sharding:
                export_shard(config.client, config.with_statement, config.limit, config.table_or_view, len(batches), batch, config.temp_table is not None, config.hash_columns, uri)
            else:
                export_batch(config.client, config.with_statement, config.limit, config.table_or_view, batch, config.batch_column, uri)
        print(f"Done extracting {len(batches)} batches to cloud storage")
    else:
        uri = config.gs_uri_prefix + config.filename_pattern
        export_all(config.client, config.with_statement, config.limit, config.table_or_view, uri)
