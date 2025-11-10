import os
from json import load
from google.cloud import bigquery


def replace(string: str):
    if string is not None:
        for key, value in os.environ.items():
            string = string.replace(f"${{{key}}}", value)
    return string


def expect(json, key):
    return replace(json[key])


def optional(json, key):
    return replace(json.get(key))


# Config "struct"
class Config:
    def __init__(self, client, do_batching, batch_column, do_sharding, temp_table, hash_columns, limit, gs_uri_prefix, with_statement, table_or_view, filename_pattern, do_count, max_batch_size, output_path, convert_to_str):
        self.client = client
        self.do_batching = do_batching
        self.batch_column = batch_column
        self.do_sharding = do_sharding
        self.temp_table = temp_table
        self.hash_columns = hash_columns
        self.limit = limit
        self.gs_uri_prefix = gs_uri_prefix
        self.with_statement = with_statement
        self.table_or_view = table_or_view
        self.filename_pattern = filename_pattern
        self.do_count = do_count
        self.max_batch_size = max_batch_size
        self.output_path = output_path
        self.convert_to_str = convert_to_str


def create_config(config_path: str, output_path: str):
    # read config json into a dict
    with open(config_path) as f:
        config = load(f)

    # client for bq operation
    billing_project = expect(config, "billingProject")
    bqclient = bigquery.Client(project=billing_project)

    # input
    input_table = optional(config, "inputTable")
    input_view = optional(config, "inputView")
    input_query = optional(config, "inputQuery")

    # count number of input parameters set
    num_input = 0
    if input_view is not None:
        num_input += 1
    if input_table is not None:
        num_input += 1
    if input_query is not None:
        num_input += 1
    if num_input != 1:
        raise ValueError("Exactly one and only one of the parameters 'inputView', 'inputTable' and 'inputQuery' must be specified!")

    temp_table = optional(config, "tempTable")
    filename_pattern = expect(config, "filenamePattern")
    limit = optional(config, "limit")
    gs_uri_prefix = optional(config, "storageUriPrefix")
    max_batch_size = optional(config, "maxBatchSize")
    hash_columns = optional(config, "hashColumns")
    if max_batch_size is not None and hash_columns is None:
        raise ValueError("in sharding mode ('maxBatchSize' is set), the parameter 'hashColumns' must be set")
    if max_batch_size is None and hash_columns is not None:
        raise ValueError("the parameter 'hashColumns' can only be used in sharding mode (i.e. when 'maxBatchSize' is set)")
    batch_column = optional(config, "batchColumn")
    if max_batch_size is not None and batch_column is not None:
        raise ValueError("Parameters 'max_batch_size' and 'batch_column' cannot be specified together!")
    convert_to_str = optional(config, "convertColumnsToString")

    # set variable for input table/view
    do_count = True
    with_statement = ""
    table_or_view = None
    if input_view is not None:
        table_or_view = input_view
    if input_table is not None:
        table_or_view = input_table
        do_count = False # can read number of rows directly without counting
    if input_query is not None:
        with_statement = f"""
WITH data AS (
{input_query}
)"""
        table_or_view = "data"

    # determine other flags
    do_sharding = max_batch_size is not None
    do_batching = batch_column is not None or do_sharding

    # return the config object
    return Config(bqclient, do_batching, batch_column, do_sharding, temp_table, hash_columns, limit,
                  gs_uri_prefix, with_statement, table_or_view, filename_pattern, do_count, max_batch_size,
                  output_path, convert_to_str)
