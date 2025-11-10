import argparse

from constants import TERMINATION_LOG, BATCHES_PREFIX
from export import *
from bq_utils import create_tmp_table, delete_table
from config import create_config

def main(config_path: str, output_path: str):
    # read configuration
    config = create_config(config_path, output_path)

    # optionally creating a temporary table
    if config.temp_table is not None:
        create_tmp_table(config.client, config.with_statement, config.in_table_or_view, config.temp_table, config.hash_columns, config.limit)
        # from now on all operations will be on temp table:
        config.in_table_or_view = config.temp_table
        # from now on all operations will not require a with statement anymore:
        config.with_statement = ""
        # from now on all operations will not have a limit:
        config.limit = None
        # number of rows can be obtained from table metadata directly
        config.do_count = False

    # determine batches
    if config.do_batching:
        batches = determine_batches(config)
    else:
        batches = None

    if config.gs_uri_prefix is None:
        # export using dataframes
        export_to_df(config, batches)
    else:
        # export directly to cloud storage
        export_to_gs(config, batches)

    # if temp table was created, delete it again
    if config.temp_table is not None:
        delete_table(config.client, config.temp_table)
        print(f"Deleted temp table {config.temp_table}")

    # write comma separated list of batch names to termination log
    if config.do_batching:
        with open(TERMINATION_LOG, 'w') as f:
            f.write(BATCHES_PREFIX+','.join(batches))

    print(f"Data extraction done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    args = vars(parser.parse_args())
    main(
        config_path=args["config"],
        output_path=args["output"]
    )
