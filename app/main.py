import timeit
from google.cloud import bigquery
from steputil import StepArgs, StepArgsBuilder

from app.bq_utils import create_tmp_table, delete_table
from app.export import determine_batches, export_to_df, export_to_gs
from app.config import Config
from app.constants import TERMINATION_LOG, BATCHES_PREFIX


def main(step: StepArgs):
    # Create config from step args
    config = Config(step)

    # Setting up client
    start_time = timeit.default_timer()
    print(f"Setting up bigquery client for project {config.billing_project}")
    client = bigquery.Client(project=config.billing_project)

    # Optionally creating a temporary table
    if config.temp_table is not None:
        create_tmp_table(client, config.with_statement, config.in_table_or_view, config.temp_table,
                        config.hash_columns, config.limit)
        # From now on all operations will be on temp table
        config.in_table_or_view = config.temp_table
        config.with_statement = ""
        config.limit = None
        config.do_count = False

    # Determine batches
    if config.do_batching:
        batches = determine_batches(client, config)
        print(f"Determined {len(batches)} batches")
    else:
        batches = None

    # Export data
    if config.gs_uri_prefix is None:
        # Export using dataframes
        export_to_df(client, config, batches, step.output)
    else:
        # Export directly to cloud storage
        export_to_gs(client, config, batches)

    # If temp table was created, delete it again
    if config.temp_table is not None:
        delete_table(client, config.temp_table)

    # Write comma separated list of batch names to termination log
    if config.do_batching:
        with open(TERMINATION_LOG, 'w') as f:
            f.write(BATCHES_PREFIX + ','.join(str(b) for b in batches))

    execution_time = timeit.default_timer() - start_time
    print(f"Data extraction completed in {execution_time:.1f} seconds")


def validate_config(config):
    """Validation function that checks config rules."""
    # Check exactly one input is specified
    num_inputs = sum([
        config.inputTable is not None,
        config.inputView is not None,
        config.inputQuery is not None
    ])

    if num_inputs != 1:
        print("ERROR: Exactly one of 'inputTable', 'inputView', or 'inputQuery' must be specified")
        return False

    # Check sharding configuration
    if config.maxBatchSize is not None and config.hashColumns is None:
        print("ERROR: In sharding mode ('maxBatchSize' is set), the parameter 'hashColumns' must be set")
        return False

    if config.maxBatchSize is None and config.hashColumns is not None:
        print("ERROR: The parameter 'hashColumns' can only be used in sharding mode (when 'maxBatchSize' is set)")
        return False

    # Check batching configuration
    if config.maxBatchSize is not None and config.batchColumn is not None:
        print("ERROR: Parameters 'maxBatchSize' and 'batchColumn' cannot be specified together")
        return False

    return True


if __name__ == "__main__":
    main(StepArgsBuilder()
         .output()
         .config("billingProject")
         .config("filenamePattern")
         .config("inputTable", optional=True)
         .config("inputView", optional=True)
         .config("inputQuery", optional=True)
         .config("tempTable", optional=True)
         .config("limit", optional=True)
         .config("storageUriPrefix", optional=True)
         .config("batchColumn", optional=True)
         .config("maxBatchSize", optional=True)
         .config("hashColumns", optional=True)
         .config("convertColumnsToString", optional=True)
         .validate(validate_config)
         .build()
         )
