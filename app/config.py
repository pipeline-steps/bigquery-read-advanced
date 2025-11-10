from steputil import StepArgs


class Config:
    """Configuration class that wraps StepArgs and provides convenient properties."""

    def __init__(self, step: StepArgs):
        self.step = step
        self.billing_project = step.config.billingProject
        self.filename_pattern = step.config.filenamePattern

        # Input configuration
        self.input_table = step.config.inputTable
        self.input_view = step.config.inputView
        self.input_query = step.config.inputQuery

        # Optional parameters
        self.temp_table = step.config.tempTable
        self.limit = int(step.config.limit) if step.config.limit else None
        self.gs_uri_prefix = step.config.storageUriPrefix
        self.batch_column = step.config.batchColumn
        self.max_batch_size = int(step.config.maxBatchSize) if step.config.maxBatchSize else None
        self.convert_to_str = step.config.convertColumnsToString

        # Parse hash columns (comma-separated string to list)
        if step.config.hashColumns:
            self.hash_columns = [col.strip() for col in step.config.hashColumns.split(',')]
        else:
            self.hash_columns = None

        # Determine input table/view and with_statement
        self.do_count = True
        self.with_statement = ""
        self.in_table_or_view = None

        if self.input_view is not None:
            self.in_table_or_view = self.input_view
        elif self.input_table is not None:
            self.in_table_or_view = self.input_table
            self.do_count = False  # Can read number of rows directly from table metadata
        elif self.input_query is not None:
            self.with_statement = f"""
WITH data AS (
{self.input_query}
)"""
            self.in_table_or_view = "data"

        # Determine batching flags
        self.do_sharding = self.max_batch_size is not None
        self.do_batching = self.batch_column is not None or self.do_sharding
