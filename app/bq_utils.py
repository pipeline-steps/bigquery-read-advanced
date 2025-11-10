import timeit


def hash_expression(hash_columns):
    """Generate FARM_FINGERPRINT hash expression for given columns."""
    cast_list = ", ".join(f"CAST({x} AS STRING)" for x in hash_columns)
    return f"abs(FARM_FINGERPRINT(CONCAT({cast_list})))"


def create_tmp_table(client, with_statement, table_or_view, temp_table, hash_columns, limit):
    """Create a temporary table with optional hash column for sharding."""
    if hash_columns is None:
        select = "*"
    else:
        select = f"*, {hash_expression(hash_columns)} as hashcol"

    print(f"Creating temp table {temp_table} from {table_or_view}")
    query = f"""
{with_statement}
CREATE OR REPLACE TABLE `{temp_table}`
CLUSTER BY hashcol
AS SELECT {select}
FROM `{table_or_view}`
"""
    if limit is not None:
        query += f"LIMIT {limit}"

    print(f"Query: {query}")
    start_time = timeit.default_timer()
    query_job = client.query(query)
    query_job.result()  # Wait for the job to finish
    execution_time = timeit.default_timer() - start_time
    print(f"Table {temp_table} created in {execution_time:.1f} seconds")


def count_num_rows(client, with_statement, view_or_table):
    """Count the number of rows in a table or view."""
    print(f"Counting rows in {view_or_table}")
    query = f"""
{with_statement}
SELECT COUNT(*)
FROM `{view_or_table}`
"""
    start_time = timeit.default_timer()
    query_job = client.query(query)
    result = query_job.result()
    num_rows = [row[0] for row in result][0]
    execution_time = timeit.default_timer() - start_time
    print(f"Found {num_rows} rows in {execution_time:.1f} seconds")
    return num_rows


def get_table_num_rows(client, table):
    """Get the number of rows directly from table metadata."""
    num_rows = client.get_table(table).num_rows
    print(f"Table {table} has {num_rows} rows")
    return num_rows


def delete_table(client, table):
    """Delete a table if it exists."""
    client.delete_table(table, not_found_ok=True)
    print(f"Deleted table {table}")


def convert_columns_to_str(df):
    """Convert all dataframe columns to string type."""
    for column in df.columns:
        df[column] = df[column].astype(str)
    return df


def store(client, with_statement, limit, query, output, filename, convert_to_str):
    """Execute query and store results using steputil output."""
    if len(with_statement) > 0:
        query = with_statement + "\n" + query
    if limit is not None:
        query = query + f"LIMIT {limit}\n"

    print(f"Query: {query}")
    start_time = timeit.default_timer()
    df = client.query(query).to_dataframe()

    # Optionally convert all columns to string, can prevent overflow for datetime objects
    if convert_to_str:
        df = convert_columns_to_str(df)

    # Use steputil output to write JSON records
    records = df.to_dict('records')
    output.writeJsons(records, filename=filename)

    execution_time = timeit.default_timer() - start_time
    print(f"{len(df)} rows extracted to {filename} in {execution_time:.1f} seconds")
    return df


def select(has_temp_table):
    """Determine SELECT clause based on whether temp table has hash column."""
    if has_temp_table:
        return "* EXCEPT(hashcol)"
    else:
        return "*"


def hash_ref(has_temp_table, hash_columns):
    """Get reference to hash column or expression."""
    if has_temp_table:
        return "hashcol"
    else:
        return hash_expression(hash_columns)


def store_shard(client, with_statement, limit, table, num_shards, shard, has_temp_table, hash_columns, output, filename, convert_to_str):
    """Store a specific shard of data."""
    query = f"""
SELECT {select(has_temp_table)}
FROM `{table}`
WHERE MOD({hash_ref(has_temp_table, hash_columns)}, {num_shards}) = {shard}
"""
    print(f"Exporting shard {shard} (of {num_shards}) from {table} to {filename}")
    return store(client, with_statement, limit, query, output, filename, convert_to_str)


def store_batch(client, with_statement, limit, table, batch, batch_column, output, filename, convert_to_str):
    """Store a specific batch based on batch column value."""
    query = f"""
SELECT * EXCEPT({batch_column})
FROM `{table}`
WHERE {batch_column} = "{batch}"
"""
    print(f"Exporting batch {batch_column}={batch} from {table} to {filename}")
    return store(client, with_statement, limit, query, output, filename, convert_to_str)


def store_all(client, with_statement, limit, table, output, filename, convert_to_str):
    """Store all data from table."""
    query = f"""
SELECT *
FROM `{table}`
"""
    print(f"Exporting all data from {table} to {filename}")
    return store(client, with_statement, limit, query, output, filename, convert_to_str)


def export(client, with_statement, limit, query):
    """Execute export query to Cloud Storage."""
    if len(with_statement) > 0:
        query = with_statement + "\n" + query
    if limit is not None:
        query = query + f"LIMIT {limit}\n"

    print(f"Query: {query}")
    start_time = timeit.default_timer()
    query_job = client.query(query)
    query_job.result()
    execution_time = timeit.default_timer() - start_time
    print(f"Exported in {execution_time:.1f} seconds")


def export_shard(client, with_statement, limit, table, num_shards, shard, has_temp_table, hash_columns, uri_pattern):
    """Export a specific shard directly to Cloud Storage."""
    query = f"""
EXPORT DATA
  OPTIONS (
    uri = '{uri_pattern}',
    format = 'JSON',
    overwrite = true)
AS (
  SELECT {select(has_temp_table)}
  FROM {table}
  WHERE MOD({hash_ref(has_temp_table, hash_columns)}, {num_shards}) = {shard}
);
"""
    print(f"Exporting shard {shard} (of {num_shards}) from {table} to {uri_pattern}")
    export(client, with_statement, limit, query)


def export_batch(client, with_statement, limit, table, batch, batch_column, uri_pattern):
    """Export a specific batch directly to Cloud Storage."""
    query = f"""
EXPORT DATA
  OPTIONS (
    uri = '{uri_pattern}',
    format = 'JSON',
    overwrite = true)
AS (
  SELECT * EXCEPT({batch_column})
  FROM {table}
  WHERE {batch_column} = "{batch}"
);
"""
    print(f"Exporting batch {batch} from {table} to {uri_pattern}")
    export(client, with_statement, limit, query)


def export_all(client, with_statement, limit, table, uri_pattern):
    """Export all data directly to Cloud Storage."""
    query = f"""
EXPORT DATA
  OPTIONS (
    uri = '{uri_pattern}',
    format = 'JSON',
    overwrite = true)
AS (
  SELECT *
  FROM {table}
);
"""
    print(f"Exporting all data from {table} to {uri_pattern}")
    export(client, with_statement, limit, query)


def get_column_values(client, with_statement, table, column):
    """Get distinct values from a column for batching."""
    print(f"Determining batches by distinct values of column {column} in {table}")
    query = f"""
{with_statement}
SELECT DISTINCT {column}
FROM `{table}`
ORDER BY {column}
"""
    print(f"Query: {query}")
    start_time = timeit.default_timer()
    query_job = client.query(query)
    results = query_job.result()
    column_values = [row[0] for row in results]
    execution_time = timeit.default_timer() - start_time
    print(f"Found {len(column_values)} distinct values in {execution_time:.1f} seconds")
    return column_values
