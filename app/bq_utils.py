import timeit


def hash_expression(hash_columns):
    # determine the concatenation of columns to be used in hash
    cast_list = ", ".join(f"CAST({x} AS STRING)" for x in hash_columns)
    # construct hash expression
    return f"abs(FARM_FINGERPRINT(CONCAT({cast_list})))"


def create_tmp_table(client, with_statement, table_or_view, temp_table, hash_columns, limit):
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
        query += "LIMIT " + str(limit)
    print(f"Query: {query}")
    # Run the query
    start_time = timeit.default_timer()
    query_job = client.query(query)
    query_job.result()  # Wait for the job to finish
    end_time = timeit.default_timer()
    execution_time = end_time - start_time
    print(f"Table {temp_table} created in {execution_time} seconds.")


def count_num_rows(client, with_statement, view_or_table):
    print(f"Counting rows in {view_or_table}")
    query = f"""
    {with_statement}
SELECT COUNT(*)
FROM `{view_or_table}`
    """
    # Run the query
    start_time = timeit.default_timer()
    query_job = client.query(query)
    result = query_job.result()
    # Parse the row count from the result
    num_rows = [row[0] for row in result][0]
    end_time = timeit.default_timer()
    execution_time = end_time - start_time
    print(f"Found {num_rows} in {execution_time} seconds.")
    return num_rows


def get_table_num_rows(client, table):
    num_rows = client.get_table(table).num_rows
    print(f"Table {table} has {num_rows} rows")
    return num_rows


def delete_table(client, table):
    client.delete_table(table, not_found_ok=True)
    print(f"Deleted table {table}")


def convert_columns_to_str(df):
    for column in df.columns:
        df[column] = df[column].astype(str)
    return df


def store(client, with_statement, limit, query, filename, convert_to_str):
    if len(with_statement) > 0:
        query = with_statement + "\n" + query
    if limit is not None:
        query = query + "LIMIT "+str(limit) + "\n"
    print(f"Query: {query}")
    start_time = timeit.default_timer()
    df = client.query(query).to_dataframe()
    # Optionally convert all columns to string, can prevent overflow for datetime objects
    if convert_to_str:
        df = convert_columns_to_str(df)
    df.to_json(path_or_buf=filename, orient='records', lines=True)
    end_time = timeit.default_timer()
    execution_time = end_time - start_time
    print(f"{len(df)} rows extracted to {filename} in  {execution_time} seconds.")
    return df


def select(has_temp_table):
    if has_temp_table:
        return "* EXCEPT(hashcol)"
    else:
        return "*"


def hash_ref(has_temp_table, hash_columns):
    if has_temp_table is None:
        return hash_expression(hash_columns)
    else:
        return "hashcol"


def store_shard(client, with_statement, limit, table, num_shards, shard, has_temp_table, hash_columns, filename, convert_to_str):
    query = f"""
SELECT {select(has_temp_table)}
FROM `{table}`
WHERE MOD({hash_ref(has_temp_table, hash_columns)}, {num_shards}) = {shard}
    """
    print(f"Exporting shard {shard} (of {num_shards}) from {table} to {filename}")
    return store(client, with_statement, limit, query, filename, convert_to_str)


def store_batch(client, with_statement, limit, table, batch, batch_column, filename, convert_to_str):
    query = f"""
SELECT * EXCEPT({batch_column})
FROM `{table}`
WHERE {batch_column} = "{batch}"
    """
    print(f"Exporting batch {batch_column}={batch} from {table} to {filename}")
    return store(client, with_statement, limit, query, filename, convert_to_str)


def store_all(client, with_statement, limit, table, filename, convert_to_str):
    query = f"""
SELECT *
FROM `{table}`
    """
    print(f"Exporting all in {table} to {filename}")
    return store(client, with_statement, limit, query, filename, convert_to_str)


def export(client, with_statement, limit, query):
    if len(with_statement) > 0:
        query = with_statement + "\n" + query
    if limit is not None:
        query = query + "LIMIT "+str(limit) + "\n"
    print(f"Query: {query}")
    start_time = timeit.default_timer()
    query_job = client.query(query)
    query_job.result()
    end_time = timeit.default_timer()
    execution_time = end_time - start_time
    print(f"Exported in {execution_time} seconds.")


def export_shard(client, with_statement, limit, table, num_shards, shard, has_temp_table, hash_columns, uri_pattern):
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
    print(f"Exporting batch all in {table} to {uri_pattern}")
    export(client, with_statement, limit, query)


def get_column_values(client, with_statement, table, column):
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
    end_time = timeit.default_timer()
    execution_time = end_time - start_time
    print(f"Found {len(column_values)} distinct values in {execution_time} seconds.")
    return column_values


