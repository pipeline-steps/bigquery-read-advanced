# bigquery-read-advanced

Reading large amounts of data from BigQuery, supporting batching and sharding.

## Docker Image

This application is available as a Docker image on Docker Hub: `pipelining/bigquery-read-advanced`

### Usage

```bash
docker run -v /path/to/config.json:/config.json \
           -v /path/to/output:/output \
           -v /path/to/credentials.json:/credentials.json \
           -e GOOGLE_APPLICATION_CREDENTIALS=/credentials.json \
           pipelining/bigquery-read-advanced:latest \
           --config /config.json \
           --output /output/data.jsonl
```

To see this documentation, run without arguments:
```bash
docker run pipelining/bigquery-read-advanced:latest
```

## Parameters

### Required Parameters

| Name            | Description                                                        |
|-----------------|-------------------------------------------------------------------|
| billingProject  | The GCP project id to be used as quota project                    |
| filenamePattern | Pattern for output filenames (e.g., "data_{batch}.jsonl")         |

### Input Parameters (exactly one required)

| Name        | Description                                                          |
|-------------|----------------------------------------------------------------------|
| inputTable  | BigQuery table to be read (format: project.dataset.table)          |
| inputView   | BigQuery view to be read (format: project.dataset.view)            |
| inputQuery  | SELECT query to be executed (e.g., "SELECT * FROM ...")             |

### Optional Parameters

| Name                    | Description                                                                           |
|-------------------------|--------------------------------------------------------------------------------------|
| tempTable               | Temporary table for materialization (format: project.dataset.table)                  |
| limit                   | Limit the number of rows to read                                                     |
| storageUriPrefix        | GCS URI prefix for direct export to Cloud Storage (e.g., gs://bucket/path/)         |
| batchColumn             | Column name to use for batching (splits data by distinct values)                     |
| maxBatchSize            | Maximum batch size for sharding mode (requires hashColumns)                          |
| hashColumns             | Columns to hash for sharding (comma-separated, required when maxBatchSize is set)    |
| convertColumnsToString  | Array of column names to convert to string (e.g., ["date_column", "timestamp_col"]) |

**Notes:**
  * billingProject: The user/service account needs to have bigquery.jobs.create permission on this project
  * inputTable/inputView: The user/service account needs to have bigquery.tables.getData permission
  * inputQuery: The user/service account needs to have bigquery.tables.getData permission on the queried table(s)/view(s)
  * You must specify exactly one of: inputTable, inputView, or inputQuery
  * Batching modes:
    - **Column-based batching**: Use `batchColumn` to split data by distinct values in a column
    - **Hash-based sharding**: Use `maxBatchSize` and `hashColumns` to shard data into fixed-size batches
    - Cannot use both batching modes simultaneously
  * storageUriPrefix: When set, data is exported directly to GCS instead of local dataframes
  * tempTable: Useful for materializing complex queries or views before processing
