
import os

from google.cloud import bigquery, storage

def batch_upload_files_to_bucket(bucket_name, source_directory):
    """
    Upload files in batch to a bucket in Google Cloud Storage
    :param bucket_name: destination bucket
    :param source_directory: directory containing files to upload
    :return: No return
    >>> # Example of usage:
    >>> # batch_upload_files_to_bucket(bucket_name='gbif_occurrence_data', source_directory='./out/occurrences/raw/')
    """

    storage_client = storage.Client(os.getenv("GCP_PROJECT"))
    bucket = storage_client.bucket(bucket_name)

    file_names = os.listdir(source_directory)

    for file_name in file_names:
        blob = bucket.blob(file_name)
        blob.upload_from_filename(f'{source_directory}{file_name}')
        print(f'File {file_name} uploaded to {bucket_name} as {file_name}')


def load_file_to_table_bq(table_id: str, path_schema_json, path_data_file):
    """
    Load a file to BigQuery table.
    Following Google Cloud Documentation:
    https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-gcs-json?hl=en
    :param table_id: BigQuery table id 'project_id.dataset_id.table_name'
    :param path_schema_json: Path to the schema jason file
    :param path_data_file: Path to the data file in JSONL format.
    :return: No return
    >>> # Example of usage
     >>> # load_file_to_table_bq(
     >>> #     table_id=f'{os.getenv('GCP_PROJECT')}.{os.getenv('BQ_DATASET_NAME')}.climate_chelsa_bioclim',
     >>> #     path_data_file='./data/climate/climate_dataset.jsonl',
     >>> #     path_schema_json='./schema_chelsa_bioclim.json'
    >>>  # )
    """
    client = bigquery.Client()

    table_id = table_id

    schema = client.schema_from_json(path_schema_json)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    load_job = client.load_table_from_file(
        file_obj=open(path_data_file, 'r+b'),
        destination=table_id,
        location="europe-west2",
        job_config=job_config
    )

    load_job.result()

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))