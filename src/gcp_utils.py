
import os

from google.cloud import storage


def batch_upload_files_to_bucket(bucket_name, source_directory):
    """
    Upload files in batch to a bucket in Google Cloud Storage
    :param bucket_name: destination bucket
    :param source_directory: directory containing files to upload
    :return: No return
    >>> # Example of use:
    >>> # batch_upload_files_to_bucket(bucket_name='gbif_occurrence_data', source_directory='./out/occurrences/raw/')
    """

    storage_client = storage.Client(os.getenv("GCP_PROJECT"))
    bucket = storage_client.bucket(bucket_name)

    file_names = os.listdir(source_directory)

    for file_name in file_names:
        blob = bucket.blob(file_name)
        blob.upload_from_filename(f'{source_directory}{file_name}')
        print(f'File {file_name} uploaded to {bucket_name} as {file_name}')