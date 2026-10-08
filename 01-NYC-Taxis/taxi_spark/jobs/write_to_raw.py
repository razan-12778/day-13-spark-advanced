import argparse
import requests
import tempfile

from google.cloud import storage

from taxi_spark.functions.session import get_spark_session
from taxi_spark.functions.schema import enforce_schema


def upload_to_gcs(file_path, bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.chunk_size = 8 * 1024 * 1024

    blob.upload_from_filename(
        file_path,
        timeout=600,
    )


def delete_blob(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()


def download_and_save_parquet(url: str, bucket: str, date: str) -> None:
    """
    Download a Parquet file, upload it to the raw folder in GCS,
    and validate its schema with Spark.

    :param url: URL to download the Parquet file from.
    :param bucket: GCS bucket name.
    :param date: Date in the format yyyy-MM.
    """

    blob_name = f"raw/yellow_tripdata_{date}.parquet"

    response = requests.get(
        url,
        stream=True,
        timeout=300,
    )
    response.raise_for_status()

    with tempfile.NamedTemporaryFile(suffix=".parquet") as temp_file:
        for chunk in response.iter_content(
            chunk_size=1024 * 1024
        ):
            if chunk:
                temp_file.write(chunk)

        temp_file.flush()

        # Upload the downloaded file to GCS first.
        upload_to_gcs(
            temp_file.name,
            bucket,
            blob_name,
        )

    # Read the file from GCS so the job also works on DataProc.
    spark = get_spark_session()

    try:
        gcs_path = f"gs://{bucket}/{blob_name}"

        df = spark.read.parquet(gcs_path)

        enforce_schema(df)

    finally:
        spark.stop()


def main() -> None:
    """
    Main function that sets up argument parsing,
    builds the download URL, and runs the job.
    """

    parser = argparse.ArgumentParser(
        description="Download Parquet from a URL and save to GCS."
    )

    parser.add_argument(
        "--date",
        required=True,
        help="Date in the format yyyy-MM",
    )

    parser.add_argument(
        "--bucket",
        required=True,
        help="GCS bucket name",
    )

    args = parser.parse_args()

    url = (
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"yellow_tripdata_{args.date}.parquet"
    )

    download_and_save_parquet(
        url,
        args.bucket,
        args.date,
    )


if __name__ == "__main__":
    main()