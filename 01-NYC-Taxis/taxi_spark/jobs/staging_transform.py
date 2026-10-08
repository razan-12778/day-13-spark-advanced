import argparse

from taxi_spark.functions.session import get_spark_session
from taxi_spark.functions.calculations import (
    calculate_trip_duration,
    calculate_haversine_distance,
)
from taxi_spark.functions.cleaning import (
    remove_duplicates,
    handle_nulls,
    type_casting,
    normalize_strings,
    format_dates,
    filter_coordinates,
    rename_columns,
)


def staging_pipeline(raw_uri: str, staging_uri: str) -> None:
    """
    Processes a raw Parquet dataset and writes the cleaned data to a staging URI.

    :param raw_uri: URI of the raw Parquet dataset.
    :param staging_uri: URI where the processed Parquet dataset will be saved.
    """

    spark = get_spark_session()

    try:
        df = spark.read.parquet(raw_uri)

        df = remove_duplicates(df)
        df = handle_nulls(df)
        df = type_casting(df)
        df = normalize_strings(df)
        df = format_dates(df)
        df = filter_coordinates(df)

        df = calculate_trip_duration(df)

        # Rename columns before calculating haversine distance,
        # because the calculation function expects lowercase names.
        df = rename_columns(df)

        df = calculate_haversine_distance(df)

        df.write.mode("overwrite").parquet(staging_uri)

    finally:
        spark.stop()


def main() -> None:
    """
    Main function that sets up argument parsing and builds the GCS paths.
    """
    parser = argparse.ArgumentParser(
        description="Preprocess the raw data and save it to the staging area in GCS"
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

    raw_uri = (
        f"gs://{args.bucket}/raw/"
        f"yellow_tripdata_{args.date}.parquet"
    )

    staging_uri = (
        f"gs://{args.bucket}/staging/"
        f"yellow_tripdata_{args.date}"
    )

    staging_pipeline(
        raw_uri,
        staging_uri,
    )


if __name__ == "__main__":
    main()