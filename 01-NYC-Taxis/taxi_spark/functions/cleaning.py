import re
from pyspark.sql import DataFrame, functions as F


def remove_duplicates(df: DataFrame) -> DataFrame:
    """
    Removes duplicate rows based on all columns.

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: New DataFrame with all duplicate rows removed.
    """
    return df.dropDuplicates()


def handle_nulls(df: DataFrame) -> DataFrame:
    """
    Fills null values in specified columns with predefined constants.
    'Tip_Amt', 'Tolls_Amt', 'surcharge', 'mta_tax' are filled with 0.0
    'Rate_Code' is filled with -1.0
    'store_and_forward' is filled with -1.0

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: New DataFrame with null values in specified columns replaced.
    """
    fill_values = {
        "Tip_Amt": 0.0,
        "Tolls_Amt": 0.0,
        "surcharge": 0.0,
        "mta_tax": 0.0,
        "Rate_Code": -1.0,
        "store_and_forward": -1.0,
    }

    return df.fillna(fill_values)


def type_casting(df: DataFrame) -> DataFrame:
    """
    Casts 'Passenger_Count' to integer and 'Trip_Distance' to float.

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: A new DataFrame with columns cast to specified types.
    """
    return (
        df.withColumn("Passenger_Count", F.col("Passenger_Count").cast("int"))
        .withColumn("Trip_Distance", F.col("Trip_Distance").cast("float"))
    )


def normalize_strings(df: DataFrame) -> DataFrame:
    """
    Trims and uppercases the 'vendor_name' and 'Payment_Type' columns.

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: A new DataFrame with normalized string values.
    """
    return (
        df.withColumn("vendor_name", F.upper(F.trim(F.col("vendor_name"))))
        .withColumn("Payment_Type", F.upper(F.trim(F.col("Payment_Type"))))
    )


def format_dates(df: DataFrame) -> DataFrame:
    """
    Converts 'Trip_Pickup_DateTime' and 'Trip_Dropoff_DateTime' to timestamp format.

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: A new DataFrame with dates converted to timestamps.
    """
    return (
        df.withColumn(
            "Trip_Pickup_DateTime",
            F.to_timestamp(F.col("Trip_Pickup_DateTime")),
        )
        .withColumn(
            "Trip_Dropoff_DateTime",
            F.to_timestamp(F.col("Trip_Dropoff_DateTime")),
        )
    )


def filter_coordinates(df: DataFrame) -> DataFrame:
    """
    Filters rows where coordinates are outside the valid range.
    (-180 <= Longitude <= 180, -90 <= Latitude <= 90)

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: A new DataFrame with rows having valid coordinates.
    """
    return df.filter(
        (F.col("Start_Lon").between(-180, 180))
        & (F.col("Start_Lat").between(-90, 90))
        & (F.col("End_Lon").between(-180, 180))
        & (F.col("End_Lat").between(-90, 90))
    )


def rename_columns(df: DataFrame) -> DataFrame:
    """
    Renames all columns by replacing non-alphanumeric characters with underscores
    and converting the text to lower case.

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: A new DataFrame with renamed columns.
    """
    for column in df.columns:
        new_column = re.sub(r"[^a-zA-Z0-9]", "_", column).lower()
        df = df.withColumnRenamed(column, new_column)

    return df