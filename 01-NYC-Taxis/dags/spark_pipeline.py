import glob
import os

import pendulum
from airflow.decorators import dag
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)


PROJECT = "lewagon-de-student-7"
BUCKET = "razan-nyc-taxis-2026"
REGION = "europe-west1"
CLUSTER = "nyc-taxis-cluster"
DATE = "2009-01"

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WHEEL = glob.glob(os.path.join(PROJECT_ROOT, "dist", "*.whl"))[0]

WRITE_TO_RAW = os.path.join(
    PROJECT_ROOT, "taxi_spark", "jobs", "write_to_raw.py"
)
STAGING_TRANSFORM = os.path.join(
    PROJECT_ROOT, "taxi_spark", "jobs", "staging_transform.py"
)
CREATE_PROCESSED = os.path.join(
    PROJECT_ROOT, "taxi_spark", "jobs", "create_processed.py"
)


@dag(
    "spark-pipeline",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["spark"],
)
def spark_pipeline_dag():

    copy_wheel = LocalFilesystemToGCSOperator(
        task_id="copy_wheel_to_gcs",
        src=WHEEL,
        dst=f"python/{os.path.basename(WHEEL)}",
        bucket=BUCKET,
    )

    copy_write_to_raw = LocalFilesystemToGCSOperator(
        task_id="copy_write_to_raw_to_gcs",
        src=WRITE_TO_RAW,
        dst="jobs/write_to_raw.py",
        bucket=BUCKET,
    )

    copy_staging_transform = LocalFilesystemToGCSOperator(
        task_id="copy_staging_transform_to_gcs",
        src=STAGING_TRANSFORM,
        dst="jobs/staging_transform.py",
        bucket=BUCKET,
    )

    copy_create_processed = LocalFilesystemToGCSOperator(
        task_id="copy_create_processed_to_gcs",
        src=CREATE_PROCESSED,
        dst="jobs/create_processed.py",
        bucket=BUCKET,
    )

    write_to_raw = DataprocSubmitJobOperator(
        task_id="write_to_raw",
        project_id=PROJECT,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": f"gs://{BUCKET}/jobs/write_to_raw.py",
                "python_file_uris": [
                    f"gs://{BUCKET}/python/{os.path.basename(WHEEL)}"
                ],
                "args": [f"--date={DATE}", f"--bucket={BUCKET}"],
            },
        },
    )

    staging_transform = DataprocSubmitJobOperator(
        task_id="staging_transform",
        project_id=PROJECT,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": (
                    f"gs://{BUCKET}/jobs/staging_transform.py"
                ),
                "python_file_uris": [
                    f"gs://{BUCKET}/python/{os.path.basename(WHEEL)}"
                ],
                "args": [f"--date={DATE}", f"--bucket={BUCKET}"],
            },
        },
    )

    create_processed = DataprocSubmitJobOperator(
        task_id="create_processed",
        project_id=PROJECT,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": (
                    f"gs://{BUCKET}/jobs/create_processed.py"
                ),
                "python_file_uris": [
                    f"gs://{BUCKET}/python/{os.path.basename(WHEEL)}"
                ],
                "args": [f"--date={DATE}", f"--bucket={BUCKET}"],
            },
        },
    )

    (
        [
            copy_wheel,
            copy_write_to_raw,
            copy_staging_transform,
            copy_create_processed,
        ]
        >> write_to_raw
        >> staging_transform
        >> create_processed
    )


spark_pipeline_dag()
