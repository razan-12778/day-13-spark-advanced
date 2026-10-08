# 🎯 Goal

Your goal is to build a pipeline in PySpark that will do the following:

1. Take a parquet from [New York City Taxi and Limousine Commission website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (we'll be using the Yellow Taxi Trip records) for a given month and upload it to a raw folder in your bucket.
1. Perform some data cleaning, add some additional columns, and store it in the staging area of your bucket.
1. Create two processed versions of the data:
    1. One for data analytics
    1. One for a machine learning workload
1. Implement the training of the the model and save that to the bucket for your data scientist.
1. Orchestrate the pipeline using Airflow to automatically submit the jobs to Google DataProc.

That might sound like a lot! But we will build it out step by step.

<br>

# 0️⃣ Setup

❓ First duplicate the `.env.sample` file and rename it to `.env`, fill in the values and `direnv reload` to load your environment variables. The bucket can be a new one just for this exercise! Then create the bucket. Don't forget that bucket names need to be **globally** unique, if your chosen bucket name is taken, try a different one - make sure to update your `.env`!

<details>
<summary markdown='span'>💡 gsutil reminder</summary>
To create new bucket:

```bash
gsutil mb -l eu gs://$BUCKET_NAME
```
</details>

Next we will need to add add an extension to Spark in order to allow us to work with Google Cloud Storage.

```bash
wget -P ~/spark/jars https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
```

Next checkout the file `taxi_spark/functions/session.py` to see how the extension is used.

Why do you think we **do not** need the extension when we are **not working** locally?

<br>

# 1️⃣ Download the data

The following command will download one month of data that we can use to start creating our transformations:

```bash
wget -P data https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-01.parquet
```

You can load the data into a Spark DataFrame in `notebooks/test.ipynb`. When creating pipelines in Spark, it is strongly recommended to develop in a notebook. It will make it easier to tweak and rerun your transformations to iterate towards the results you want!

<br>

# 2️⃣ Check schema

The first function you need to implement is `enforce_schema` in `taxi_spark/functions/schema.py`. This function should check that the schema of the dataframe is correct. If it is not correct it should raise an error. For this exercise you can assume that the schema you get from `2009-01` is valid.

🧪 Once you have created the function you can check it with:

```bash
pytest tests/test_taxi_spark/test_functions/test_schema.py
```

<br>

# 3️⃣ Write the write_to_raw job

Now it is time to create the first job which should:
- Download the data based on an input date
- Check the schema
- Write the data to the **raw** folder in your bucket

❓ Checkout the code in `taxi_spark/jobs/write_to_raw.py` and implement the missing parts.

🧪 To test the job, run it with:

```bash
# This will run the Spark job locally!
python taxi_spark/jobs/write_to_raw.py --bucket=$BUCKET_NAME --date=2009-01
```

Have a look on Google Cloud Storage and check everything looks good in your bucket!

<br>

# 4️⃣ Submit the job

Now we want to submit the Spark job to DataProc. To do this we will use the `gcloud` CLI tool. There is one argument that is quite difficult to find to allow us to make the external request. By default, a Spark cluster is not allowed to make external requests.

Setup the network to allow external requests:

```bash
gcloud compute networks create spark-vpc --subnet-mode=auto
```

```bash
gcloud compute routers create spark-router --network=spark-vpc --region=$GCP_REGION
```

```bash
gcloud compute networks subnets update spark-vpc \
    --region=$GCP_REGION \
    --enable-private-ip-google-access
```

```bash
gcloud compute firewall-rules create allow-internal \
    --network=spark-vpc \
    --allow tcp,udp,icmp \
    --source-ranges=10.128.0.0/9
```

```bash
gcloud compute routers nats create spark-nat \
    --router=spark-router \
    --auto-allocate-nat-external-ips \
    --nat-all-subnet-ip-ranges \
    --enable-logging \
    --region=$GCP_REGION
```

Quite a lot of steps 😅 but now we can create the cluster to submit our jobs using:

```bash
make create-cluster
```

❓ Implement the rest of the command in the Makefile under `submit-write-to-raw` to submit the job to your DataProc cluster!

<details>
<summary markdown='span'>💡 Hint</summary>

Think about how to get your files from your local dev environment to available in a bucket

</details>

<details>
<summary markdown='span'>🎁 Makefile solution</summary>

This will run the Spark job on your DataProc cluster!

```bash
submit-write-to-raw:
	$(eval WHEEL_NAME=$(shell poetry build -f wheel --no-ansi 2>&1 | awk '/Built/ {print $$3}'))
	gsutil cp "dist/$(WHEEL_NAME)" "gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)"
	gcloud dataproc jobs submit pyspark \
		--cluster=$(CLUSTER_NAME) \
		--project=$(PROJECT_ID) \
		--region=$(GCP_REGION) \
		--py-files="gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)" \
		taxi_spark/jobs/write_to_raw.py -- "--date=2009-01" "--bucket=$(BUCKET_NAME)"
```
</details>

<br>

# 5️⃣ Write the functions for cleaning the data

There are nine functions you need to implement in `taxi_spark/functions/cleaning.py` and `taxi_spark/functions/calculations.py` follow the instruction in the doc strings.

Remember to test using a notebook as you go!

<br>

# 6️⃣ Implement your own tests

This time you need to implement your own tests for your functions. Once you are done you can run `make test` to check that everything is working as expected.

You should end with an output similar to:

```bash
============================================= test session starts ==============================================
platform linux -- Python 3.12.8, pytest-8.3.4, pluggy-1.5.0 -- /home/lorcanrae/code/lorcanrae/data-engineering-challenges/04-Data-at-Scale/02-Spark-Advanced/01-NYC-Taxis/.venv/bin/python
cachedir: .pytest_cache
rootdir: /home/lorcanrae/code/lorcanrae/data-engineering-challenges/04-Data-at-Scale/02-Spark-Advanced/01-NYC-Taxis
configfile: pyproject.toml
plugins: anyio-4.8.0
collected 15 items

tests/test_taxi_spark/test_functions/test_calculations.py::test_calculate_trip_duration PASSED           [  6%]
tests/test_taxi_spark/test_functions/test_calculations.py::test_calculate_haversine_distance PASSED      [ 13%]
tests/test_taxi_spark/test_functions/test_cleaning.py::test_remove_duplicates PASSED                     [ 20%]
tests/test_taxi_spark/test_functions/test_cleaning.py::test_handle_nulls PASSED                          [ 26%]
tests/test_taxi_spark/test_functions/test_cleaning.py::test_type_casting PASSED                          [ 33%]
tests/test_taxi_spark/test_functions/test_cleaning.py::test_normalize_strings PASSED                     [ 40%]
tests/test_taxi_spark/test_functions/test_cleaning.py::test_format_dates PASSED                          [ 46%]
tests/test_taxi_spark/test_functions/test_cleaning.py::test_filter_coordinates PASSED                    [ 53%]
tests/test_taxi_spark/test_functions/test_cleaning.py::test_rename_columns PASSED                        [ 60%]
tests/test_taxi_spark/test_functions/test_processing.py::test_add_time_bins PASSED                       [ 66%]
tests/test_taxi_spark/test_functions/test_processing.py::test_add_pickup_date PASSED                     [ 73%]
tests/test_taxi_spark/test_functions/test_processing.py::test_drop_coordinates PASSED                    [ 80%]
tests/test_taxi_spark/test_functions/test_processing.py::test_aggregate_metrics PASSED                   [ 86%]
tests/test_taxi_spark/test_functions/test_processing.py::test_sort_by_date_and_time PASSED               [ 93%]
tests/test_taxi_spark/test_functions/test_schema.py::test_enforce_schema PASSED                          [100%]

============================================= 15 passed in 22.49s ==============================================
```

<br>

# 7️⃣ Implement and submit

Now you've completed and tested your transformations individually, put it all together by finishing `taxi_spark/jobs/staging_transformations.py`.

When you think you have it, try and run the job locally with:

```bash
# This will run the Spark job locally!
python taxi_spark/jobs/staging_transform.py --bucket=$BUCKET_NAME --date=2009-01
```

To make it easier to submit your transformation job to DataProc, add `make-submit-staging-transform` to the Makefile! It will be very similar to the `submit-write-to-raw` make command.

<details>
<summary markdown='span'>🎁 Makefile solution</summary>

This will run the Spark job on your DataProc cluster!

```bash
submit-staging-transform:
	$(eval WHEEL_NAME=$(shell poetry build -f wheel --no-ansi 2>&1 | awk '/Built/ {print $$3}'))
	gsutil cp "dist/$(WHEEL_NAME)" "gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)"
	gcloud dataproc jobs submit pyspark \
		--cluster=$(CLUSTER_NAME) \
		--project=$(PROJECT_ID) \
		--region=$(GCP_REGION) \
		--py-files="gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)" \
		taxi_spark/jobs/staging_transform.py -- "--date=2009-01" "--bucket=$(BUCKET_NAME)"
```
</details>

<br>

# 8️⃣ Processing job

At this point you know the drill! Implement `processing.py` and `ml.py`, then combine together to make `create_processed.py`, and finally submitting the job!

When you think you have it, try and run the job locally with:

```bash
# This will run the Spark job locally!
python taxi_spark/jobs/create_processed.py --bucket=$BUCKET_NAME --date=2009-01
```

To make it easier to submit your machine learning and aggregated data processing job to DataProc, add `submit-create-processed` to the Makefile! It will be very similar to the previous two make commands you created!

<details>
<summary markdown='span'>🎁 Makefile solution</summary>

This will run the Spark job on your DataProc cluster!

```bash
submit-create-processed:
	$(eval WHEEL_NAME=$(shell poetry build -f wheel --no-ansi 2>&1 | awk '/Built/ {print $$3}'))
	gsutil cp "dist/$(WHEEL_NAME)" "gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)"
	gcloud dataproc jobs submit pyspark \
		--cluster=$(CLUSTER_NAME) \
		--project=$(PROJECT_ID) \
		--region=$(GCP_REGION) \
		--py-files="gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)" \
		taxi_spark/jobs/create_processed.py -- "--date=2009-01" "--bucket=$(BUCKET_NAME)"
```
</details>

<br>

# 9️⃣ Orchestrate

Now we want to Orchestrate the whole process with Airflow. Start your Airflow service with:

```bash
docker compose up
```

Then implement the dag in the dags folder!

The operators you will need are in the GCP Provider for Airflow docs [at this link](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/dataproc/index.html).

To run the DAGs you will have to add a GCP Connection like we did in the **Advanced Airflow challenge**.

<br>

# 🏁 Finishing up

If you have come this far, you have implemented a full data pipeline in Spark that is orchestrated with Airflow! Congratulations 🎉 That's a massive achievement!

🧪 Test all your code with:

```bash
make test
```

And don't forget to git add, commit, and push your code to GitHub to track your progress on Kitt!

## Teardown

Make sure you teardown your cluster to save 💸 with:

```bash
make destroy-cluster
```

And clean up any network assets with:

```bash
make destroy-network-and-firewall
```

<br>

# Bonus

You can try and run the dag for many months and then create a new aggregation across an entire year of data!

<br>
