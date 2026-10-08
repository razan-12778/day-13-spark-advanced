include ../../../make.inc
include .env

.PHONEY: submit-write-to-raw submit-staging-transform submit-create-processed test

UUID := $(shell uuidgen | tr '[:upper:]' '[:lower:]')


test: pytest-and-write-output


create-cluster:
	gcloud dataproc clusters create $(CLUSTER_NAME) \
		--project=$(PROJECT_ID) \
		--region=$(GCS_REGION) \
		--network=spark-vpc \
		--master-machine-type e2-standard-2 \
		--master-boot-disk-size 50 \
		--num-workers 2 \
		--worker-machine-type e2-standard-2 \
		--worker-boot-disk-size 50


submit-write-to-raw:
	$(eval WHEEL_NAME=$(shell poetry build -f wheel --no-ansi 2>&1 | awk '/Built/ {print $$3}'))
	gsutil cp "dist/$(WHEEL_NAME)" "gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)"
	gcloud dataproc jobs submit pyspark \
		--cluster=$(CLUSTER_NAME) \
		--project=$(PROJECT_ID) \
		--region=$(GCS_REGION) \
		--py-files="gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)" \
		taxi_spark/jobs/write_to_raw.py -- "--date=2009-01" "--bucket=$(BUCKET_NAME)"


submit-staging-transform:
	$(eval WHEEL_NAME=$(shell poetry build -f wheel --no-ansi 2>&1 | awk '/Built/ {print $$3}'))
	gsutil cp "dist/$(WHEEL_NAME)" "gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)"
	gcloud dataproc jobs submit pyspark \
		--cluster=$(CLUSTER_NAME) \
		--project=$(PROJECT_ID) \
		--region=$(GCS_REGION) \
		--py-files="gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)" \
		taxi_spark/jobs/staging_transform.py -- "--date=2009-01" "--bucket=$(BUCKET_NAME)"


submit-create-processed:
	$(eval WHEEL_NAME=$(shell poetry build -f wheel --no-ansi 2>&1 | awk '/Built/ {print $$3}'))
	gsutil cp "dist/$(WHEEL_NAME)" "gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)"
	gcloud dataproc jobs submit pyspark \
		--cluster=$(CLUSTER_NAME) \
		--project=$(PROJECT_ID) \
		--region=$(GCS_REGION) \
		--py-files="gs://$(BUCKET_NAME)/python/$(WHEEL_NAME)" \
		taxi_spark/jobs/create_processed.py -- "--date=2009-01" "--bucket=$(BUCKET_NAME)"


destroy-cluster:
	gcloud dataproc clusters delete $(CLUSTER_NAME) \
		--project=$(PROJECT_ID) \
		--region=$(GCS_REGION) \
		--quiet


destroy-network-and-firewall:
	gcloud compute firewall-rules delete allow-internal
	gcloud compute routers nats delete spark-nat \
		--router=spark-router \
		--region=$(GCS_REGION)
	gcloud compute routers delete spark-router \
		--region=$(GCS_REGION)
	gcloud compute networks delete spark-vpc