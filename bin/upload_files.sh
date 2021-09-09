#!/bin/bash

aws s3 cp ../kf-portal-etl-docker/kf_etl_dev.conf s3://kf-strides-variant-parquet-prd/jobs/conf/kf-etl-dev.conf

aws s3 cp ../kf-portal-etl-docker/mondo_terms.json.gz s3://kf-strides-variant-parquet-prd/ontologies/mondo/mondo_terms.json.gz
aws s3 cp ../kf-portal-etl-docker/ncit.tsv.gz s3://kf-strides-variant-parquet-prd/ontologies/ncit/ncit.tsv.gz
aws s3 cp ../kf-portal-etl-docker/hpo_terms.json.gz s3://kf-strides-variant-parquet-prd/ontologies/hpo/hpo_terms.json.gz
aws s3 cp ../kf-portal-etl-docker/duo_code/duo.csv s3://kf-strides-variant-parquet-prd/ontologies/duo_codes/duo.csv
aws s3 cp ../kf-portal-etl-docker/studies_short_name.tsv s3://kf-strides-variant-parquet-prd/mapping/studies_short_name.tsv
aws s3 cp ../kf-portal-etl-docker/data_category_existing_data.tsv s3://kf-strides-variant-parquet-prd/mapping/data_category_existing_data.tsv