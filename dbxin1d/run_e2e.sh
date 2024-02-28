#!/usr/bin/env bash

echo y | databricks bundle destroy
databricks bundle validate
databricks bundle deploy -t dev --force-lock
databricks bundle run full_etl_aviation_training
