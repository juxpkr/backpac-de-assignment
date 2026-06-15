#!/usr/bin/env bash
set -e

sbt package

spark-submit \
  --class com.backpac.assignment.ActivityLogTableJob \
  --master local[2] \
  --driver-memory 2g \
  target/scala-2.12/backpac_de_assignment_2.12-0.1.0.jar \
  --input /app/data/test \
  --output /app/warehouse/activity_events_test \
  --database backpac \
  --table activity_events_test
