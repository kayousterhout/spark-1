#!/bin/bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

NUM_PARTS=128
SPARK_EVENTS_DIR=/mnt/spark-events

#NUM_STAGES=6

for NUM_STAGES in 7 #4 6
do
  LOG_SUFFIX=`date +"%m_%d_%H_%M_%S"`
  $FWDIR/bin/run-example org.apache.spark.examples.DrizzleBaseline $NUM_PARTS $NUM_STAGES >& /mnt/drizzle-baseline-$NUM_PARTS-$NUM_STAGES-$LOG_SUFFIX.log
  APP_ID=`cat /mnt/drizzle-baseline-$NUM_PARTS-$NUM_STAGES-$LOG_SUFFIX.log | grep "Connected to Spark cluster with app ID" | awk '{print $NF}'`
  echo "For $NUM_PARTS $NUM_STAGES, $APP_ID finished. Parsing."
  pushd $FWDIR/scripts
  python parse_logs.py --pdf-relative-path $SPARK_EVENTS_DIR/$APP_ID | tail -n +12
  popd
done
