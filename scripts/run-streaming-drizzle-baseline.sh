#!/bin/bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

NUM_PARTS=128
DEPTH=7
NUM_BATCHES=50
SPARK_EVENTS_DIR=/mnt/spark-events

for BATCH_SIZE_MS in 4096 2048 1024 512 256 128
do
  LOG_SUFFIX=`date +"%m_%d_%H_%M_%S"`
  LOG_FILE="/mnt/streaming-baseline-$NUM_PARTS-$NUM_BATCHES-$BATCH_SIZE_MS-$DEPTH-$LOG_SUFFIX.log"
  $FWDIR/bin/run-example org.apache.spark.examples.streaming.QueueStream $NUM_PARTS $NUM_BATCHES $BATCH_SIZE_MS $DEPTH >& $LOG_FILE 
  APP_ID=`cat $LOG_FILE | grep "Connected to Spark cluster with app ID" | awk '{print $NF}'`
  #echo "For $NUM_PARTS $NUM_STAGES, $APP_ID finished. Parsing."
  pushd $FWDIR/scripts
  python parse_logs.py --pdf-relative-path $SPARK_EVENTS_DIR/$APP_ID > /dev/null
  popd
done
