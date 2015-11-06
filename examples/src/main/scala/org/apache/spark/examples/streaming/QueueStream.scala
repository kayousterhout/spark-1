/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.streaming

import scala.collection.mutable.SynchronizedQueue

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Milliseconds}

object QueueStream {

  def main(args: Array[String]) {

    val numPartitions = if (args.length > 0) args(0).toInt else 128
    val numIters = if (args.length > 1) args(1).toInt else 10
    val batchMs = if (args.length > 2) args(2).toInt else 1000
    val depth = if (args.length > 3) args(3).toInt else 7

    StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("QueueStream")

    val sc = new SparkContext(sparkConf)
    // Create the context
    // Let all the executors join
    Thread.sleep(5000)

    sc.parallelize(0 until sc.getExecutorMemoryStatus.size, 
      sc.getExecutorMemoryStatus.size).foreach { 
      x => Thread.sleep(10) 
    }

    val ssc = new StreamingContext(sc, Milliseconds(batchMs))

    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    val rddQueue = new SynchronizedQueue[RDD[Int]]()

    // Create the QueueInputDStream and use it do some processing
    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.map(x => (x*2).toLong)
    mappedStream.foreachRDD { x =>
      println("TREE SUM is " + x.treeReduce(_ + _, depth))
    }
    // val reducedStream = mappedStream.reduceByKey((x: Int, y: Int) => x + y, 4)

    val start = System.nanoTime()
    ssc.start()

    // Create and push some RDDs into
    for (i <- 1 to numIters) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 1000000, numPartitions)
      Thread.sleep(batchMs)
    }

    ssc.stop(true, true)

    val end = System.nanoTime()

    println("DRIZ: TIME FOR " + numIters + " batches with batchMs " + batchMs + " " + (end-start)/1e3 + " microsecs")
  }
}
