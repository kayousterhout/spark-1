/*
 * Copyright 2014 The Regents of The University California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples

import scala.concurrent.{Await, ExecutionContext, future}
import scala.concurrent.duration._

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat

import org.apache.spark.{Logging, Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.util.LongArrayWritable

/**
 * Job that repeatedly sorts random data and stores the output on disk.
 */
object ConcurrentSortJobs extends Logging {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Sort Job")
    val spark = new SparkContext(conf)

    val numReduceTasks = args(0).toInt
    val inputFilename = args(1)
    val secondFilename =args(2)

    try {
      import ExecutionContext.Implicits.global
      val firstJobFuture = future {
        spark.setJobGroup("first", "First sort job")
        System.out.println(s"Running first job in ${Thread.currentThread()}")
        doSortJob(spark, numReduceTasks, inputFilename)
      }

      val secondJobFuture = future {
        spark.setJobGroup("second", "Second sort job")
        System.out.println(s"Running second job in ${Thread.currentThread()}")
        doSortJob(spark, numReduceTasks, secondFilename)
      }

      System.out.println("Waiting for both jobs to finish!")
      Await.result(firstJobFuture, 100 minutes)
      Await.result(secondJobFuture, 100 minutes)

    } finally {
      // Be sure to always stop the SparkContext, even when an exception is thrown; otherwise, the
      // event logs are more difficult to access.
      spark.stop()
    }
  }

  def doSortJob(spark: SparkContext, numReduceTasks: Int, filename: String): Unit = {
    val unsortedRddDisk = spark.sequenceFile(
      filename, classOf[LongWritable], classOf[LongArrayWritable])
    // Convert the RDD back to Longs, because LongWritables aren't serializable, so Spark
    // doesn't handle them nicely.
    val unsortedRddLongs = unsortedRddDisk.map { pair =>
      (pair._1.get(), pair._2.get())
    }
    val partitioner = new LongPartitioner(numReduceTasks)
    val sortedRdd = new ShuffledRDD[Long, Array[Long], Array[Long]](
      unsortedRddLongs, partitioner)
      .setKeyOrdering(Ordering[Long])
      .map(pair => (new LongWritable(pair._1), new LongArrayWritable(pair._2)))
    sortedRdd.saveAsNewAPIHadoopFile[SequenceFileOutputFormat[LongWritable, LongArrayWritable]](
      s"${filename}_sorted")
  }
}
