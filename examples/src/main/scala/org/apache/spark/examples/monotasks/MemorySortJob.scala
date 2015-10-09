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

package org.apache.spark.examples.monotasks

import scala.util.Random
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.ShuffledRDD

/**
 * Job that generates random data and stores the data in memory, and then repeatedly shuffles the
 * data.
 */
object MemorySortJob {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Sort Job")
    conf.set("spark.shuffle.writeToDisk", "false")
    val spark = new SparkContext(conf)

    // Sleep for a few seconds to give all of the executors a chance to register. Without this
    // sleep, the first stage can get scheduled before all of the executors have registered,
    // leading to load imbalance.
    Thread.sleep(5000)

    val numMapTasks = if (args.length > 0) args(0).toInt else 16
    val numReduceTasks = if (args.length > 1) args(1).toInt else 128
    val itemsPerPartition = if (args.length > 2) args(2).toInt else 6400000
    val itemsPerValue = if (args.length > 3) args(3).toInt else 6
    val numShuffles = if (args.length > 4) args(4).toInt else 10

    try {
      // Generate in-memory data.
      val unsortedRdd = spark.parallelize(1 to numMapTasks, numMapTasks).flatMap { i =>
        val random = new Random(i)
        Array.fill(itemsPerPartition)((random.nextLong, Array.fill(itemsPerValue)(random.nextLong)))
      }
      // Force the data to be materialized in-memory.
      unsortedRdd.cache()
      unsortedRdd.count()

      (0 until numShuffles).foreach { i =>
        val partitioner = new LongPartitioner(numReduceTasks)
        val sortedRdd = new ShuffledRDD[Long, Array[Long], Array[Long]](
          unsortedRdd, partitioner)
          .setKeyOrdering(Ordering[Long])
        sortedRdd.cache.count
        sortedRdd.unpersist()
      }
    } finally {
      // Be sure to always stop the SparkContext, even when an exception is thrown; otherwise, the
      // event logs are more difficult to access.
      spark.stop()
    }
  }
}
