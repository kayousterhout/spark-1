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

import scala.sys.process._
import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Runs two jobs: one that writes data to disk, and a second that does some computation.
 */
object DiskAndComputeJobs {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Disk and Compute Jobs")
    val spark = new SparkContext(conf)

    val numDiskTasks = if (args.length > 0) args(0).toInt else 16
    val itemsPerPartition = if (args.length > 1) args(1).toInt else 1000
    // This needs to be large so that the job is disk-bound.
    val valuesPerItem = if (args.length > 2) args(2).toInt else 10000

    try {
      val diskRdd = spark.parallelize(1 to numDiskTasks, numDiskTasks).flatMap { i =>
        val random = new Random(i)
        Array.fill(itemsPerPartition)((random.nextLong, Array.fill(valuesPerItem)(random.nextLong)))
      }
      diskRdd.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)
      // Count the rdd to force it to be persisted to disk.
      diskRdd.count

      // Clear the buffer cache on all the machines, so that the data gets read from disk.
      val commands = Seq(
        "bash", "-c", "/root/ephemeral-hdfs/bin/slaves.sh /root/spark-ec2/clear-cache.sh")
      commands.lines

      // Count the RDD again; this time, it is read from disk.
      diskRdd.count

      // Run compute-intensive job.
      val recordsPerTaskPerSecond = 1724000
      val targetSeconds = if (args.length > 3) args(3).toInt else 13
      val availableCores = if (args.length > 4) args(4).toInt else 40
      val numItems = targetSeconds * recordsPerTaskPerSecond * availableCores
      val numComputeTasks = if (args.length > 5) args(5).toInt else 80
      spark.parallelize(1 to numItems, numComputeTasks).map(java.lang.Math.tan(_)).count

    } finally {
      // Be sure to always stop the SparkContext, even when an exception is thrown; otherwise,
      // the event logs are more difficult to access because they remain in the in-progress state.
      spark.stop()
    }
  }
}
