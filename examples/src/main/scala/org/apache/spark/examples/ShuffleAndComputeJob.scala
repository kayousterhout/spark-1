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

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Runs shuffle job(s) that shuffle randomly generated data concurrently with a compute-heavy
 * job.
 */
object ShuffleAndComputeJob {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Shuffle Job")
    val spark = new SparkContext(conf)
    val numTasks = if (args.length > 0) args(0).toInt else 160
    val itemsPerPartition = if (args.length > 1) args(1).toInt else 1500000
    val longsPerValue = if (args.length > 2) args(2).toInt else 5
    def numShuffles = if (args.length > 3) args(3).toInt else 2
    val rdd = spark.parallelize(1 to numTasks, numTasks).flatMap { _ =>
      Array.fill(itemsPerPartition)((Random.nextLong, Array.fill(longsPerValue)(Random.nextLong)))
    }
    // The goal here is just to shuffle the data with minimal computation, so this doesn't sort
    // the shuffled data. The reduceByKey should result in very few keys being combined, because
    // the number of items generated is small relative to the space of all longs.
    val shuffledRdd = rdd.reduceByKey((a, b) => b)
    println("Generating and caching original RDD")
    rdd.cache.count
    shuffledRdd.count

    import ExecutionContext.Implicits.global
    val computeRunnable = future {
      spark.parallelize(1 to 4, 4).flatMap { _ =>
        Array.fill(itemsPerPartition * 10)(
          (Random.nextLong, Array.fill(longsPerValue)(Random.nextLong)))
      }.count
    }

    // Now, concurrently run two experiments.
    val shuffleRunnable = future {
      println("Running shuffle twice")
      (1 to numShuffles).foreach { _ =>
        shuffledRdd.count
      }
    }

    Await.result(shuffleRunnable, 10 minutes)
    Await.result(computeRunnable, 10 minutes)
    spark.stop()
  }
}
