// scalastyle:off println
package org.apache.spark.examples

import scala.math.random

import org.apache.spark._

object DrizzleBaseline {
  def main(args: Array[String]) {
    val NUM_TRIALS = 5
    val slices = if (args.length > 0) args(0).toInt else 2
    val depth = if (args.length > 1) args(1).toInt else 3    
    val n = math.min(1000000L * slices, Long.MaxValue).toInt // avoid overflow

    val conf = new SparkConf()
    if (conf.getBoolean("spark.scheduler.drizzle", true)) {
      conf.setAppName("Drizzle")
    } else {
      conf.setAppName("Baseline")
    }
    val sc = new SparkContext(conf)

    // Let all the executors join
    Thread.sleep(5000)
    // Warm up the JVM and copy the JAR out to all the machines etc.
    sc.parallelize(0 until sc.getExecutorMemoryStatus.size,
      sc.getExecutorMemoryStatus.size).foreach { x =>
      Thread.sleep(10)
    }

    val rdd = sc.parallelize(1L to n, slices).map(i => 2L * i).cache()
    rdd.localCheckpoint() // truncate the lineage to get rid of parallel collection task size
    rdd.count()

    for (i <- 0 to NUM_TRIALS) {
      val begin = System.nanoTime
      val sum = rdd.treeReduce(_ + _, depth)
      val end = System.nanoTime
      println("Sum of " + n + " elements took " + (end - begin)/1e3 + " microseconds")
    }
    sc.stop()
  }
}
