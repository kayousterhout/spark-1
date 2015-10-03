// scalastyle:off println
package org.apache.spark.examples

import scala.math.random

import org.apache.spark._

object DrizzleBaseline {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Drizzle Baseline")
    val sc = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val depth = if (args.length > 1) args(1).toInt else 3
    val n = math.min(1000000L * slices, Long.MaxValue).toInt // avoid overflow
    val sum = sc.parallelize(1L to n, slices). map {i => 2L * i}.
              treeReduce(_ + _, depth)
    println("Sum is " + sum)
    sc.stop()
  }
}
