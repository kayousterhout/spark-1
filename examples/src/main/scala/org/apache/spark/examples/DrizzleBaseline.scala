// scalastyle:off println
package org.apache.spark.examples

import scala.math.random

import org.apache.spark._

/** Computes an approximation to pi */
object DrizzleBaseline {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Drizzle Baseline")
    val sc = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(1000000L * slices, Int.MaxValue).toInt // avoid overflow
    val sum = sc.parallelize(1 to n, slices). map {i => 2 * i}.
              treeReduce(_ + _)
    println("Sum is " + sum)
    sc.stop()
    //val count = sc.parallelize(0 until n, slices).map { i =>
      //2 * i
    //}.reduce(_ + _)
    //println("Pi is roughly " + 4.0 * count / n)
  }
}
