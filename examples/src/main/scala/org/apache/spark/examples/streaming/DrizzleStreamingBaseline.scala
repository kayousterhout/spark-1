/* Licensed to the Apache Software Foundation (ASF) under one or more
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
 * limitations under the License. */

package org.apache.spark.examples.streaming

import scala.util.Random
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

class RandoReceiver(range: Int)
  extends Receiver[Int](StorageLevel.MEMORY_ONLY) {
  def onStart() {
    new Thread("RandoReceiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    /* Nothing to do as the thread will die when isStopped is set true. */
  }

  private def receive() {
    val rand = new Random()
    while (!isStopped) {
      store(rand.nextInt(range))
    }
  }
}

object DrizzleStreamingBaseline {
  def main(args: Array[String]) {
    var batchSize = if (args.length > 0) args(0).toInt else 10
    var range = if (args.length > 1) args(1).toInt else 16

    val sparkConf = new SparkConf().setAppName("DrizzleStreamBaseline")
    val ssc = new StreamingContext(sparkConf, Seconds(batchSize))

    val nums = ssc.receiverStream(new RandoReceiver(16))

    val numFreq = nums.map(x => (x, 1)).reduceByKey(_ + _)

    numFreq.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
