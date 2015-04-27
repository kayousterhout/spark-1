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

package org.apache.spark

import org.scalatest.FunSuite
import org.scalatest.Matchers

import org.apache.spark.ShuffleSuite.NonJavaSerializableClass
import org.apache.spark.rdd.{CoGroupedRDD, OrderedRDDFunctions, RDD, ShuffledRDD, SubtractedRDD}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.{ShuffleDataBlockId, ShuffleBlockId}
import org.apache.spark.util.MutablePair

abstract class ShuffleSuite extends FunSuite with Matchers with LocalSparkContext {

  val conf = new SparkConf(loadDefaults = false)

  // Ensure that the DAGScheduler doesn't retry stages whose fetches fail, so that we accurately
  // test that the shuffle works (rather than retrying until all blocks are local to one Executor).
  conf.set("spark.test.noStageRetry", "true")

  test("groupByKey without compression") {
    val myConf = conf.clone().set("spark.shuffle.compress", "false")
    sc = new SparkContext("local", "test", myConf)
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1)), 4)
    val groups = pairs.groupByKey(4).collect()
    assert(groups.size === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))
  }

  test("shuffle non-zero block size") {
    sc = new SparkContext("local-cluster[2,1,512]", "test", conf)
    val NUM_BLOCKS = 3

    val a = sc.parallelize(1 to 10, 2)
    val b = a.map { x =>
      (x, new NonJavaSerializableClass(x * 2))
    }
    // If the Kryo serializer is not used correctly, the shuffle would fail because the
    // default Java serializer cannot handle the non serializable class.
    val c = new ShuffledRDD[Int,
      NonJavaSerializableClass,
      NonJavaSerializableClass](b, new HashPartitioner(NUM_BLOCKS))
    c.setSerializer(new KryoSerializer(conf))
    val shuffleId = c.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId

    assert(c.count === 10)

    // All blocks must have non-zero size
    (0 until NUM_BLOCKS).foreach { id =>
      val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, id)
      assert(statuses.forall(s => s._2 > 0))
    }
  }

}

object ShuffleSuite {

  def mergeCombineException(x: Int, y: Int): Int = {
    throw new SparkException("Exception for map-side combine.")
  }

  class NonJavaSerializableClass(val value: Int) extends Comparable[NonJavaSerializableClass] {
    override def compareTo(o: NonJavaSerializableClass): Int = {
      value - o.value
    }
  }
}
