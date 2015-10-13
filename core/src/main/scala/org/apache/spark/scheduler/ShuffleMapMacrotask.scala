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

package org.apache.spark.scheduler

import java.nio.ByteBuffer

import scala.collection.mutable.{HashMap, HashSet}
import scala.language.existentials

import org.apache.spark.{Partition, ShuffleDependency, SparkEnv, TaskContextImpl}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.monotasks.Monotask
import org.apache.spark.monotasks.compute.ShuffleMapMonotask
import org.apache.spark.monotasks.disk.DiskWriteMonotask
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.MultipleShuffleBlocksId

/**
 * Describes a group of monotasks that will divide the elements of an RDD into multiple buckets
 * (based on a partitioner specified in the ShuffleDependency) and stores the result on disk.
 */
private[spark] class ShuffleMapMacrotask(
    stageId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    dependencyIdToPartitions: HashMap[Long, HashSet[Partition]],
    @transient private var locs: Seq[TaskLocation])
  extends Macrotask[MapStatus](stageId, partition, dependencyIdToPartitions) {

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }
  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partition.index)

  override def getMonotasks(context: TaskContextImpl): Seq[Monotask] = {
    // First, construct a ShuffleMapMonotask, which converts an RDD into a set of shuffle blocks
    // that each have serialized (and possibly compressed) data.
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[Any, Any, _])](
      ByteBuffer.wrap(taskBinary.value), SparkEnv.get.dependencyManager.replClassLoader)
    val shuffleMapMonotask = new ShuffleMapMonotask(context, rdd, partition, dep)

    // Create one disk write monotask that will write all of the shuffle blocks.
    val blockId = shuffleMapMonotask.shuffleDataId
    val diskWriteMonotask = new DiskWriteMonotask(context, blockId, blockId)
    diskWriteMonotask.addDependency(shuffleMapMonotask)

    // Create the monotasks that will generate the RDD to be shuffled.
    val rddMonotasks = rdd.buildDag(
      partition, dependencyIdToPartitions, context, shuffleMapMonotask)

    // Create a monotask to serialize the result and return all of the monotasks we've created.
    val allMonotasks = Seq(shuffleMapMonotask, diskWriteMonotask) ++ rddMonotasks
    addResultSerializationMonotask(context, shuffleMapMonotask.getResultBlockId(), allMonotasks)
  }
}
