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

import org.apache.spark.{Partition, ShuffleDependency, TaskContextImpl}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.monotasks.Monotask
import org.apache.spark.monotasks.compute.{ExecutionMonotask, ShuffleMapMonotask}
import org.apache.spark.monotasks.disk.DiskWriteMonotask
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Describes a group of monotasks that will divides the elements of an RDD into multiple buckets
 * (based on a partitioner specified in the ShuffleDependency) and stores the result in the
 * BlockManager.
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

  override def getExecutionMonotask(context: TaskContextImpl): (RDD[_], ExecutionMonotask[_, _]) = {
    val ser = context.env.closureSerializer.newInstance()
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[Any, Any, _])](
      ByteBuffer.wrap(taskBinary.value), context.dependencyManager.replClassLoader)
    (rdd, new ShuffleMapMonotask(context, rdd, partition, dep))
  }

  override def getMonotasks(context: TaskContextImpl): Seq[Monotask] = {
    val ser = context.env.closureSerializer.newInstance()
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[Any, Any, _])](
      ByteBuffer.wrap(taskBinary.value), context.dependencyManager.replClassLoader)
    val shuffleMapMonotask = new ShuffleMapMonotask(context, rdd, partition, dep)

    // Create one disk write monotask for each shuffle block.
    val diskWriteMonotasks = shuffleMapMonotask.getResultBlockIds().map { shuffleBlockId =>
      // Use the same block id for the in-memory as on-disk data! Because later, we may actually
      // want to just leave some of the data in-memory.
      val diskWriteMonotask = new DiskWriteMonotask(
        context, shuffleBlockId, shuffleBlockId, StorageLevel.DISK_ONLY)
      diskWriteMonotask.addDependency(shuffleMapMonotask)
      diskWriteMonotask
    }

    val rddMonotasks = rdd.buildDag(partition, dependencyIdToPartitions, context, shuffleMapMonotask)
    val allMonotasks = Seq(shuffleMapMonotask) ++ rddMonotasks ++ diskWriteMonotasks
    addResultSerializationMonotask(context, shuffleMapMonotask.getResultBlockId(), allMonotasks)
  }
}
