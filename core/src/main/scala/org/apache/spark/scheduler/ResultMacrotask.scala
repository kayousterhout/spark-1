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

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext, TaskGoop}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.monotasks.Monotask
import org.apache.spark.monotasks.compute.ResultMonotask
import org.apache.spark.rdd.RDD

/**
 * Describes a group of monotasks that will use the input RDD to compute a result (e.g., the count
 * of elements) to the driver application.
 */
private[spark] class ResultMacrotask[T, U: ClassTag](
    stageId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient locs: Seq[TaskLocation],
    val outputId: Int)
  extends Macrotask[U](stageId, partition) with Serializable {

  @transient private[this] val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }
  // This is only callable on the driver side.
  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString = s"ResultTask($stageId, ${partition.index})"

  // Deserializes the task binary and creates the rest of the monotasks needed to run the
  // macrotask.
  override def getMonotasks(goop: TaskGoop): Seq[Monotask] = {
    // TODO: Task.run() setups up TaskContext and sets hostname in metrics; need to do that here!
    val ser = goop.env.closureSerializer.newInstance()
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), goop.dependencyManager.replClassLoader)

    goop.setContext(stageId, partition.index)
    val inputMonotasks: Seq[Monotask] =
      rdd.dependencies.flatMap(_.getMonotasks(goop, partition.index))
    val computeMonotask = new ResultMonotask(goop, rdd, partition, func)
    // Create dependency graph: compute monotask depends on all input monotasks.
    inputMonotasks.foreach(computeMonotask.addDependency(_))

    inputMonotasks ++ Seq(computeMonotask)
  }
}
