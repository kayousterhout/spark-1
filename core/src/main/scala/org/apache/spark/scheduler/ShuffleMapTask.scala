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

package org.apache.spark.scheduler

import java.nio.ByteBuffer

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.storage.StorageLevel

/**
* A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
* specified in the ShuffleDependency).
*
* See [[org.apache.spark.scheduler.Task]] for more information.
*
 * @param stageId id of the stage this task belongs to
 * @param taskBinary broadcast version of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 * @param nextStageLocs task locations of next stage (for future tasks in Drizzle)
 *        TODO: Can we send BlockManagerIds or is that too expensive ?
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation],
    internalAccumulators: Seq[Accumulator[Long]],
    nextStageLocs: Option[Seq[BlockManagerId]] = None)
  extends Task[MapStatus](stageId, stageAttemptId, partition.index, internalAccumulators)
  with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) {
    this(0, 0, null, new Partition { override def index: Int = 0 }, null, null)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  var rdd: RDD[_] = null

  var dep: ShuffleDependency[_, _, _] = null

  override def prepTask(): Unit = {
    // Deserialize the RDD using the broadcast variable.
    val deserializeStartTime = System.currentTimeMillis()
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rddI, depI) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    rdd = rddI
    dep = depI
  }

  override def runTask(context: TaskContext): MapStatus = {
    if (dep == null || rdd == null) {
      prepTask()
    }

    metrics = Some(context.taskMetrics)
    var writer: ShuffleWriter[Any, Any] = null
    try {
      val manager = SparkEnv.get.shuffleManager
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      val status = writer.stop(success = true).get

      logDebug(s"DRIZ: In ShuffleMapTask ${partition.index} for stage $stageId, got " +
        s"${nextStageLocs.map(_.size).getOrElse(0)} locations")
      if (!nextStageLocs.isEmpty && dep.partitioner.numPartitions == nextStageLocs.get.length) {
        val pushShuffleData = SparkEnv.get.conf.getBoolean("spark.scheduler.drizzle.push", true)
        if (pushShuffleData) {
          uploadOutputToNextTaskLocations()
        } else {
          // Push metadata saying that this map task finished, so that the tasks in the next stage
          // know they can begin pulling the data.
          logDebug(s"DRIZ: In ShuffleMapTask ${partition.index} for stage $stageId, sending " +
            "MapOutputStatus to all reduce locs")
          val numReduces = nextStageLocs.get.length
          val uniqueLocations = nextStageLocs.get.toSet
          uniqueLocations.foreach { blockManagerId =>
            logDebug(s"DRIZ: Sending MapOutputStatus to ${blockManagerId.host}")
            // TODO: Directly tell the local block manager.
            SparkEnv.get.blockTransferService.mapOutputReady(
              blockManagerId.host,
              blockManagerId.port,
              dep.shuffleId,
              partition.index,
              numReduces,
              status)
          }
        }
      }
      status
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }


  /**
   * If we have the locations for the tasks in the next stage, pushes the shuffle blocks output by
   * this task to the appropriate next task. */
  private def uploadOutputToNextTaskLocations() {
    // TODO(shivaram): These calls are all non-blocking. Should we wait for any of them ?
    // NOTE(shivaram): Assumes reduce ids are from 0 to n-1
    nextStageLocs.get.zipWithIndex.foreach {
      case (loc, reduceId) =>
        // TODO(shivaram): Don't make this copy if loc is same as local block manager
        //if (loc != SparkEnv.get.blockManager.blockManagerId) {
        val blockId = ShuffleBlockId(dep.shuffleId, partitionId, reduceId)
        logDebug(s"DRIZ: Sending $blockId to $loc")
        SparkEnv.get.blockTransferService.uploadBlock(
          loc.host,
          loc.port,
          loc.executorId,
          blockId,
          SparkEnv.get.shuffleManager.shuffleBlockResolver.getBlockData(blockId),
          // TODO: Will this get freed correctly ??
          StorageLevel.MEMORY_ONLY)
      //}
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
