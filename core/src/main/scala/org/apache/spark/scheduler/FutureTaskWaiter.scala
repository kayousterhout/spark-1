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

import scala.collection.mutable.HashSet

import org.apache.spark.Logging
import org.apache.spark.MapOutputTracker
import org.apache.spark.SparkConf
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.storage.BlockManager
import org.apache.spark.util.TimeStampedHashMap

private[spark] case class FutureTaskInfo(shuffleId: Int, numMaps: Int, reduceId: Int, taskId: Long,
  nonZeroPartitions: Option[Array[Int]], taskCb: () => Unit)

private[spark] class FutureTaskWaiter(
    conf: SparkConf,
    blockManager: BlockManager,
    mapOutputTracker: MapOutputTracker) extends Logging {

  // Key is (shuffleId, reduceId)
  private val futureTaskInfo = new TimeStampedHashMap[(Int, Int), FutureTaskInfo]
  // Key is (shuffleId, reduceId), value is the set of blockIds we are waiting for
  private val futureTasksBlockWait = new TimeStampedHashMap[(Int, Int), HashSet[Int]]

  // How many blocks to wait for
  private val fractionBlocksToWaitFor = conf.getDouble("spark.scheduler.drizzle.wait", 1.0)
  logInfo(s"DRIZ: Going to wait $fractionBlocksToWaitFor blocks")

  private val pushDrizzle = conf.getBoolean("spark.scheduler.drizzle.push", true)

  /**
   * Submits a future task that will get triggered when all the shuffle blocks have been
   * copied.
   */
  def submitFutureTask(info: FutureTaskInfo) {
    futureTasksBlockWait.synchronized {
      val blocksToWaitFor = if (info.nonZeroPartitions.isDefined) {
        info.nonZeroPartitions.get.toSet
      } else {
        (0 until info.numMaps).toArray.toSet
      }

      val blocksToWaitForSeq = blocksToWaitFor.toSeq

      // Check if all the blocks already exist. If so just trigger taskCb
      val availableBlocks = if (pushDrizzle) {
        // Count the number of blocks that are already in the BlockManager.
        val blockStatuses = blockManager.getStatuses(blocksToWaitForSeq.map(mapId => 
          ShuffleBlockId(info.shuffleId, mapId, info.reduceId)))
        blocksToWaitForSeq.zip(blockStatuses).filter { x =>
          x._2.isDefined
        }.map(_._1).toSet
      } else {
        // Count how many outputs have been registered with the MapOutputTracker for this shuffle
        // and intersect with blocksToWaitFor to only get how many for this reduce are available
        mapOutputTracker.getAvailableMapOutputs(info.shuffleId).intersect(blocksToWaitFor)
      }

      val mapsToWait = Math.ceil(blocksToWaitFor.size * fractionBlocksToWaitFor).toInt
      val numMapsPending = blocksToWaitFor.size - availableBlocks.size

      if (availableBlocks.size >= mapsToWait) {
        logDebug(s"DRIZ: Enough blocks ($availableBlocks of ${info.numMaps}) already exists for " +
          s"FutureTask ${info.taskId}. Returning")
        info.taskCb()
      } else {
        futureTaskInfo.put((info.shuffleId, info.reduceId), info)
        // NOTE: Its fine not to synchronize here as two future tasks shouldn't be submitted at the same time
        // Calculate the number of blocks to wait for before starting future task
        val waitForBlocks = blocksToWaitFor.diff(availableBlocks)
        futureTasksBlockWait.put((info.shuffleId, info.reduceId), new HashSet[Int]() ++ waitForBlocks)
        logDebug(s"DRIZ: For ${info.taskId} with ${info.numMaps} outputs going to wait for $waitForBlocks blocks. Already available $availableBlocks")
      }
    }
  }

  def shuffleBlockReady(shuffleBlockId: ShuffleBlockId): Unit = {
    val key = (shuffleBlockId.shuffleId, shuffleBlockId.reduceId)
    futureTasksBlockWait.synchronized {
      if (futureTaskInfo.contains(key)) {
        logDebug(s"Found FT for shuffle ${shuffleBlockId.shuffleId} and reduce ${shuffleBlockId.reduceId}")
        if (futureTasksBlockWait.contains(key)) {
          futureTasksBlockWait(key) -= shuffleBlockId.mapId
          logDebug(s"FT for shuffle ${shuffleBlockId.shuffleId}, reduce ${shuffleBlockId.reduceId} " +
            s"waiting for ${futureTasksBlockWait(key)} maps")
          // If the total number we need to wait for is greater than the remaining
          val numRemainingTaskAtTrigger = math.floor(futureTaskInfo(key).numMaps * (1.0 -
            fractionBlocksToWaitFor))
          if (futureTasksBlockWait(key).size <= numRemainingTaskAtTrigger) {
            val cb = futureTaskInfo(key).taskCb
            futureTasksBlockWait.remove(key)
            futureTaskInfo.remove(key)
            // TODO(shivaram): Run this in futureExecutionContext ?
            // This should be cheap though as its just queuing this
            logDebug(s"All maps recv. Running task for shuffle ${shuffleBlockId.shuffleId}, reduce "
              + s"${shuffleBlockId.reduceId}")
            cb()
          }
        }
      }
    }
  }

  def addMapStatusAvailable(shuffleId: Int, mapId: Int, numReduces: Int, mapStatus: MapStatus) {
    // NOTE: This should be done before we trigger future tasks.
    mapOutputTracker.addStatus(shuffleId, mapId, mapStatus)
    futureTasksBlockWait.synchronized {
      // Register the output for each reduce task.
      (0 until numReduces).foreach { reduceId =>
        shuffleBlockReady(new ShuffleBlockId(shuffleId, mapId, reduceId))
      }
    }
  }

}
