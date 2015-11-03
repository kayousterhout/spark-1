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

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.storage.StorageLevel

/**
 * A FutureTask Notifier implements methods to push either shuffle blocks
 * or MapStatus information about the shuffle blocks to the next stage
 */
private[spark] object FutureTaskNotifier extends Logging {

  def taskCompleted(
      status: MapStatus,
      mapId: Int,
      shuffleId: Int,
      numReduces: Int,
      nextStageLocs: Option[Seq[BlockManagerId]],
      shuffleWriteMetrics: Option[ShuffleWriteMetrics],
      skipZeroByteNotifications: Boolean): Unit = {
    if (!nextStageLocs.isEmpty && numReduces == nextStageLocs.get.length) {
      val drizzleRpcsStart = System.nanoTime
      val pushShuffleData = SparkEnv.get.conf.getBoolean("spark.scheduler.drizzle.push", true)
      if (pushShuffleData) {
        uploadOutputToNextTaskLocations(status, mapId, shuffleId, numReduces, nextStageLocs,
          skipZeroByteNotifications)
      } else {
        sendMapStatusToNextTaskLocations(status, mapId, shuffleId, numReduces, nextStageLocs,
          skipZeroByteNotifications)
      }
      shuffleWriteMetrics.map(_.incShuffleWriteTime(System.nanoTime -
        drizzleRpcsStart))
    }
  }

  // Push metadata saying that this map task finished, so that the tasks in the next stage
  // know they can begin pulling the data.
  private def sendMapStatusToNextTaskLocations(
      status: MapStatus,
      mapId: Int,
      shuffleId: Int, 
      numReduces: Int,
      nextStageLocs: Option[Seq[BlockManagerId]],
      skipZeroByteNotifications: Boolean) {
    logDebug(s"DRIZ: In ShuffleMapTask map ${mapId} shuffle $shuffleId, sending " +
      "MapOutputStatus to all reduce locs")
    val numReduces = nextStageLocs.get.length
    val uniqueLocations = if (skipZeroByteNotifications) {
      nextStageLocs.get.zipWithIndex.filter { x =>
        status.getSizeForBlock(x._2) != 0L
      }.map(_._1).toSet
    } else {
      nextStageLocs.get.toSet
    }
    uniqueLocations.foreach { blockManagerId =>
      SparkEnv.get.blockTransferService.mapOutputReady(
        blockManagerId.host,
        blockManagerId.port,
        shuffleId,
        mapId,
        numReduces,
        status)
    }
  }

  /**
   * If we have the locations for the tasks in the next stage, pushes the shuffle blocks output by
   * this task to the appropriate next task.
   */
  private def uploadOutputToNextTaskLocations(
      status: MapStatus,
      mapId: Int,
      shuffleId: Int, 
      numReduces: Int,
      nextStageLocs: Option[Seq[BlockManagerId]],
      skipZeroByteNotifications: Boolean) {
    // TODO(shivaram): These calls are all non-blocking. Should we wait for any of them ?
    // NOTE(shivaram): Assumes reduce ids are from 0 to n-1
    nextStageLocs.get.zipWithIndex.foreach {
      case (loc, reduceId) =>
        // TODO(shivaram): Don't make this copy if loc is same as local block manager
        if (! (skipZeroByteNotifications && status.getSizeForBlock(reduceId) == 0L) ) {
          val blockId = ShuffleBlockId(shuffleId, mapId, reduceId)
          logDebug(s"DRIZ: Sending $blockId to $loc")
          SparkEnv.get.blockTransferService.uploadBlock(
            loc.host,
            loc.port,
            loc.executorId,
            blockId,
            SparkEnv.get.shuffleManager.shuffleBlockResolver.getBlockData(blockId),
            // TODO: Will this get freed correctly ??
            StorageLevel.MEMORY_ONLY)
        }
    }
  }
}
