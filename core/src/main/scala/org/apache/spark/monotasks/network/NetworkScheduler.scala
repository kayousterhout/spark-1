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

package org.apache.spark.monotasks.network

import scala.collection.mutable.{HashMap, Queue}

import org.apache.spark.{Logging, SparkConf, TaskState}
import org.apache.spark.storage.BlockManagerId

/**
 * Scheduler for network monotasks.
 *
 * Threading model: all of the methods in the NetworkScheduler need to be synchronized, because
 * they may be called by the LocalDagScheduler (e.g., submitMonotask) or by a NetworkMonotask
 * when data is received over the network.
 */
private[spark] class NetworkScheduler(conf: SparkConf) extends Logging {

  // Max megabytes of data to keep in flight (should be set to roughly saturate the network link
  // but not significantly exceed it, to ensure runtime predictability).
  // TODO: it would be better to do this based on network utilization.
  val maxOutstandingBytes = conf.getLong("spark.reducer.maxMbInFlight", 384) * 1024 * 1024
  private var currentOutstandingBytes = 0L

  // Do simple round robin. In the future, could do something more sophisticated, like should
  // ideally take request size into account.
  // TODO: currently nothing deleted from this ever.
  private val blockManagerIdToMonotasks = new HashMap[BlockManagerId, Queue[NetworkMonotask]]()
  // So we can reliably index in.
  private var blockManagerIds = Seq.empty[BlockManagerId]
  private var currentIndex = 0

  // Used only for monitoring and debugging.
  private var numWaitingMonotasks = 0

  // TODO: synchronize?
  private var waitingBytes = 0L

  // This isn't synchronized since it's only used for monitoring.
  def getOutstandingBytes: Long = currentOutstandingBytes

  def getWaitingAndOutstandingBytes(): Long = (currentOutstandingBytes + waitingBytes)

  def submitTask(monotask: NetworkMonotask) = synchronized {
    numWaitingMonotasks += 1
    waitingBytes += monotask.totalResultSize
    monotask.context.updateTaskState(TaskState.RUNNING_NON_COMPUTE)
    if (!blockManagerIdToMonotasks.contains(monotask.remoteAddress)) {
      blockManagerIdToMonotasks(monotask.remoteAddress) = new Queue[NetworkMonotask]()
      blockManagerIds = blockManagerIdToMonotasks.keys.toSeq
      // Set to the new one. not perfect.
      currentIndex = blockManagerIds.indexOf(monotask.remoteAddress)
    }
    blockManagerIdToMonotasks(monotask.remoteAddress) += monotask
    maybeLaunchTasks()
  }

  def bytesReceived(totalBytes: Long) = synchronized {
    currentOutstandingBytes -= totalBytes
    maybeLaunchTasks()
  }

  private def incrementCurrentIndex() {
    val before = currentIndex
    currentIndex = (currentIndex + 1) % blockManagerIds.size
  }

  private[spark] def maybeLaunchTasks() {
    if (numWaitingMonotasks == 0) {
      return
    }

    // Move currentIndex to the next block manager that we have monotasks for.
    while (blockManagerIdToMonotasks(blockManagerIds(currentIndex)).isEmpty) {
      incrementCurrentIndex()
    }

    val blockManagerId = blockManagerIds(currentIndex)
    val monotaskQueue = blockManagerIdToMonotasks(blockManagerId)
    monotaskQueue.headOption.map { monotask =>
      if (currentOutstandingBytes == 0L ||
        currentOutstandingBytes + monotask.totalResultSize <= maxOutstandingBytes) {
        if (monotask.totalResultSize > maxOutstandingBytes) {
          logWarning(s"NetworkMonotask ${monotask.taskId} has request size of " +
            s"${monotask.totalResultSize}, which is larger than the maximum number of " +
            s"outstanding bytes ($maxOutstandingBytes)")
        }
        currentOutstandingBytes += monotask.totalResultSize
        logInfo(s"Launching monotask ${monotask.taskId} for macrotask " +
          s"${monotask.context.taskAttemptId} on block manager $blockManagerId " +
          s"($currentOutstandingBytes bytes outstanding)")
        numWaitingMonotasks -= 1
        monotask.launch(this)
        waitingBytes -= monotask.totalResultSize
        monotaskQueue.dequeue()
        incrementCurrentIndex()
      }
    }
    logInfo(s"$numWaitingMonotasks remaining monotasks waiting to be scheduled")
  }
}
