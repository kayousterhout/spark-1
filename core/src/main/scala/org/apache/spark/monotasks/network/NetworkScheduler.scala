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

import scala.collection.mutable.{ArrayBuffer, HashMap, Queue}

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
  private var currentOutstandingBytes = 0L

  private var currentMacrotaskId = -1L
  private val macrotaskQueue = new Queue[Long]()
  private val macrotaskIdToMonotasks = new HashMap[Long, ArrayBuffer[NetworkMonotask]]

  // TODO: synchronize?
  private var waitingBytes = 0L
  var currentStartTimeNanos = 0L

  // This isn't synchronized since it's only used for monitoring.
  def getOutstandingBytes: Long = currentOutstandingBytes

  def getWaitingAndOutstandingBytes(): Long = (currentOutstandingBytes + waitingBytes)

  def submitTask(monotask: NetworkMonotask) = synchronized {
    monotask.context.updateTaskState(TaskState.RUNNING_NON_COMPUTE)
    val macrotaskId = monotask.context.taskAttemptId
    // Launch if no monotasks are currently running, or if this has the same id as the currently
    // running macrotask.
    if (currentOutstandingBytes == 0 || macrotaskId == currentMacrotaskId) {
      monotask.launch(this)
      if (currentOutstandingBytes == 0) {
        currentMacrotaskId = macrotaskId
        currentStartTimeNanos = System.nanoTime
      }
      currentOutstandingBytes += monotask.totalResultSize
    } else {
      waitingBytes += monotask.totalResultSize
      if (!macrotaskIdToMonotasks.contains(macrotaskId)) {
        macrotaskIdToMonotasks(macrotaskId) = new ArrayBuffer[NetworkMonotask]()
        macrotaskQueue += macrotaskId
      }
      macrotaskIdToMonotasks(macrotaskId) += monotask
    }
  }

  def bytesReceived(totalBytes: Long) = synchronized {
    currentOutstandingBytes -= totalBytes

    if (currentOutstandingBytes == 0 && !macrotaskQueue.isEmpty) {
      currentStartTimeNanos = System.nanoTime
      if (!macrotaskQueue.isEmpty) {
        // We can launch monotasks for the next macrotask.
        // TODO: consider starting monotasks for the next macrotask when the outstanding bytes to
        // any one executor is low? Or does that introduce annoying load balancing?
        currentMacrotaskId = macrotaskQueue.dequeue()
        macrotaskIdToMonotasks(currentMacrotaskId).foreach { monotask =>
          monotask.launch(this)
          currentOutstandingBytes += monotask.totalResultSize
          waitingBytes -= monotask.totalResultSize
        }
        macrotaskIdToMonotasks.remove(currentMacrotaskId)
      }
    }
  }
}
