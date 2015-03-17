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

import scala.collection.mutable.Queue

import org.apache.spark.{Logging, SparkConf}

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
  val maxOutstandingBytes = conf.getLong("spark.reducer.maxMbInFlight", 48) * 1024 * 1024
  private var currentOutstandingBytes = 0L

  private val monotaskQueue = new Queue[NetworkMonotask]()

  def submitTask(monotask: NetworkMonotask) = synchronized {
    monotaskQueue += monotask
    maybeLaunchTasks()
  }

  def bytesReceived(totalBytes: Long) = synchronized {
    currentOutstandingBytes -= totalBytes
    maybeLaunchTasks()
  }

  private[spark] def maybeLaunchTasks() {
    monotaskQueue.headOption.map { monotask =>
      if (currentOutstandingBytes == 0L ||
        currentOutstandingBytes + monotask.totalResultSize < maxOutstandingBytes) {
        if (monotask.totalResultSize > maxOutstandingBytes) {
          logWarning(s"NetworkMonotask ${monotask.taskId} has request size of " +
            s"${monotask.totalResultSize}, which is larger than the maximum number of " +
            s"outstanding bytes ($maxOutstandingBytes)")
        }
        currentOutstandingBytes += monotask.totalResultSize
        logInfo(s"Launching monotask ${monotask.taskId} for macrotask " +
          "${monotask.context.taskAttemptId}")
        monotask.launch(this)
        monotaskQueue.dequeue()
      }
    }
  }
}
