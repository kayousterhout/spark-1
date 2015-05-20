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

package org.apache.spark.monotasks.compute

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.Logging
import org.apache.spark.executor.ExecutorBackend
import org.apache.spark.util.Utils

private[spark] sealed trait RunningTasksUpdate
private[spark] object TaskStarted extends RunningTasksUpdate
private[spark] object TaskCompleted extends RunningTasksUpdate

private[spark] class ComputeScheduler(executorBackend: ExecutorBackend) extends Logging {
  private val threads = Runtime.getRuntime.availableProcessors()

  // TODO: This threadpool currently uses a single FIFO queue when the number of tasks exceeds the
  //       number of threads; eventually, we'll want a smarter queueing strategy.
  private val computeThreadpool = Utils.newDaemonFixedThreadPool(threads, "compute-monotask-thread")
  private val workQueue = computeThreadpool.getQueue()

  val numRunningTasks = new AtomicInteger(0)

  logDebug(s"Started ComputeScheduler with $threads parallel threads")

  private def updateRunningTasksAndNotifyBackend(updateType: RunningTasksUpdate) {
    val currentlyRunningTasks = updateType match {
      case TaskStarted =>
        numRunningTasks.incrementAndGet()
      case TaskCompleted =>
        numRunningTasks.decrementAndGet()
    }
    val freeCores = threads - currentlyRunningTasks - workQueue.size()
    // TODO: We may want to consider updating the driver less frequently, otherwise these messages
    //       to the driver may become a bottleneck.
    executorBackend.updateFreeCores(freeCores)
  }

  def submitTask(monotask: ComputeMonotask) {
    computeThreadpool.execute(new Runnable {
      override def run(): Unit = {
        updateRunningTasksAndNotifyBackend(TaskStarted)
        monotask.executeAndHandleExceptions()
        updateRunningTasksAndNotifyBackend(TaskCompleted)
      }
    })
  }
}
