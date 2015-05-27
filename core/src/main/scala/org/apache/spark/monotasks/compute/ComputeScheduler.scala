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

import java.util.Comparator
import java.util.concurrent.{PriorityBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.executor.ExecutorBackend
import org.apache.spark.util.Utils
import org.apache.spark.monotasks.MonotaskRunnable

private[spark] sealed trait RunningTasksUpdate
private[spark] object TaskStarted extends RunningTasksUpdate
private[spark] object TaskCompleted extends RunningTasksUpdate

private class PrepareMonotasksFirst extends Comparator[Runnable] {

  private def isPrepareMonotask(runnable: MonotaskRunnable) =
    runnable.monotask.isInstanceOf[PrepareMonotask]

  override def compare(runnable1: Runnable, runnable2: Runnable): Int = {
    val monotaskRunnable1 = runnable1.asInstanceOf[MonotaskRunnable]
    val monotaskRunnable2 = runnable2.asInstanceOf[MonotaskRunnable]

    val runnable1IsPrepare = isPrepareMonotask(monotaskRunnable1)
    val runnable2IsPrepare = isPrepareMonotask(monotaskRunnable2)
    if (runnable1IsPrepare && !runnable2IsPrepare) {
      return -1
    } else if (runnable2IsPrepare && !runnable1IsPrepare) {
      return 1
    } else {
      return (monotaskRunnable1.monotask.taskId - monotaskRunnable2.monotask.taskId).toInt
    }
  }
}

private[spark] class ComputeScheduler(
    executorBackend: ExecutorBackend,
    sparkConf: SparkConf,
    threads: Int = Runtime.getRuntime.availableProcessors())
  extends Logging {

  // TODO: This threadpool currently uses a single FIFO queue when the number of tasks exceeds the
  //       number of threads; eventually, we'll want a smarter queueing strategy.
  private val threadFactory = Utils.namedThreadFactory("compute-monotask-thread")
  private val workQueue = new PriorityBlockingQueue[Runnable](11, new PrepareMonotasksFirst)
  private val computeThreadpool = new ThreadPoolExecutor(
    threads, threads, 0L, TimeUnit.MILLISECONDS, workQueue, threadFactory)

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

  def submitTask(computeMonotask: ComputeMonotask) {
    computeThreadpool.execute(new MonotaskRunnable(computeMonotask) {
      override def run(): Unit = {
        updateRunningTasksAndNotifyBackend(TaskStarted)
        computeMonotask.executeAndHandleExceptions()
        updateRunningTasksAndNotifyBackend(TaskCompleted)
      }
    })
  }
}
