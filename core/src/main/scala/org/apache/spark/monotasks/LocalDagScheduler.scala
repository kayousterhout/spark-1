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

package org.apache.spark.monotasks

import scala.collection.mutable.HashSet

import org.apache.spark.Logging
import org.apache.spark.monotasks.compute.{ComputeMonotask, ComputeScheduler}
import org.apache.spark.monotasks.network.{NetworkMonotask, NetworkScheduler}

/**
 * LocalDagScheduler tracks running and waiting monotasks. When all of a monotask's
 * dependencies have finished executing, the LocalDagScheduler will submit the monotask
 * to the appropriate scheduler to be executed once sufficient resources are available.
 */
private[spark] class LocalDagScheduler extends Logging {
  val computeScheduler = new ComputeScheduler
  val networkScheduler = new NetworkScheduler
  // TODO: Comment this back in once the disk scheduler has been added.
  // val diskScheduler = new DiskScheduler

  /* IDs of monotasks that are waiting for dependencies to be satisfied. This exists solely for
   * debugging/testing and is not needed for maintaining correctness. */
  val waitingMonotasks = new HashSet[Long]()

  /* IDs of monotasks that have been submitted to a scheduler to be run. This exists solely for
   * debugging/testing and is not needed for maintaining correctness. */
  val runningMonotasks = new HashSet[Long]()

  def submitMonotask(monotask: Monotask) {
    if (monotask.dependencies.isEmpty) {
      scheduleMonotask(monotask)
    } else {
      waitingMonotasks += monotask.taskId
    }
  }

  def submitMonotasks(monotasks: Seq[Monotask]) {
    monotasks.foreach(submitMonotask(_))
  }

  def handleTaskCompletion(completedMonotask: Monotask) {
    completedMonotask.dependents.foreach { monotask =>
      monotask.dependencies -= completedMonotask.taskId
      if (monotask.dependencies.isEmpty) {
        scheduleMonotask(monotask)
      }
    }
    runningMonotasks.remove(completedMonotask.taskId)
  }

  def handleTaskFailure(failedMonotask: Monotask) {

  }

  /**
   * Submits a monotask to the relevant scheduler to be executed. This method should only be called
   * after all of the monotask's dependencies have been satisfied.
   */
  private def scheduleMonotask(monotask: Monotask) {
    assert(monotask.dependencies.isEmpty)
    monotask match {
      case computeMonotask: ComputeMonotask => computeScheduler.submitTask(computeMonotask)
      case networkMonotask: NetworkMonotask => networkScheduler.submitTask(networkMonotask)
      // TODO: Comment this in once the disk scheduler has been added.
      // case diskMonotask: DiskMonotask => diskScheduler.submitTask(diskMonotask)
      case _ => logError("Received unexpected type of monotask")
    }
    /* Add the monotask to runningMonotasks before removing it from waitingMonotasks to avoid
     * a race condition in waitUntilAllTasksComplete where both sets are empty. */
    runningMonotasks += monotask.taskId
    waitingMonotasks.remove(monotask.taskId)
  }

  /**
   * For testing only. Waits until all monotasks have completed, or until the specified time has
   * elapsed. Returns true if all monotasks have completed and false if the specified amount of time
   * elapsed before all monotasks completed.
   */
  def waitUntilAllTasksComplete(timeoutMillis: Int): Boolean = {
    val finishTime = System.currentTimeMillis + timeoutMillis
    while (!(waitingMonotasks.isEmpty && runningMonotasks.isEmpty)) {
      if (System.currentTimeMillis > finishTime) {
        return false
      }
      /* Sleep rather than using wait/notify, because this is used only for testing and wait/notify
       * add overhead in the general case. */
      Thread.sleep(10)
    }
    true
  }
}
