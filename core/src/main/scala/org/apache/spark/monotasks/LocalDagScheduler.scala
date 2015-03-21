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

import java.nio.ByteBuffer

import scala.collection.mutable.{HashMap, HashSet}

import org.apache.spark.{Logging, SparkConf, TaskState}
import org.apache.spark.executor.ExecutorBackend
import org.apache.spark.monotasks.compute.{ComputeMonotask, ComputeScheduler}
import org.apache.spark.monotasks.disk.{DiskMonotask, DiskScheduler}
import org.apache.spark.monotasks.network.{NetworkMonotask, NetworkScheduler}
import org.apache.spark.storage.BlockManager

/**
 * LocalDagScheduler tracks running and waiting monotasks. When all of a monotask's
 * dependencies have finished executing, the LocalDagScheduler will submit the monotask
 * to the appropriate scheduler to be executed once sufficient resources are available.
 *
 * TODO: The LocalDagScheduler should implement thread safety using an actor or event loop, rather
 *       than having all methods be synchronized (which can lead to monotasks that block waiting
 *       for the LocalDagScheduler).
 */
private[spark] class LocalDagScheduler(
    executorBackend: ExecutorBackend,
    conf: SparkConf,
    val blockManager: BlockManager)
  extends Logging {

  val computeScheduler = new ComputeScheduler(executorBackend)
  val networkScheduler = new NetworkScheduler(conf)
  val diskScheduler = new DiskScheduler(blockManager)

  /* IDs of monotasks that are waiting for dependencies to be satisfied. This exists solely for
   * debugging/testing and is not needed for maintaining correctness. */
  val waitingMonotasks = new HashSet[Long]()

  /* IDs of monotasks that have been submitted to a scheduler to be run. This exists solely for
   * debugging/testing and is not needed for maintaining correctness. */
  val runningMonotasks = new HashSet[Long]()

  /* Maps macrotask attempt ID to the IDs of monotasks that are part of that macrotask but have not
   * finished executing yet. A macrotask is not finished until its set is empty and it has a result.
   * This is also used to determine whether to notify the executor backend that a task has failed
   * (used to avoid duplicate failure messages if multiple monotasks for the macrotask fail). */
  val macrotaskRemainingMonotasks = new HashMap[Long, HashSet[Long]]()

  /* Maps macrotask attempt ID to that macrotask's serialized task result. This gives the
   * LocalDagScheduler a way to store macrotask result buffers in the event that the monotask that
   * creates the result is not the last monotask to execute for that macrotask (the macrotask cannot
   * return its result until all of its monotasks have finished). */
  val macrotaskResults = new HashMap[Long, ByteBuffer]()

  def getNumRunningComputeMonotasks(): Int = {
    computeScheduler.numRunningTasks.get()
  }

  def getNumRunningMacrotasks(): Int = {
    macrotaskRemainingMonotasks.keySet.size
  }

  def submitMonotask(monotask: Monotask) = synchronized {
    if (monotask.dependencies.isEmpty) {
      scheduleMonotask(monotask)
    } else {
      waitingMonotasks += monotask.taskId
    }
    val taskAttemptId = monotask.context.taskAttemptId
    logDebug(s"Submitting monotask $monotask (id: ${monotask.taskId}) for macrotask $taskAttemptId")
    macrotaskRemainingMonotasks.getOrElseUpdate(taskAttemptId, new HashSet[Long]()) +=
      monotask.taskId
  }

  /** It is assumed that all monotasks for a specific macrotask are submitted at the same time. */
  def submitMonotasks(monotasks: Seq[Monotask]) = synchronized {
    monotasks.foreach(submitMonotask(_))
  }

  /**
   * Marks the monotask as successfully completed by updating the dependency tree and running any
   * newly runnable monotasks.
   *
   * @param completedMonotask The monotask that has completed.
   * @param serializedTaskResult If the monotask was the final monotask for the macrotask, a
   *                             serialized TaskResult to be sent to the driver (None otherwise).
   */
  def handleTaskCompletion(
      completedMonotask: Monotask,
      serializedTaskResult: Option[ByteBuffer] = None) = synchronized {
    val taskAttemptId = completedMonotask.context.taskAttemptId
    logDebug(s"Monotask $completedMonotask (id: ${completedMonotask.taskId}) for " +
      s"macrotask $taskAttemptId has completed.")
    runningMonotasks.remove(completedMonotask.taskId)

    if (macrotaskRemainingMonotasks.contains(taskAttemptId)) {
      completedMonotask.dependents.foreach { monotask =>
        monotask.dependencies -= completedMonotask.taskId
        if (monotask.dependencies.isEmpty) {
          assert(
            waitingMonotasks.contains(monotask.taskId),
            "Monotask dependencies should only include tasks that have not yet run")
          scheduleMonotask(monotask)
        }
      }

      if ((macrotaskRemainingMonotasks(taskAttemptId) -= completedMonotask.taskId).isEmpty) {
        // All monotasks for this macrotask have completed, so send the result to the
        // executorBackend.
        serializedTaskResult.orElse(macrotaskResults.get(taskAttemptId)).map { result =>
          completedMonotask.context.markTaskCompleted()
          logDebug(s"Notfiying executorBackend about successful completion of task $taskAttemptId")
          executorBackend.statusUpdate(taskAttemptId, TaskState.FINISHED, result)

          macrotaskRemainingMonotasks -= taskAttemptId
          macrotaskResults -= taskAttemptId
        }.getOrElse{
          logError(s"Macrotask $taskAttemptId does not have a result even though all of its " +
            "monotasks have completed.")
        }
      } else {
        // If we received a result, store it so it can be passed to the executorBackend once all of
        // the monotasks for this macrotask have completed.
        serializedTaskResult.foreach(macrotaskResults(taskAttemptId) = _)
      }
    } else {
      // Another monotask in this macrotask must have failed while completedMonotask was running,
      // causing the macrotask to fail and its taskAttemptId to be removed from
      // macrotaskRemainingMonotasks. We should fail completedMonotask's dependents in case they
      // have not been failed already, which can happen if they are not dependents of the monotask
      // that failed.
      failDependentMonotasks(completedMonotask)
    }
  }

  /**
   * Marks the monotask and all monotasks that depend on it as failed and notifies the executor
   * backend that the associated macrotask has failed.
   *
   * @param failedMonotask The monotask that failed.
   * @param serializedFailureReason A serialized TaskFailedReason describing why the task failed.
   */
  def handleTaskFailure(failedMonotask: Monotask, serializedFailureReason: ByteBuffer)
    = synchronized {
    runningMonotasks -= failedMonotask.taskId
    failDependentMonotasks(failedMonotask, Some(failedMonotask.taskId))
    val taskAttemptId = failedMonotask.context.taskAttemptId
    // Notify the executor backend that the macrotask has failed, if we didn't already.
    if (macrotaskRemainingMonotasks.remove(taskAttemptId).isDefined) {
      failedMonotask.context.markTaskCompleted()
      executorBackend.statusUpdate(taskAttemptId, TaskState.FAILED, serializedFailureReason)
    }

    macrotaskResults.remove(taskAttemptId)
  }

  private def failDependentMonotasks(
      monotask: Monotask,
      originalFailedTaskId: Option[Long] = None) {
    // TODO: We don't interrupt monotasks that are already running. See
    //       https://github.com/NetSys/spark-monotasks/issues/10
    val message = originalFailedTaskId.map { taskId =>
      s"it dependend on monotask $taskId, which failed"
    }.getOrElse(s"another monotask in macrotask ${monotask.context.taskAttemptId} failed")

    monotask.dependents.foreach { dependentMonotask =>
      logDebug(s"Failing monotask ${dependentMonotask.taskId} because $message.")
      waitingMonotasks -= dependentMonotask.taskId
      failDependentMonotasks(dependentMonotask, originalFailedTaskId)
    }
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
      case diskMonotask: DiskMonotask => diskScheduler.submitTask(diskMonotask)
      case _ => logError(s"Received unexpected type of monotask: $monotask")
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
    while (!this.synchronized(waitingMonotasks.isEmpty && runningMonotasks.isEmpty)) {
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
