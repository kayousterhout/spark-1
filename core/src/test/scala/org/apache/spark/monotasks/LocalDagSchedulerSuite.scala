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

import org.mockito.Mockito._

import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

import org.apache.spark.TaskContext
import org.apache.spark.executor.ExecutorBackend

class LocalDagSchedulerSuite extends FunSuite with Matchers with BeforeAndAfterEach {
  private var executorBackend: ExecutorBackend = _
  private var localDagScheduler: LocalDagScheduler = _

  override def beforeEach() {
    executorBackend = mock(classOf[ExecutorBackend])
    localDagScheduler = new LocalDagScheduler(executorBackend)
  }

  test("submitMonotasks: tasks with no dependencies are run immediately") {
    val noDependencyMonotask = new SimpleMonotask(0)
    localDagScheduler.submitMonotasks(List(noDependencyMonotask))

    assert(localDagScheduler.waitingMonotasks.isEmpty)
    assert(1 === localDagScheduler.runningMonotasks.size)
    assert(localDagScheduler.runningMonotasks.contains(noDependencyMonotask.taskId))
  }

  test("submitMonotasks: tasks with unsatisfied dependencies are not run immediately") {
    val firstMonotask = new SimpleMonotask(0)
    val secondMonotask = new SimpleMonotask(0)
    secondMonotask.addDependency(firstMonotask)
    localDagScheduler.submitMonotasks(List(firstMonotask, secondMonotask))

    assert(1 === localDagScheduler.runningMonotasks.size)
    assert(localDagScheduler.runningMonotasks.contains(firstMonotask.taskId))
    assert(1 === localDagScheduler.waitingMonotasks.size)
    assert(localDagScheduler.waitingMonotasks.contains(secondMonotask.taskId))
  }

  test("handleTaskCompletion results in appropriate new monotasks being run") {
    val firstMonotask = new SimpleMonotask(0)
    val secondMonotask = new SimpleMonotask(0)
    secondMonotask.addDependency(firstMonotask)
    localDagScheduler.submitMonotasks(List(firstMonotask, secondMonotask))
    localDagScheduler.handleTaskCompletion(firstMonotask)

    assert(1 === localDagScheduler.runningMonotasks.size)
    assert(localDagScheduler.runningMonotasks.contains(secondMonotask.taskId))
    assert(localDagScheduler.waitingMonotasks.isEmpty)
  }

  /**
   * Tests that when a serialized task result is provided to handleTaskCompletion(), the local
   * DAG scheduler removes the associated macrotask from the set of running macrotasks, and also
   * updates the executor backend that the task has finished.
   */
  test("handleTaskCompletion works correctly when a serializedTaskResult is provided") {

  }

  /**
   * Tests a more complicated case where monotasks form the following DAG (where E depends on
   * C and D, and C depends on A and B).
   *
   *     A --,
   *          >-- C --,
   *     B --'         >-- E
   *              D --'
   */
  test("handleTaskCompletion properly handles complex DAGs") {
    val monotaskA = new SimpleMonotask(0)
    val monotaskB = new SimpleMonotask(0)
    val monotaskC = new SimpleMonotask(0)
    val monotaskD = new SimpleMonotask(0)
    val monotaskE = new SimpleMonotask(0)

    monotaskC.addDependency(monotaskA)
    monotaskC.addDependency(monotaskB)
    monotaskE.addDependency(monotaskC)
    monotaskE.addDependency(monotaskD)

    localDagScheduler.submitMonotasks(List(monotaskA, monotaskB, monotaskC, monotaskD, monotaskE))

    // At first, tasks A, B, and D should be running.
    assert(3 === localDagScheduler.runningMonotasks.size)
    assert(localDagScheduler.runningMonotasks.contains(monotaskA.taskId))
    assert(localDagScheduler.runningMonotasks.contains(monotaskB.taskId))
    assert(localDagScheduler.runningMonotasks.contains(monotaskD.taskId))
    assert(2 === localDagScheduler.waitingMonotasks.size)

    // When D finishes, no new tasks should be run, because E still depends on task C.
    localDagScheduler.handleTaskCompletion(monotaskD)
    assert(2 === localDagScheduler.runningMonotasks.size)
    assert(2 === localDagScheduler.waitingMonotasks.size)

    // Similarly, when B finishes, no new tasks should be run, because C still depends on A.
    localDagScheduler.handleTaskCompletion(monotaskB)
    assert(1 === localDagScheduler.runningMonotasks.size)
    assert(2 === localDagScheduler.waitingMonotasks.size)

    // When A finishes, C should be run.
    localDagScheduler.handleTaskCompletion(monotaskA)
    assert(1 === localDagScheduler.runningMonotasks.size)
    assert(localDagScheduler.runningMonotasks.contains(monotaskC.taskId))
    assert(1 === localDagScheduler.waitingMonotasks.size)
    assert(localDagScheduler.waitingMonotasks.contains(monotaskE.taskId))

    // Finally, when C finishes, E can be run.
    localDagScheduler.handleTaskCompletion(monotaskC)
    assert(localDagScheduler.waitingMonotasks.isEmpty)
    assert(1 === localDagScheduler.runningMonotasks.size)
    assert(localDagScheduler.runningMonotasks.contains(monotaskE.taskId))

    // Make a dummy result to pass in as part of the task completion to signal that the macrotask
    // has completed.
    val result = ByteBuffer.allocate(2)
    localDagScheduler.handleTaskCompletion(monotaskE, Some(result))
    assertDataStructuresEmpty()
  }

  test("waitUntilAllTasksComplete returns immediately when no tasks are running or waiting") {
    assert(localDagScheduler.waitUntilAllTasksComplete(0))
  }

  test("waitUntilAllTasksComplete waits for all tasks to complete") {
    // Create a simple DAG with two tasks.
    val firstMonotask = new SimpleMonotask(0)
    val secondMonotask = new SimpleMonotask(0)
    secondMonotask.addDependency(firstMonotask)
    localDagScheduler.submitMonotasks(List(firstMonotask, secondMonotask))

    assert(!localDagScheduler.waitUntilAllTasksComplete(10))

    localDagScheduler.handleTaskCompletion(firstMonotask)
    assert(!localDagScheduler.waitUntilAllTasksComplete(10))

    // Make a dummy result to pass in as part of the task completion to signal that the macrotask
    // has completed.
    val result = ByteBuffer.allocate(2)
    localDagScheduler.handleTaskCompletion(secondMonotask, Some(result))
    assert(localDagScheduler.waitUntilAllTasksComplete(10))

    assertDataStructuresEmpty()
  }

  // TODO: need this??
  private def assertDataStructuresEmpty() = {
    assert(localDagScheduler.waitingMonotasks.isEmpty)
    assert(localDagScheduler.runningMonotasks.isEmpty)
    assert(localDagScheduler.runningMacrotaskAttemptIds.isEmpty)
  }
}