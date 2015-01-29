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

import org.mockito.Matchers.{any, eq => meq}
import org.mockito.Mockito._

import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.monotasks.LocalDagScheduler
import org.apache.spark.scheduler.LiveListenerBus

class ComputeMonotaskSuite extends FunSuite with BeforeAndAfterEach {

  var localDagScheduler: LocalDagScheduler = _
  var taskContext: TaskContext = _
  val taskMetrics: TaskMetrics = new TaskMetrics()

  override def beforeEach() {
    // Create a sparkEnv to be used to serialize results in ExecutionMonotasks.
    val listenerBus = new LiveListenerBus
    val sparkEnv = SparkEnv.create(
      new SparkConf(true), "testExecutor", "localhost", 30000, true, true, listenerBus)

    localDagScheduler = mock(classOf[LocalDagScheduler])
    taskContext = new TaskContext(sparkEnv, localDagScheduler, 500, null, 12)
    taskContext.initialize(0, 0)
  }

  test("executeAndHandleExceptions handles exceptions and notifies LocalDagScheduler of failure") {
    val monotask = new ComputeMonotask(taskContext) {
      override def execute() {
        throw new Exception("task failed")
      }
    }

    monotask.executeAndHandleExceptions()
    /* When an exception is thrown, the execute() method should still mark the task context as
     * completed, and should notify the local DAG scheduler that the task has failed. */
    assert(taskContext.isCompleted)
    verify(localDagScheduler).handleTaskFailure(meq(monotask), any())
  }
}
