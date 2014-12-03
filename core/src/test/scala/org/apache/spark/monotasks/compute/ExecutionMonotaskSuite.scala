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

import org.mockito.Mockito._
import org.mockito.Matchers.{any, eq => meq}

import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark.{LocalSparkContext, SparkContext, TaskContext}
import org.apache.spark.monotasks.LocalDagScheduler

class ExecutionMonotaskSuite extends FunSuite with LocalSparkContext with BeforeAndAfterEach {

  override def beforeEach() {
    /* Need to create a local spark context so that SparkEnv gets initialized (which is used
     * in ExecutionMonotask). */
    sc = new SparkContext("local", "test")
  }

  test("execute correctly handles thrown exceptions") {
    /* When an exception is thrown, the execute() method should still mark the task context as
     * completed, and snould notify the local DAG scheduler that the task has failed. */
    val localDagScheduler = mock(classOf[LocalDagScheduler])
    val taskContext = mock(classOf[TaskContext])
    when(taskContext.localDagScheduler).thenReturn(localDagScheduler)

    val monotask = new ExecutionMonotask[Int, Int](taskContext, null, null) {
      override def getResult(): Int = {
        throw new Exception("task failed")
      }
    }

    monotask.execute()
    verify(taskContext).markTaskCompleted()
    verify(localDagScheduler).handleTaskFailure(meq(monotask), any())
  }

  test("execute tells DAG scheduler when task compeltes successfully") {

  }
}