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

import org.mockito.Mockito.{mock, when}

import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}

class NetworkSchedulerSuite extends FunSuite with BeforeAndAfterEach with Matchers {

  var networkScheduler: NetworkScheduler = _
  val maxOutstandingMegabytes = 1L

  override def beforeEach() {
    val conf = new SparkConf(false)
    conf.set("spark.reducer.maxMbInFlight", maxOutstandingMegabytes.toString)
    networkScheduler = new NetworkScheduler(conf)
  }

  test("submitTask: task is launched when nothing is outstanding") {
    val monotask1 = makeMonotask(100, 1)
    networkScheduler.submitTask(monotask1)
    assert(monotask1.hasLaunched)
  }

  test("submitTask: task is launched if currently running monotasks are for the same macrotask") {
    val macrotaskId = 1
    val monotask1 = makeMonotask(100, macrotaskId)
    networkScheduler.submitTask(monotask1)
    assert(monotask1.hasLaunched)

    val monotask2 = makeMonotask(500, macrotaskId)
    networkScheduler.submitTask(monotask2)
    assert(monotask2.hasLaunched)

    val monotask3 = makeMonotask(700, macrotaskId)
    networkScheduler.submitTask(monotask3)
    assert(monotask3.hasLaunched)
  }

  test("submitTask: task is queued if currently running monotasks for different macrotask") {
    val monotask1 = makeMonotask(100, 1)
    networkScheduler.submitTask(monotask1)
    assert(monotask1.hasLaunched)

    val monotask2 = makeMonotask(100, 2)
    networkScheduler.submitTask(monotask2)
    assert(!monotask2.hasLaunched)
  }

  test("bytesReceived: no more monotasks are launched until macrotask finishes") {
    // Submit a bunch of monotasks for the first macrotask.
    val macrotaskId = 1
    val monotask1 = makeMonotask(100, macrotaskId)
    networkScheduler.submitTask(monotask1)
    assert(monotask1.hasLaunched)

    val monotask2 = makeMonotask(500, macrotaskId)
    networkScheduler.submitTask(monotask2)
    assert(monotask2.hasLaunched)

    val monotask3 = makeMonotask(700, macrotaskId)
    networkScheduler.submitTask(monotask3)
    assert(monotask3.hasLaunched)

    // Submit monotasks for the 2nd macrotask (it shouldn't be launched).
    val monotask4 = makeMonotask(100, 2)
    networkScheduler.submitTask(monotask4)
    assert(!monotask4.hasLaunched)

    val monotask5 = makeMonotask(100, 2)
    networkScheduler.submitTask(monotask5)
    assert(!monotask5.hasLaunched)

    networkScheduler.bytesReceived(700)
    assert(!monotask4.hasLaunched)
    assert(!monotask5.hasLaunched)

    networkScheduler.bytesReceived(500)
    assert(!monotask4.hasLaunched)
    assert(!monotask5.hasLaunched)

    networkScheduler.bytesReceived(100)
    assert(monotask4.hasLaunched)
    assert(monotask5.hasLaunched)
  }

  test("bytesReceived: does nothing when no more monotasks") {
    val monotask1 = makeMonotask(100, 1)
    networkScheduler.submitTask(monotask1)
    assert(monotask1.hasLaunched)
    networkScheduler.bytesReceived(100)
  }

  def makeMonotask(size: Long, macrotaskId: Long): DummyNetworkMonotask = {
    val taskContext = mock(classOf[TaskContext])
    when(taskContext.taskAttemptId).thenReturn(macrotaskId)
    val blockIds = Array[(BlockId, Long)](
      (ShuffleBlockId(0,0,0), size))
    new DummyNetworkMonotask(blockIds, taskContext)

  }

  class DummyNetworkMonotask(blockIds: Array[(BlockId, Long)], taskContext: TaskContext)
    extends NetworkMonotask(taskContext, BlockManagerId("executor", "test-client", 1), blockIds) {

    var hasLaunched = false
    override def launch(scheduler: NetworkScheduler) {
      hasLaunched = true
    }
  }
}
