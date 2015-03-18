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

  // Mocked TaskContext to use when creating NetworkMonotask (the scheduler uses the TaskContext
  // to output log messages).
  val taskContext = mock(classOf[TaskContext])
  when(taskContext.taskAttemptId).thenReturn(15L)

  override def beforeEach() {
    val conf = new SparkConf(false)
    conf.set("spark.reducer.maxMbInFlight", maxOutstandingMegabytes.toString)
    networkScheduler = new NetworkScheduler(conf)
  }

  test("submitTask: monotask isn't launched if the outstanding bytes exceeds the maximum") {
    val monotask1 = makeMonotask(100)
    val monotask2 = makeMonotask(100)
    val monotask3 = makeMonotask(networkScheduler.maxOutstandingBytes - 150)

    networkScheduler.submitTask(monotask1)
    assert(monotask1.hasLaunched)
    networkScheduler.submitTask(monotask2)
    assert(monotask2.hasLaunched)
    networkScheduler.submitTask(monotask3)
    assert(!monotask3.hasLaunched,
      "NetworkScheduler should ensure that the total number of outstanding bytes doesn't exceed " +
        "the maximum allowed")

    networkScheduler.bytesReceived(100)
    assert(monotask3.hasLaunched)
  }

  test("submitTask: monotask is launched if the outstanding bytes is below the maximum") {
    val monotask1 = makeMonotask(100)
    val monotask2 = makeMonotask(200)

    networkScheduler.submitTask(monotask1)
    assert(monotask1.hasLaunched)
    networkScheduler.submitTask(monotask2)
    assert(monotask2.hasLaunched)
  }

  /**
   * This test creates a monotask with totalResultSize larger than maxOutstandingBytes. Such a
   * monotask should only be launched if it's the only monotask running (to mitigate the fact that
   * its size is so large).
   */
  test("submitTask: monotask larger than maxOutstandingBytes is eventually launched") {
    val smallMonotask = makeMonotask(3)
    val bigMonotask = makeMonotask(networkScheduler.maxOutstandingBytes + 1)

    networkScheduler.submitTask(smallMonotask)
    assert(smallMonotask.hasLaunched)

    networkScheduler.submitTask(bigMonotask)
    assert(!bigMonotask.hasLaunched)

    networkScheduler.bytesReceived(3)
    assert(bigMonotask.hasLaunched)
  }

  test("submitTask: round robins over multiple executors") {
    val execAMonotask0 = makeMonotask(512 * 1024, "execA")
    val execBMonotask0 = makeMonotask(512 * 1024, "execB")
    val execBMonotask1 = makeMonotask(512 * 1024, "execB")
    val execCMonotask0 = makeMonotask(512 * 1024, "execC")

    // Submit all of the monotasks. Only the first two should be run (to respect the
    // maxOutstandingBytes).
    networkScheduler.submitTask(execAMonotask0)
    assert(execAMonotask0.hasLaunched)
    networkScheduler.submitTask(execBMonotask0)
    assert(execBMonotask0.hasLaunched)
    networkScheduler.submitTask(execBMonotask1)
    assert(!execBMonotask1.hasLaunched)
    networkScheduler.submitTask(execCMonotask0)
    assert(!execCMonotask0.hasLaunched)

    // When one of the monotasks finishes, the monotask for executor C should be run next,
    // because the scheduler should do round-robin over the executors.
    networkScheduler.bytesReceived(512 * 1024)
    assert(execCMonotask0.hasLaunched)
    assert(!execBMonotask1.hasLaunched)

    // When another monotask finishes, the last monotask should be run.
    networkScheduler.bytesReceived(512 * 1024)
    assert(execBMonotask1.hasLaunched)
  }

  def makeMonotask(size: Long, executorId: String = "test-client"): DummyNetworkMonotask = {
    val blockIds = Array[(BlockId, Long)](
      (ShuffleBlockId(0,0,0), size))
    new DummyNetworkMonotask(blockIds, executorId)

  }

  class DummyNetworkMonotask(blockIds: Array[(BlockId, Long)], executorId: String)
    extends NetworkMonotask(taskContext, BlockManagerId(executorId, "test-client", 1), blockIds) {

    var hasLaunched = false
    override def launch(scheduler: NetworkScheduler) {
      hasLaunched = true
    }
  }
}
