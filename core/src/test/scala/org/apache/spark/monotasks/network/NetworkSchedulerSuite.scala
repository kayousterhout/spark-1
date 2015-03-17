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

import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}

class NetworkSchedulerSuite extends FunSuite with BeforeAndAfterEach with Matchers {

  var networkScheduler: NetworkScheduler = _

  override def beforeEach() {
    networkScheduler = new NetworkScheduler()
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

  def makeMonotask(size: Long): DummyNetworkMonotask = {
    val blockIds = Array[(BlockId, Long)](
      (ShuffleBlockId(0,0,0), size))
    new DummyNetworkMonotask(blockIds)

  }

  class DummyNetworkMonotask(blockIds: Array[(BlockId, Long)])
    extends NetworkMonotask(null, BlockManagerId("test-client", "test-client", 1), blockIds) {

    var hasLaunched = false
    override def launch(scheduler: NetworkScheduler) {
      hasLaunched = true
    }
  }
}
