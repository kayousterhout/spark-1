/*
 * Copyright 2016 The Regents of The University California
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

import org.scalatest.FunSuite

class RoundRobinBlockingQueueSuite extends FunSuite {
  test("items are dequeued in round robin order") {
    val queue = new RoundRobinBlockingQueue[String, String]
    assert(queue.isEmpty())

    queue.enqueue("a", "1a")
    queue.enqueue("a", "2a")
    queue.enqueue("a", "3a")
    assert(!queue.isEmpty())

    assert(queue.dequeue() == "1a")

    queue.enqueue("b", "1b")
    assert(queue.dequeue() == "2a")

    assert(!queue.isEmpty())

    assert(queue.dequeue() == "1b")
    assert(!queue.isEmpty())

    queue.enqueue("b", "2b")
    queue.enqueue("b", "3b")

    assert(queue.dequeue() == "3a")
    assert(queue.dequeue() == "2b")
    assert(queue.dequeue() == "3b")
    assert(queue.isEmpty())
  }
}
