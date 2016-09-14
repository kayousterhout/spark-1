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

import scala.collection.mutable.{ArrayBuffer, HashMap, Queue}

/**
 * A blocking queue that returns items in round-robin order, based on user-specified keys.
 */
class RoundRobinBlockingQueue[K, V] {
  private val keyToQueue = new HashMap[K, Queue[V]]()
  // Assume that there are a fixed number of keys, so they never needed to be removed
  // (this holds if, for example, keys are remote hosts, since we're assuming a static cluster for
  // now).
  private val keys = new ArrayBuffer[K]
  private var currentIndex = 0

  def enqueue(key: K, item: V): Unit = synchronized {
    val queue = keyToQueue.get(key).getOrElse {
      val newQueue = new Queue[V]()
      keyToQueue.put(key, newQueue)
      keys.append(key)
      newQueue
    }
    queue.enqueue(item)
    notify()
  }

  def dequeue(): V = synchronized {
    while (true) {
      (0 until keys.length).foreach {i =>
        val currentKey = keys(currentIndex)
        // Update currentIndex
        currentIndex = (currentIndex + 1) % keys.length
        val queue = keyToQueue(currentKey)
        if (!queue.isEmpty) {
          return queue.dequeue()
        }
      }
      wait()
    }
    // This exception is needed to satisfy the Scala compiler.
    throw new Exception("Should not reach this state")
  }

  def isEmpty(): Boolean = {
    keyToQueue.forall {
      case (_, queue) => queue.isEmpty
    }
  }
}
