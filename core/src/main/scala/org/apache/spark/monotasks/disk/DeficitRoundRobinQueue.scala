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

package org.apache.spark.monotasks.disk

import scala.collection.mutable.{ArrayBuffer, HashMap, Queue}

import org.apache.spark.Logging

/**
 * A blocking queue that implements deficit round-robin over queues. DiskMonotasks are placed into
 * different queues based on user-specified keys.
 *
 * Each queue internally uses a RoundRobinByRemoteMachineQueue to prioritize tasks over machines.
 */
private[spark] class DeficitRoundRobinQueue[K] extends Logging {
  class DeficitQueue(val t: String) {
    val queue = new Queue[DiskMonotask]()
    var deficit: Double = 0

    /** Returns the minimum quantum needed for the queue to return something. */
    def minQuantumNeeded: Double = {
      queue.headOption.map { head =>
        // NB: If the Queue is changed to a non-FIFO queue, will need to make sure this is
        // nonnegative.
        val ret = head.virtualSize - deficit
        logDebug(s"For queue for $t with size ${queue.length}, deficit $deficit and " +
          s"head size ${head.virtualSize} so returning $ret")
        ret
      }.getOrElse(Double.MaxValue)
    }

    def grantQuantum(): Unit = {
      if (queue.isEmpty) {
        deficit = 0
      } else {
        deficit += currentQuantum
      }
    }

    def maybeDequeue(): Option[DiskMonotask] = {
      queue.headOption.flatMap { head =>
        val headSize = head.virtualSize
        if (deficit >= headSize) {
          deficit -= headSize
          Some(queue.dequeue())
        } else {
          None
        }
      }
    }
  }

  private val keyToQueue = new HashMap[K, DeficitQueue]()
  private val keys = new ArrayBuffer[K]
  /** Next index to launch something at. When this is 0, should re-compute the quantum. */
  private var currentIndex = 0
  /** Quantum to grant to each non-empty queue in this round. */
  private var currentQuantum: Double = 0

  def length: Int = keyToQueue.map(_._2.queue.length).sum

  def enqueue(key: K, item: DiskMonotask): Unit = synchronized {
    val queue = keyToQueue.get(key).getOrElse {
      val newQueue = new DeficitQueue(key.toString)
      keyToQueue.put(key, newQueue)
      keys.append(key)
      newQueue
    }
    queue.queue.enqueue(item)
    notify()
  }

  private def updateQuantum(): Unit = {
    // This should only happen when the queue was empty, so currentIndex should be at 0.
    assert(currentIndex == 0)

    // Calculate the smallest quantum needed to launch something, and use that.
    // This is O(numQueues), so this may need to be re-considered if the number
    // of queues is large (for now, we expect is to be small because there are only a few types
    // of disk monotasks).
    currentQuantum = keyToQueue.map(_._2.minQuantumNeeded).min
  }

  /**
   * Plan:
   * -first go the end of the queue, and grant everyone the quantum (possibly stopping to run
   * something). This can happen in a method that returns an option.
   * -next, loop through one more time. this is important to reset all of the things that are
   *  empty to 0.
   * -now, wait!
   * @return
   */
  def dequeue(): DiskMonotask = synchronized {
    // This is needed if the queue is empty: in that case, we can't grant the quantum for key 0,
    // because we don't know what the quantum should bel.
    var needToGrantQuantum = false

    while (isEmpty()) {
      // 0 out all of the deficit counters.
      keyToQueue.foreach {
        case (key, queue) =>
          queue.deficit = 0
      }
      // Reset the current index so that when a new quantum is granted, all of the queues get it.
      currentIndex = 0
      needToGrantQuantum = true
      wait()
    }

    while (true) {
      (currentIndex until keys.length).foreach {i =>
        val queue = keyToQueue(keys(currentIndex))

        if (needToGrantQuantum) {
          updateQuantum()
          queue.deficit += currentQuantum
          needToGrantQuantum = false
        }

        // Get something from the queue maybe
        queue.maybeDequeue.map { monotask =>
          logDebug(s"With quantum $currentQuantum, dequeued something for ${keys(currentIndex)} " +
            s"that has size ${monotask.virtualSize}. deficit now ${queue.deficit}")
          // If this returns, currentIndex will *not* be updated, which is correct: we may be
          // able to dequeue more things from the same queue.
          return monotask
        }

        // Only update currentIndex if nothing was launched (otherwise, may need to launch more
        // things at the current index next time).  As part of updating the index, grant the
        // quantum to the next index.
        currentIndex = (currentIndex + 1) % keys.length
        if (currentIndex == 0) {
          // Decide on the correct quantum for the next round.
          updateQuantum()
        }
        // Grant the quantum to the currentIndex.
        keyToQueue(keys(currentIndex)).grantQuantum()
      }
    }
    // This exception is needed to satisfy the Scala compiler.
    throw new Exception("Should not reach this state")
  }

  def isEmpty(): Boolean = {
    keyToQueue.forall {
      case (_, queue) => queue.queue.isEmpty
    }
  }
}
