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

import java.net.SocketAddress
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.HashMap

import org.apache.spark.{Logging, SparkEnv, SparkException}

private[spark] class NetworkScheduler() extends Logging {
  /** Number of bytes that this executor is currently waiting to receive over the network. */
  private var currentOutstandingBytes = new AtomicLong(0)

  /**
   * Queue of monotasks waiting to be executed. submitMonotask() puts monotasks in this queue,
   * and a separate thread executes them, so that launching network monotasks doesn't happen
   * in the main scheduler thread (network monotasks are asynchronous, but launching a large
   * number of them can still take a non-negligible amount of time in aggregate).
   */
  private val monotaskQueue = new LinkedBlockingQueue[NetworkMonotask]()

  /** These queues include monotasks that have started. Invariant: first thing in queue will
    * always be running. */
  private val executorIdToMonotaskQueue =
    new HashMap[SocketAddress, LinkedBlockingQueue[NetworkResponseMonotask]]

  // Start a thread responsible for executing the network monotasks in monotaskQueue.
  private val monotaskLaunchThread = new Thread(new Runnable() {
    override def run(): Unit = {
      while (true) {
        monotaskQueue.take().execute(NetworkScheduler.this)
      }
    }
  })
  monotaskLaunchThread.setDaemon(true)
  monotaskLaunchThread.setName("Network monotask launch thread")
  monotaskLaunchThread.start()

  /** Whether to fairly schedule across remote executors. */
  private var fairSchedule = false

  def submitTask(monotask: NetworkMonotask): Unit = {
    fairSchedule = SparkEnv.get.conf.getBoolean("spark.monotasks.network.fairSchedule", false)
    monotask match {
      case networkResponseMonotask: NetworkResponseMonotask =>
        logInfo(s"KNET Monotask ${monotask.taskId} block ${networkResponseMonotask.blockId} to " +
          s"${networkResponseMonotask.channel.remoteAddress()} READY ${System.currentTimeMillis}")
        if (fairSchedule) {
          val address = networkResponseMonotask.channel.remoteAddress()
          if (!executorIdToMonotaskQueue.contains(address)) {
            logInfo(s"$address was not already in executorIdToMonotaskQueue so adding it")
            executorIdToMonotaskQueue.put(
              address, new LinkedBlockingQueue[NetworkResponseMonotask]())
          }
          logInfo(s"Total of ${executorIdToMonotaskQueue.size} things in queue")

          // Add the monotask to the queue of monotasks for the executor it's sending data to.
          val monotasksForExecutor = executorIdToMonotaskQueue(address)
          monotasksForExecutor.synchronized {
            if (monotasksForExecutor.isEmpty) {
              logInfo(s"No monotasks queued for executor $address, so launching a monotask!")
              // Start the monotask! Nothing is running on the executor.
              monotaskQueue.put(monotask)
            }
            logInfo(s"Adding monotask $monotask to queue for $address")
            // In either case, put the monotask in the queue for the executor.
            monotasksForExecutor.put(networkResponseMonotask)
          }
        } else {
          // Immediately submit the monotask for scheduling.
          monotaskQueue.put(monotask)
        }

      case _ =>
        monotaskQueue.put(monotask)
    }
  }


  def handleNetworkResponseSent(monotask: NetworkResponseMonotask): Unit = {
    logInfo(s"KNET Monotask ${monotask.taskId} block ${monotask.blockId} to " +
      s"${monotask.channel.remoteAddress()} SENT ${System.currentTimeMillis}")
    if (fairSchedule) {
      // Run the next thing on the queue, if there is anything.

      val monotasksForExecutor = executorIdToMonotaskQueue(monotask.channel.remoteAddress())
      monotasksForExecutor.synchronized {
        // Remove the first monotask in the queue, which should be this one that's running.
        val removedMonotask = monotasksForExecutor.remove()
        if (removedMonotask != monotask) {
          throw new SparkException(s"Monotask in queue $removedMonotask with id " +
            s"${removedMonotask.taskId} is not the same as the one that was running " +
            s"$monotask ${monotask.taskId}")
        }
        // Start the nw-first monotask in the queue, if there is one.
        val newMonotask = monotasksForExecutor.peek()
        if (newMonotask != null) {
          logInfo(s"Network monotask ${monotask.taskId} finished, so launching ${newMonotask}")
          monotaskQueue.put(newMonotask)
        }
      }
    }
  }

  /**
   * Used to keep track of the bytes outstanding over the network. Can be called with a negative
   * value to indicate bytes that are no longer outstanding.
   */
  def addOutstandingBytes(bytes: Long) = currentOutstandingBytes.addAndGet(bytes)

  def getOutstandingBytes: Long = currentOutstandingBytes.get()
}
