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

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

import org.apache.spark.Logging
import org.apache.spark.util.Utils

private[spark] class NetworkScheduler() extends Logging {
  /** Number of bytes that this executor is currently waiting to receive over the network. */
  private var currentOutstandingBytes = new AtomicLong(0)

  /** Number of bytes that have been sent to Netty but haven't yet been sent out on the network. */
  private var currentOutstandingBytesToSend = new AtomicLong(0)

  private val responseQueueTimes = new LinkedBlockingQueue[Long]()
  private val tempBuffer = new ArrayBuffer[Long]()
  private val totalBytesSent = new AtomicLong(0)

  /**
   * Queue of monotasks waiting to be executed. submitMonotask() puts monotasks in this queue,
   * and a separate thread executes them, so that launching network monotasks doesn't happen
   * in the main scheduler thread (network monotasks are asynchronous, but launching a large
   * number of them can still take a non-negligible amount of time in aggregate.
   */
  private val monotaskQueue = new LinkedBlockingQueue[NetworkMonotask]()

  // Start a thread responsible for executing the network monotasks on monotaskQueue.
  private val monotaskLaunchThread = new Thread(new Runnable() {
    override def run(): Unit = {
      while (true) {
        val networkMonotask = monotaskQueue.take()
        networkMonotask.execute(NetworkScheduler.this)
      }
    }
  })
  monotaskLaunchThread.setDaemon(true)
  monotaskLaunchThread.setName("Network monotask launch thread")
  monotaskLaunchThread.start()

  def submitTask(monotask: NetworkMonotask) {
    monotaskQueue.put(monotask)
  }

  /**
   * Used to keep track of the bytes outstanding over the network. Can be called with a negative
   * value to indicate bytes that are no longer outstanding.
   */
  def addOutstandingBytes(bytes: Long) = currentOutstandingBytes.addAndGet(bytes)

  def getOutstandingBytes: Long = currentOutstandingBytes.get()

  def addOutstandingBytesToSend(bytes: Long) = {
    currentOutstandingBytesToSend.addAndGet(bytes)
    totalBytesSent.addAndGet(bytes)
  }
  def getOutstandingBytesToSend: Long = currentOutstandingBytesToSend.get()
  def getTotalBytesSent: Long = totalBytesSent.get()

  def addResponseQueueTime(time: Long) = responseQueueTimes.put(time)
  def getAverageResponseQueueTime: Long = {
    tempBuffer.clear()
    responseQueueTimes.drainTo(tempBuffer)
    if (tempBuffer.size > 0) {
      tempBuffer.sum / tempBuffer.size
    } else {
      0L
    }
  }
}
