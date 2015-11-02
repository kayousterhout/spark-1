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

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.Logging
import org.apache.spark.util.Utils

private[spark] class NetworkScheduler() extends Logging {
  /** Number of bytes that this executor is currently waiting to receive over the network. */
  private var currentOutstandingBytesToReceive = new AtomicLong(0)
  
  /** Number of bytes that this executor is currently waiting to send over the network. */
  private var currentOutstandingBytesToSend = new AtomicLong(0)

  def submitTask(monotask: NetworkMonotask) {
    monotask.execute(NetworkScheduler.this)
  }

  /**
   * Used to keep track of the bytes that are outstanding to be received over the network. Can be
   * called with a negative value to indicate bytes that are no longer outstanding.
   */
  def addOutstandingBytesToReceive(bytes: Long) = currentOutstandingBytesToReceive.addAndGet(bytes)

  def getOutstandingBytesToReceive: Long = currentOutstandingBytesToReceive.get()

  /**
   * Used to keep track of the bytes that are ready to be sent out over the network. Can be called
   * with a negative value to indicate bytes that are no longer outstanding.
   */
  def addOutstandingBytesToSend(bytes: Long) = currentOutstandingBytesToSend.addAndGet(bytes)

  def getOutstandingBytesToSend: Long = currentOutstandingBytesToSend.get()
}
