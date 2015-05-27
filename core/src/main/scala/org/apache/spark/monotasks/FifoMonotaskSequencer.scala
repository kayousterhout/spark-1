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

package org.apache.spark.monotasks

import scala.collection.mutable.Queue

/** A MonotaskSequencer that behaves like a queue: monotasks are returned in FIFO order. */
private[spark] class FifoMonotaskSequencer extends MonotaskSequencer {
  val monotaskQueue = new Queue[Monotask]

  override def add(monotask: Monotask): Unit = {
    monotaskQueue += monotask
  }

  override def remove(): Option[Monotask] = {
    try {
      Some(monotaskQueue.dequeue())
    } catch {
      case _: Throwable =>
        None
    }
  }
}