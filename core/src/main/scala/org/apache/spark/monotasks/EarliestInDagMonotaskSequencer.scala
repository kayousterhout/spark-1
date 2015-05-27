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

import org.apache.spark.monotasks.compute.{ComputeMonotask, PrepareMonotask}

/**
 * A monotask sequencer that returns PrepareMonotasks before any other type of monotask.  Within
 * each of those two categories, monotasks are returned in FIFO order.
 */
private[spark] class EarliestInDagMonotaskSequencer extends MonotaskSequencer {

  val prepareMonotaskQueue = new Queue[PrepareMonotask]
  val nonPrepareMonotaskQueue = new Queue[ComputeMonotask]

  override def add(monotask: Monotask): Unit = {
    monotask match {
      case prepareMonotask: PrepareMonotask =>
        prepareMonotaskQueue += prepareMonotask
      case computeMonotask: ComputeMonotask =>
        nonPrepareMonotaskQueue += computeMonotask
      case _ =>
        throw new IllegalStateException(
          "EarliestInDagMonotaskSequencer should only be used for compute monotasks; " +
          s"received monotask $monotask")
    }
  }

  override def remove(): Option[Monotask] = {
    if (!prepareMonotaskQueue.isEmpty) {
      Some(prepareMonotaskQueue.dequeue())
    } else if (!nonPrepareMonotaskQueue.isEmpty) {
      Some(nonPrepareMonotaskQueue.dequeue())
    } else {
      None
    }
  }
}
