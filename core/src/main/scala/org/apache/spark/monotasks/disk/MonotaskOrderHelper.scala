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

import scala.collection.mutable.HashMap

/**
 * Queue that handles balancing over different monotask types.
 *
 * All methods are synchronized because the queue may be accessed by different disk accessors
 * simultaneously.
 */
class MonotaskOrderHelper {
  val monotaskTypeToOperationBytes = new HashMap[Class[_], Long]

  /**
   * Returns the given monotask types, sorted by which types should be run first.
   *
   * Accepts a list of types as input because it's possible not all of the types that are available
   * to run will be un monotaskTypeToOperationBytes yet (e.g., if no tasks have yet started of that
   * type).
   */
  def getOrderedTypes(unorderedTyes: Seq[Class[_]]): Seq[Class[_]] = synchronized {
    unorderedTyes.sortBy(monotaskTypeToOperationBytes.get(_).getOrElse(0L))
  }

  def updateStateForStartedMonotask(monotaskClass: Class[_], size: Long): Unit = synchronized {
    // Update the bytes used by each type of monotask.
    val existingBytes = monotaskTypeToOperationBytes.get(monotaskClass).getOrElse(0L)
    monotaskTypeToOperationBytes.put(monotaskClass, existingBytes + size)
  }
}
