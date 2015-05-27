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

/**
 * A MonotaskSequencer is responsible for determining the order in which a set of monotasks should
 * be run.  Each MonotaskSequencer must implement exactly two methods: an add() method, which adds
 * a new monotask to the set of monotasks that need to be run at some point in the future, and
 * a remove() method, which returns the next monotask that should be run.  remove() must only
 * return monotasks that have previously been add()'ed, and each monotask that is add()'ed should
 * be returned by remove() exactly once.
 */
private[spark] trait MonotaskSequencer {
  def add(monotask: Monotask): Unit
  def remove(): Option[Monotask]
}
