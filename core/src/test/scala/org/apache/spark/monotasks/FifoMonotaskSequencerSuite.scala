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

import org.mockito.Mockito.mock

import org.scalatest.FunSuite

import org.apache.spark.monotasks.compute.{ComputeMonotask, PrepareMonotask}

class FifoMonotaskSequencerSuite extends FunSuite {
  test("Monotasks are returned in FIFO order") {
    val monotask1 = mock(classOf[PrepareMonotask])
    val monotask4 = mock(classOf[PrepareMonotask])
    val monotask6 = mock(classOf[PrepareMonotask])

    val monotask2 = mock(classOf[ComputeMonotask])
    val monotask3 = mock(classOf[ComputeMonotask])
    val monotask5 = mock(classOf[ComputeMonotask])
    val monotask7 = mock(classOf[ComputeMonotask])

    val sequencer = new FifoMonotaskSequencer
    sequencer.add(monotask1)
    sequencer.add(monotask2)
    sequencer.add(monotask3)
    sequencer.add(monotask4)
    sequencer.add(monotask5)
    sequencer.add(monotask6)
    sequencer.add(monotask7)

    assert(sequencer.remove() === Some(monotask1))
    assert(sequencer.remove() === Some(monotask2))
    assert(sequencer.remove() === Some(monotask3))
    assert(sequencer.remove() === Some(monotask4))
    assert(sequencer.remove() === Some(monotask5))
    assert(sequencer.remove() === Some(monotask6))
    assert(sequencer.remove() === Some(monotask7))
    assert(sequencer.remove() === None)
  }
}
