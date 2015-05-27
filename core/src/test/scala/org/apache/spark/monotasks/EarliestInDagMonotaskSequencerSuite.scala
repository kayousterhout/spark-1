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

class EarliestInDagMonotaskSequencerSuite extends FunSuite {
  test("Prepare monotasks are returned before other types of monotasks") {
    val prepareMonotask1 = mock(classOf[PrepareMonotask])
    val prepareMonotask2 = mock(classOf[PrepareMonotask])
    val prepareMonotask3 = mock(classOf[PrepareMonotask])

    val computeMonotask1 = mock(classOf[ComputeMonotask])
    val computeMonotask2 = mock(classOf[ComputeMonotask])
    val computeMonotask3 = mock(classOf[ComputeMonotask])
    val computeMonotask4 = mock(classOf[ComputeMonotask])

    val sequencer = new EarliestInDagMonotaskSequencer
    sequencer.add(prepareMonotask1)
    sequencer.add(computeMonotask1)
    sequencer.add(computeMonotask2)
    sequencer.add(prepareMonotask2)
    sequencer.add(computeMonotask3)
    sequencer.add(prepareMonotask3)
    sequencer.add(computeMonotask4)

    assert(sequencer.remove() === Some(prepareMonotask1))
    assert(sequencer.remove() === Some(prepareMonotask2))
    assert(sequencer.remove() === Some(prepareMonotask3))
    assert(sequencer.remove() === Some(computeMonotask1))
    assert(sequencer.remove() === Some(computeMonotask2))
    assert(sequencer.remove() === Some(computeMonotask3))
    assert(sequencer.remove() === Some(computeMonotask4))
    assert(sequencer.remove() === None)
  }
}
