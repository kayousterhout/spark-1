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

package org.apache.spark.monotasks.scheduler

import scala.collection.mutable.{HashMap, HashSet}

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.monotasks.{Monotask, MonotaskType}
import org.apache.spark.monotasks.compute.PrepareMonotask

/**
 * Helps track which resources are being used by a particular class.
 *
 * Each instance of this class should correspond to exactly one macrotask. This class handles
 * tracking what monotasks are currently running for the macrotask (to consolidate phase change
 * messages sent to monotasks schedulers).
 */
private[spark] class TaskResourceTracker extends Logging {

  private class PhaseInfo(val id: Int, var numRunningMonotasks: Int)

  /** Stores information about each resource that has currently running monotasks. */
  private val resourceToInfo = new HashMap[MonotaskType.Value, PhaseInfo]

  /**
   * The id to use for the next phase. Start at 1, because the phase that requests tasks from the
   * driver is 0.
   */
  private var nextPhaseId: Int = 1

  private var previousPhaseForked = false

  /**
   * Updates the tracking about the queues for each type of resource (which determines when the
   * Executor tells the Driver to assign it more tasks).
   *
   * The stageId is the ID of the stage for this task, and should be the same every time
   * updateQueueTracking is called (it's passed in here for convenience, because when the
   * TaskContextImpl is first created, it doesn't have a stage ID set yet).
   *
   * This class handles coalescing monotasks of the same type that run at the same time (or back
   * to back) so that the Monotasks scheduler can deal with fewer phases.
   *
   * TODO: It would be better to pass in a DAG of phases from the LocalDagScheduler. There isn't
   *       quite enough information here to construct the ideal DAG (it's hard to infer information
   *       about branches in the DAG, and in particular, when the branches re-converge).
   */
  def updateQueueTracking(
      stageId: Int,
      completedMonotask: Monotask,
      startedMonotasks: HashSet[Monotask]): Unit = {
    logInfo(s"UpdateQueueTracking called for $completedMonotask ${startedMonotasks.mkString(",")}")
    val completedMonotaskType = MonotaskType.getType(completedMonotask)

    // TODO: special case: if completed and finished same type, and no branching, do nothing.
    val startedMonotaskTypes = startedMonotasks.map(MonotaskType.getType(_))
    if (!previousPhaseForked &&
        !completedMonotask.isInstanceOf[PrepareMonotask] &&
        startedMonotaskTypes.size == 1 &&
        startedMonotaskTypes.contains(completedMonotaskType)) {
      logInfo(s"Skipping update about finished monotask $completedMonotask, because it uses the " +
        "same resource as the previous phase, so can be coalesced to avoid unnecessary " +
        "notifications to MonotasksScheduler")
      return
    }

    // finishedPhaseInfo is an option containing a two-item tuple. It will be None if there isn't a
    // phase that just finished. If the option is set, the first item is a MonotaskType (with the
    // type of the phase that finished) and the second item is the unique id for the phase.
    val finishedPhaseInfo: Option[(MonotaskType.Value, Int)] = {
      if (completedMonotask.isInstanceOf[PrepareMonotask]) {
        // We don't track the PrepareMonotasks as a phase, so we don't need to tell the
        // MonotaskScheduler about them finishing (and we won't have tracked it in
        // resourceToInfo).
        None
      } else {
        assert(resourceToInfo.contains(completedMonotaskType),
          s"Compeleted monotask type $completedMonotaskType expected to be in set of currently " +
            "running resources.")
        var phaseInfo = resourceToInfo(completedMonotaskType)
        phaseInfo.numRunningMonotasks -= 1
        logInfo(s"TaskContextImpl: just finished $completedMonotask; count is " +
          s"${phaseInfo.numRunningMonotasks}")
        assert(phaseInfo.numRunningMonotasks >= 0)
        if (phaseInfo.numRunningMonotasks > 0) {
          // There are still monotasks running for the resource, so nothing new should have started
          // (if something new did, this task has a complex DAG we don't understand).
          assert(startedMonotasks.isEmpty)
          None
        } else if (phaseInfo.numRunningMonotasks == 0) {
          resourceToInfo.remove(completedMonotaskType)
          Some((completedMonotaskType, phaseInfo.id))
        } else {
          assert(false, "The number of running monotasks for a resource should not be negative")
          None
        }
      }
    }

    // TODO FIX First, update counters for the started monotask (do this before updating the
    // counters for
    // completedMonotask, because if the monotasks are of the same type, we need to make sure to
    // increase the counter for that monotask type first, so that the counter doesn't reach 0 and
    // cause us to report a completed phase to the MonotasksScheduler).

    // startedPhaseInfo will be a set of 2-item tuples of phases that started. The first item in
    // each tuple is the type of the phase, and the second item is the unique ID of the phase.
    val startedPhaseInfo = startedMonotasks.flatMap { startedMonotask =>
      val startedMonotaskType = MonotaskType.getType(startedMonotask)
      if (resourceToInfo.contains(startedMonotaskType)) {
        val phaseInfo = resourceToInfo(startedMonotaskType)
        phaseInfo.numRunningMonotasks += 1
        // Coalesce this into the same phase as the other monotasks that are currently running for
        // resource.
        None
      } else {
        val phaseId = nextPhaseId
        nextPhaseId += 1
        logInfo(s"TaskContextImpl: just started $startedMonotaskType with phase id $phaseId")
        resourceToInfo.put(startedMonotaskType, new PhaseInfo(phaseId, 1))
        Some((startedMonotaskType, phaseId))
      }
    }

    if (previousPhaseForked && !startedMonotasks.isEmpty) {
      // Something else is starting -- so assume this means the forked part of the DAG has ended.
      previousPhaseForked = false
    } else {
      previousPhaseForked = startedMonotaskTypes.size > 1
    }

    // Make sure that there's a phase in either finishedPhaseInfo or startedPhaseInfo. If there's
    // not, it means that the monotasks that just finished was in the middle of a phase, so the
    // scheduler doesn't need to be told about it.
    if (!finishedPhaseInfo.isEmpty || !startedPhaseInfo.isEmpty) {
      // Tell the MonotasksScheduler that the macrotask is a new phase, so it can update internal
      // state and request new tasks from the master, as appropriate.
      val monotasksScheduler = SparkEnv.get.monotasksScheduler
      monotasksScheduler.handlePhaseChange(
        stageId, finishedPhaseInfo, startedPhaseInfo, resourceToInfo.size == 0)
    }
  }
}
