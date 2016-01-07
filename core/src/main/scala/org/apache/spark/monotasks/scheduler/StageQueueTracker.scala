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

import scala.collection.mutable.{Set, ArrayBuffer, HashMap}

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.executor.ExecutorBackend
import org.apache.spark.monotasks.MonotaskType

/**
 * Tracks information about the queues for each type of monotask as a stage's tasks are processed
 * on a particular worker. There should be exactly one of this object for each worker and for each
 * stage that's running on that worker.
 */
private[spark] class StageQueueTracker(
    numCores: Int,
    numDisks: Int,
    executorBackend: ExecutorBackend) extends Logging{
  /**
   * Number of running tasks in the stage tracked by this QueueTracker. Used to determine when
   * the queueTracker can be deleted.
   */
  private var numRunningTasks = 0

  /** For each phase, the queue data. Phases are added to this data structure as they begin. */
  private val phaseIdToQueueData = new HashMap[Int, PhaseQueueData]()

  // TODO: The initialization of the number of tasks that have already been "launched" needs to be
  //       fixed to handle multiple concurrent jobs.
  private val firstPhase = new FirstPhaseQueueData(
    phaseId = 0,
    numTasksInitiallyRequestedFromDriver = numCores,
    executorBackend)
  phaseIdToQueueData.put(0, firstPhase)

  private val monotaskTypeToConcurrency = HashMap[MonotaskType.Value, Int](
    MonotaskType.Compute -> numCores,
    MonotaskType.Disk -> numDisks,
    MonotaskType.Network -> 1
  )

  /** Returns true if there are no remaining tasks for this stage, so this can be deleted. */
  def canBeDeleted: Boolean = numRunningTasks == 0

  /** Called on a worker each time a task starts and/or finishes a phase of execution. */
  def handlePhaseChange(
      previousPhase: Option[(MonotaskType.Value, Int)],
      newPhases: Set[(MonotaskType.Value, Int)],
      macrotaskIsFinished: Boolean): Unit = {
    // Update the number of running tasks if the phase change is for the first or last phase.
    if (previousPhase.isEmpty) {
      numRunningTasks += 1
    } else if (macrotaskIsFinished) {
      numRunningTasks -= 1
    }

    // Update the information stored in phaseIdToQueueData to make sure it has prevoiusPhase, all
    // of the phases in newPhases, and dependencies between them.
    newPhases.foreach { newPhaseTypeAndId =>
      val id = newPhaseTypeAndId._2
      val newPhaseType = newPhaseTypeAndId._1
      // If this is the first task to reach the next phase, create the appropriate state.
      val newPhaseData = if (!phaseIdToQueueData.contains(id)) {
        val newPhaseData = new PhaseQueueData(
          newPhaseType,
          id,
          monotaskTypeToConcurrency(newPhaseType)
        )
        phaseIdToQueueData.put(id, newPhaseData)
        newPhaseData
      } else {
        phaseIdToQueueData(id)
      }

      // Make sure the phase has previousPhase as a dependency (it's possible
      // this dependency isn't set up, even if the phase ID was already in phaseIdToQueueData, if
      // there was a branch in the monotask DAG).
      // If there's no previous phase, it means that the first phase just finished.
      val previousPhaseId = previousPhase.map(_._2).getOrElse(0)
      phaseIdToQueueData.get(previousPhaseId) match {
        case Some(previousPhaseData) =>
          previousPhaseData.addNextPhaseData(newPhaseData)

        case None =>
          throw new SparkException(s"Invalid call to StageQueueTracker: handlePhaseChange " +
            s"called with previous phase $previousPhaseId, but no tasks for that phase have " +
            "been started yet (the only phases that have been started are " +
            s"${phaseIdToQueueData.keysIterator.mkString(",")}.")
      }
    }

    // Finally, do the actual work: tell the PhaseQueueData object for the previous phase that a
    // task has finished.
    val previousPhaseData = previousPhase match {
      case Some(previousPhaseTypeAndId) =>
        phaseIdToQueueData(previousPhaseTypeAndId._2)
      case None =>
        firstPhase
    }

    previousPhaseData.handleTaskFinished(newPhases)
  }
}

/**
 * Stores metadata about the queue at a particular phase in a stage's execution.
 *
 * Concurrency: all of these methods are called from the LocalDagScheduler, so they're guaranteed
 * to be called from a single thread.
 */
private class PhaseQueueData(
    phaseType: MonotaskType.Value,
    val phaseId: Int,
    private val concurrency: Int) extends Logging {

  /** Prefix to use in all log statements. */
  protected val logPrefix = s"Phase $phaseId ($phaseType):"
  protected var isThrottled = false
  protected var numApprovedToStart: Int = 0
  protected var numFinished: Int = 0

  private var previousPhases = new ArrayBuffer[PhaseQueueData]

  /**
   * Phases that depend on this one completing.
   *
   * This is populated opportunistically (when A finishes and B starts, we can infer that B
   * depended on A), so it may not be complete at any given time.
   */
  private val nextPhaseIdToData = new HashMap[Int, PhaseQueueData]

  def addNextPhaseData(nextPhaseData: PhaseQueueData): Unit = {
    if (!nextPhaseIdToData.contains(nextPhaseData.phaseId)) {
      nextPhaseIdToData.put(nextPhaseData.phaseId, nextPhaseData)
      nextPhaseData.previousPhases += this
      // If the number of tasks that finished on this stage is fewer than the number of tasks that
      // have started on the next stage, something went wrong with our accounting. This might happen
      // for tasks where sometimes tasks in a stage skip a particular stage.
      assert(numFinished >= nextPhaseData.numApprovedToStart)
    }
  }

  /**
   * Approves a new macrotask to start on this phase and propagates the approval back to previous
   * stages if necessary.
   */
  protected def approveTaskToStart(): Unit = {
    var aTaskIsAvailableToStart = true
    previousPhases.foreach { previousPhaseData =>
      if (previousPhaseData.numFinished <= numApprovedToStart) {
        // The previous phase hasn't finished any tasks beyond the ones that this phase has
        // already started.
        aTaskIsAvailableToStart = false
      }
    }

    val enoughResourcesAvailableToStartTask = numApprovedToStart - numFinished < concurrency
    if (aTaskIsAvailableToStart && enoughResourcesAvailableToStartTask) {
      // This phase can start one more macrotask.
      numApprovedToStart += 1
      logDebug(s"$logPrefix approveTaskToStart(): numApprovedToStart is now $numApprovedToStart")
    }
    else {
      // The next phases are demanding tasks faster than we can keep up, so this phase doesn't need
      // to throttle anymore.
      isThrottled = false
    }

    // Only release a task from the previous phase if there are enough resources available to start
    // a task. Otherwise, the length of this phase's queue didn't change, so we shouldn't
    // release a task from the previous phase.
    if (enoughResourcesAvailableToStartTask) {
      // If not enough resources, it means we're clogged up and nothing changed! So nothing in
      // previous should change.
      var throttledPreviousStages = 0
      previousPhases.foreach { previousPhaseData =>
        if (previousPhaseData.isThrottled) {
          throttledPreviousStages += 1
          // If I'm allowed to start one, my queue decreases by one, so the phase before me can now
          // start one too.
          previousPhaseData.approveTaskToStart()
        }
      }
      assert(throttledPreviousStages <= 1,
        s"Throttled previous stages was $throttledPreviousStages! This isn't handled correctly " +
        "right now (to handle this, we'd need to make sure branching in DAG doesn't lead to " +
        "multiple tasks getting approved at an upstream phase).")
    }
  }

  /** Should be called when a macrotask finishes this phase. */
  def handleTaskFinished(startedNewPhases: Set[(MonotaskType.Value, Int)]): Unit = {
    numFinished += 1
    logDebug(s"$logPrefix handleTaskFinished ($numFinished tasks have finished)")

    var needToUpdateThrottling = false
    // For each next phase, if it's not throttled and has enough resources, "start" a monotask on
    // that phase. Only look at the next phases that actually started (any phases that are stored
    // in nextPhaseIdToData but aren't in startedNewPhases must be dependent on other phases
    // that haven't completed yet.
    // TODO: Could consider throttling if tasks finished on this phase but didn't cause next phases
    //       to start, because that means this phase isn't on the critical path. Don't worry about
    //       right now, because for all of the DAGs we have right now, phases that fall into that
    //       category are very short, so not especially important to optimize.
    startedNewPhases.foreach { newPhaseTypeAndId =>
      val nextPhaseData = nextPhaseIdToData(newPhaseTypeAndId._2)
      if (!nextPhaseData.isThrottled &&
           ((numFinished - nextPhaseData.numFinished) <= nextPhaseData.concurrency)) {
        // The next phase isn't throttled, and there are enough resources for the task to start.
        // Note: if this code is reached, this phase is definitely not throttled, because there's
        // no queue on the next resource, and the next resource isn't throttled.
        logDebug(s"$logPrefix handleTaskFinished: approving task to start on next phase with id " +
          s"${nextPhaseData.phaseId} ($numFinished finished, " +
          s"num started on next: ${nextPhaseData.numApprovedToStart}, " +
          s"num finished on next: ${nextPhaseData.numFinished}, " +
          s"concurrency of next: ${nextPhaseData.concurrency})")
        nextPhaseData.numApprovedToStart += 1

        // There shouldn't be any queue on the next phase!
        if (nextPhaseData.numApprovedToStart != numFinished) {
          val errorMessage = (s"$logPrefix After approving task to start on next phase " +
            s"(${nextPhaseData.logPrefix}) has " +
            s"${nextPhaseData.numApprovedToStart} tasks approved to start, which should be " +
            s"equal to the number of tasks that have finished on this phase ($numFinished).")
          logError(errorMessage)
          assert(false)
        }
      } else {
        // There weren't enough resources available in the next phase for the task to start, so
        // the task will be queued. Check to see if this means that this phase now needs to be
        // throttled.
        needToUpdateThrottling = true
      }
    }

    if (needToUpdateThrottling) {
      updateThrottling()
    }

    // If this phase isn't throttled, start a new task in place of the one that finished.
    if (!isThrottled) {
      logDebug(s"$logPrefix handleTaskFinished: approving task to start on this phase")
      approveTaskToStart()
    }
  }

  /**
   * Determines how much this phase should be throttled based on how many tasks have finished
   * on this phase, and how many tasks are queued on the next phase.
   */
  private def updateThrottling(): Unit = {
    // Use all of the next phases to determine how many tasks should have been allowed to start
    // on this phase.
    val previousNumApprovedToStart = numApprovedToStart
    nextPhaseIdToData.valuesIterator.foreach { nextPhaseData =>
      // If necessary, start throttling.
      isThrottled = isThrottled || ((numFinished - nextPhaseData.numApprovedToStart) >= concurrency)
      logDebug(s"$logPrefix updateThrottling: isThrottled is now $isThrottled")
      
      if (isThrottled) {
        // If throttled, if means that the task finishing didn't cause a new task to be approved
        // to start on the next phase, so therefore there's no scenario where numApprovedToStart
        // should get larger here.  Take the minimum so that numApprovedToStart will be the minimum
        // allowed by any of the (potentially > 1) next phases.
        numApprovedToStart = Math.min(
          numApprovedToStart,
          nextPhaseData.numApprovedToStart + concurrency)
        logDebug(s"$logPrefix updateThrottling: based on next phase ${nextPhaseData.phaseId}, " +
          s"Updated approved to start to $numApprovedToStart")
      }
    }

    // If the number of tasks that is approved to change, it has decreased, so we need to update
    // all of the previous stages to be throttled appropriately.
    if (numApprovedToStart != previousNumApprovedToStart) {
      logInfo(s"numApprovedToStart decreased from $previousNumApprovedToStart to " +
        s"$numApprovedToStart, so updating throttling of previous stages")
      previousPhases.foreach(_.updateThrottling())
    }
  }
}

/**
 * Stores metadata about the first phase.
 */
private class FirstPhaseQueueData(
    phaseId: Int,
    private val numTasksInitiallyRequestedFromDriver: Int,
    private val executorBackend: ExecutorBackend)
  extends PhaseQueueData(phaseType = null, phaseId, concurrency = 1)
  with Logging {

  numApprovedToStart = numTasksInitiallyRequestedFromDriver

  /**
   * The first phase should start throttled. Launches should always be triggered by a later
   * phase.
   */
  isThrottled = true

  /**
   * Keep track of the number of tasks that have been requested from the driver, so that we don't
   * erroneously request extra tasks when numApprovedToStart gets reduced to do later throttling.
   */
  private var numTasksRequestedFromDriver = numTasksInitiallyRequestedFromDriver

  override protected def approveTaskToStart(): Unit = {
    numApprovedToStart += 1
    logDebug(s"$logPrefix approveTaskToStart, so numApprovedToStart is now $numApprovedToStart")
    val numTasksToRequest = numApprovedToStart - numTasksRequestedFromDriver
    if (numFinished > numTasksRequestedFromDriver) {
      val errorMessage = (s"$numFinished tasks have finished, which is greater than the number " +
        s"of tasks that have been requested from the driver ($numTasksRequestedFromDriver)." +
        "This likely reflects a problem with the initialization of the number of tasks initially " +
        s"requested from the driver in FirstPhaseQueueData.")
      logError(errorMessage)
      throw new SparkException(errorMessage)
    }

    if (numTasksToRequest > 0) {
      logInfo(s"Requesting $numTasksToRequest new macrotasks from driver " +
        s"($numTasksRequestedFromDriver have already been requested, and $numFinished have been " +
        "received and deserialized).")
      executorBackend.requestTasks(numTasksToRequest)
      numTasksRequestedFromDriver += numTasksToRequest
    }
  }
}
