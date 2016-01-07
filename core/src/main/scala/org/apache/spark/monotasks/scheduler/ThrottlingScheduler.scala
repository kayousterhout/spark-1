package org.apache.spark.monotasks.scheduler

import scala.collection.mutable.{Set, HashMap}

import org.apache.spark.monotasks.MonotaskType
import org.apache.spark.scheduler.{TaskSet, WorkerOffer}

private[spark] class ThrottlingScheduler(
    private val numCores: Int,
    private val numDisks: Int) extends MonotasksScheduler {
  /** For each stage ID, the queue tracker responsible for scheduling tasks for that stage. */
  private val stageIdToQueueTracker = new HashMap[Int, StageQueueTracker]()

  /** Called on the driver to determine how many tasks to launch initially. */
  def getInitialOffers(
      originalWorkerOffers: Seq[WorkerOffer], taskSet: TaskSet): Seq[WorkerOffer] = {
    // The throttling determines the correct number of tasks to assign for each resource, so
    // just use the original worker offers (which should result in one monotask per core).
    return originalWorkerOffers
  }

  /** Called on a worker each time a task changes its phase of execution. */
  def handlePhaseChange(
      stageId: Int,
      previousPhase: Option[(MonotaskType.Value, Int)],
      newPhases: Set[(MonotaskType.Value, Int)],
      macrotaskIsFinished: Boolean): Unit = {
    val stageQueueTracker = stageIdToQueueTracker.getOrElseUpdate(
      stageId,
      new StageQueueTracker(numCores, numDisks, getExecutorBackend())
    )
    stageQueueTracker.handlePhaseChange(previousPhase, newPhases, macrotaskIsFinished)

    if (stageQueueTracker.canBeDeleted) {
      stageIdToQueueTracker.remove(stageId)
    }

  }
}
