package org.apache.spark.monotasks.scheduler

import scala.collection.mutable.{HashSet, Set}

import org.apache.spark.monotasks.MonotaskType
import org.apache.spark.scheduler.{TaskSet, WorkerOffer}
import org.apache.spark.Logging

private[spark] class SlotScheduler extends MonotasksScheduler with Logging {

  /** Called on the driver to determine how many tasks to launch initially. */
  def getInitialOffers(
      originalWorkerOffers: Seq[WorkerOffer], taskSet: TaskSet): Seq[WorkerOffer] = {
    // The slot scheduler uses one task per core, so since the original worker offers assume one
    // task per core, just return them!
    val networkSlotsToAdd = if (taskSet.usesNetwork) 1 else 0
    originalWorkerOffers.map {  originalOffer =>
      val diskSlotsToAdd = if (taskSet.usesDisk) originalOffer.totalDisks else 0
      val newSlots = originalOffer.freeSlots + networkSlotsToAdd + diskSlotsToAdd
      logInfo(s"Original offer for ${originalOffer.host} with ${originalOffer.freeSlots} " +
        s"has been increased to add $diskSlotsToAdd disk slots and $networkSlotsToAdd " +
        s"network slot ($newSlots total slots)")
      new WorkerOffer(
        originalOffer.executorId, originalOffer.host, newSlots, originalOffer.totalDisks)
    }
  }

  /** Called on a worker each time a task changes its phase of execution. */
  def handlePhaseChange(
      stageId: Int,
      previousPhase: Option[(MonotaskType.Value, Int)],
      newPhases: Set[(MonotaskType.Value, Int)],
      macrotaskIsFinished: Boolean): Unit = {
    logInfo(
      s"Task in stage $stageId ${previousPhase.map(p => s"finished phase ${p._2}").getOrElse("")}" +
      s"and finished is $macrotaskIsFinished")
    if (macrotaskIsFinished) {
      logInfo(s"Task for stage $stageId has completed, so requesting new task from driver")
      getExecutorBackend().requestTasks(1)
    }
  }
}
