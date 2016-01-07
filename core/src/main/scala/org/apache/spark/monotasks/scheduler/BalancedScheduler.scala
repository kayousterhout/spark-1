package org.apache.spark.monotasks.scheduler

import scala.collection.mutable.Set

import org.apache.spark.Logging
import org.apache.spark.monotasks.MonotaskType
import org.apache.spark.scheduler.{TaskSet, WorkerOffer}

private[spark] class BalancedScheduler extends MonotasksScheduler with Logging {

  /**
   * Generates initial offers such that the tasks will be evenly distributed across the given
   * workers, and assigned immediately.
   */
  def getInitialOffers(
      originalWorkerOffers: Seq[WorkerOffer], taskSet: TaskSet): Seq[WorkerOffer] = {
    val numWorkers = originalWorkerOffers.size
    val numTasks = taskSet.tasks.length.toDouble
    val tasksPerWorker = Math.ceil(numTasks / numWorkers).toInt
    originalWorkerOffers.map {
      case WorkerOffer(executorId, host, freeSlots, numDisks) =>
        // To be safe, create a new WorkerOffer, but it would probably be fine to modify the old
        // one instead.
        logInfo(s"Changing worker offer for $host from $freeSlots free slots to $tasksPerWorker " +
          s"(to accommodate $numTasks total tasks)")
        new WorkerOffer(executorId, host, tasksPerWorker, numDisks)
    }
  }

  /** Called on a worker each time a task changes its phase of execution. */
  @Override def handlePhaseChange(
      stageId: Int,
      previousPhase: Option[(MonotaskType.Value, Int)],
      newPhaseType: Set[(MonotaskType.Value, Int)],
      macrotaskIsFinished: Boolean): Unit = {
    // Do nothing! All of the tasks should have been assigned at the beginning.
  }
}
