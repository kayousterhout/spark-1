package org.apache.spark.monotasks.scheduler

import scala.collection.mutable.Set

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.executor.ExecutorBackend
import org.apache.spark.monotasks.MonotaskType
import org.apache.spark.scheduler.{TaskSet, WorkerOffer}

/**
 * An interface for monotasks scheduler. Will be called on the driver to determine how many
 * tasks from each job should initially be assigned to each machine, and will also be used
 * by the worker to determine when to request more tasks from the master.
 */
private[spark] trait MonotasksScheduler {
  /** ExecutorBackend to use to request more tasks from the driver. */
  private var executorBackend: Option[ExecutorBackend] = None

  /** Called on the driver to determine how many tasks to launch initially. */
  def getInitialOffers(
      originalWorkerOffers: Seq[WorkerOffer], taskSet: TaskSet): Seq[WorkerOffer]

  /**
   * Called on a worker each time a task starts and/or finishes a phase of execution.
   *
   * A phase is a portion of execution that uses a single resource, and may include multiple
   * monotasks.  For example, if two compute monotasks run back to back, the MonotasksScheduler
   * will be notified about a single compute phase.
   *
   * previousStage will be None when the first monotask for a particular macrotask is launched.
   */
  def handlePhaseChange(
      stageId: Int,
      previousPhase: Option[(MonotaskType.Value, Int)],
      newPhases: Set[(MonotaskType.Value, Int)],
      macrotaskIsFinished: Boolean): Unit

  def setExecutorBackend(executorBackend: ExecutorBackend): Unit = {
    this.executorBackend = Some(executorBackend)
  }

  protected def getExecutorBackend(): ExecutorBackend = {
    executorBackend.getOrElse(
      throw new IllegalStateException("MonotaskScheduler's ExecutorBackend was attempted to be " +
        "used before it was initialized"))
  }
}

private[spark] object MonotasksScheduler extends Logging {
  def getScheduler(conf: SparkConf, numDisks: Int, numCores: Int): MonotasksScheduler = {

    val schedulerName = conf.get("spark.monotasks.scheduler", "throttling")
    logInfo(s"Using monotasks scheduler: $schedulerName")

    schedulerName match {
      case "slot" =>
        // Instantiate slot-based scheduler
        new SlotScheduler

      case "balanced" =>
        // Instantiate scheduler that evenly balances across machines.
        new BalancedScheduler

      case _ =>
        // Instantiate throttling scheduler
        new ThrottlingScheduler(numCores, numDisks)
    }
  }
}
