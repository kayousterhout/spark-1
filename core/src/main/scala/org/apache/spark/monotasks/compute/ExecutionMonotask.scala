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

package org.apache.spark.monotasks.compute

import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.executor.ExecutorUncaughtExceptionHandler
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{DirectTaskResult, IndirectTaskResult}
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.storage.{StorageLevel, TaskResultBlockId}
import org.apache.spark.util.Utils

/** Monotask that handles executing the compute part of a macro task. */
private[spark] abstract class ExecutionMonotask[T, U: ClassTag](
    goop: TaskGoop,
    val rdd: RDD[T],
    val split: Partition)
  extends ComputeMonotask(goop.localDagScheduler) with Logging {

  def getResult(): U

  override def execute() = {
    // The closure serializer is used to serialize results, for consistency with Spark.
    val closureSerializer = SparkEnv.get.closureSerializer.newInstance()
    try {
      val result = getResult()
      goop.context.markTaskCompleted()
      serializeAndSendResult(result, closureSerializer)
    } catch {
      case t: Throwable => {
        // Attempt to exit cleanly by informing the driver of our failure.
        // If anything goes wrong (or this was a fatal exception), we will delegate to
        // the default uncaught exception handler, which will terminate the Executor.
        logError(s"Exception in TID ${goop.taskAttemptId}", t)

        // TODO: support metrics
        val reason = ExceptionFailure(
          t.getClass.getName, t.getMessage, t.getStackTrace, None)
        goop.executorBackend.statusUpdate(
          goop.taskAttemptId, TaskState.FAILED, closureSerializer.serialize(reason))

        // Don't forcibly exit unless the exception was inherently fatal, to avoid
        // stopping other tasks unnecessarily.
        if (Utils.isFatalError(t)) {
          ExecutorUncaughtExceptionHandler.uncaughtException(t)
        }
      }
    }
  }

  private def serializeAndSendResult(result: U, closureSerializer: SerializerInstance) {
    val resultSer = SparkEnv.get.serializer.newInstance()
    val valueBytes = resultSer.serialize(result)

    // TODO: deal with accumulator updates (previously, accumulators done using thread locals,
    // which don't work anymore!
    goop.context.taskMetrics.setJvmGCTime()
    val directResult = new DirectTaskResult(
      valueBytes, new HashMap[Long, Any](), goop.context.taskMetrics)
    val serializedDirectResult = closureSerializer.serialize(directResult)
    val resultSize = serializedDirectResult.limit

    // directSend = sending directly back to the driver
    val (serializedResult, directSend) = {
      if (resultSize >= goop.maximumResultSizeBytes) {
        val blockId = TaskResultBlockId(goop.taskAttemptId)
        SparkEnv.get.blockManager.putBytes(
          blockId, serializedDirectResult, StorageLevel.MEMORY_AND_DISK_SER)
        (closureSerializer.serialize(new IndirectTaskResult[Any](blockId)), false)
      } else {
        (serializedDirectResult, true)
      }
    }

    goop.executorBackend.statusUpdate(goop.taskAttemptId, TaskState.FINISHED, serializedResult)

    if (directSend) {
      logInfo(s"Finished TID ${goop.taskAttemptId}. $resultSize bytes result sent to driver")
    } else {
      logInfo(
        s"Finished TID ${goop.taskAttemptId}. $resultSize bytes result sent via BlockManager)")
    }
  }
}