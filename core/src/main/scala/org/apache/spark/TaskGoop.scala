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

package org.apache.spark

import org.apache.spark.executor.{DependencyManager, ExecutorBackend}
import org.apache.spark.util.Utils
import org.apache.spark.monotasks.LocalDagScheduler


/**
 * All of the goop associated with running a macrotask, that may be needed by the monotasks
 * associated with that macrotask.
 *
 * TODO: this is the worst name of all time. Just merge with TaskContext? MonotaskEnv?
 */
private[spark] class TaskGoop(
// TODO: is env thread safe? is this the right way to be passing one around?
    val env: SparkEnv,
    val localDagScheduler: LocalDagScheduler,
    val taskAttemptId: Long,
    val maximumResultSizeBytes: Long,
    val executorBackend: ExecutorBackend,
    val dependencyManager: DependencyManager)
  extends Logging {

  // context is the only part of this that should/can be accessed by external developers.
  // TODO: should the context just get incorporated into this class? generally not attempting
  // to preserve developer API.
  var context: TaskContext = _

  // Should be called during the PrepareMonotask
  def setContext(stageId: Int, partitionId: Int) = {
    context = new TaskContext(stageId, partitionId, taskAttemptId, runningLocally = false)
    context.taskMetrics.hostname = Utils.localHostName()
  }
}
