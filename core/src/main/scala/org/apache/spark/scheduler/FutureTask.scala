/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import java.nio.ByteBuffer

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter

/**
 * FutureTask (Drizzle specific) is used to pre-schedule tasks. 
 * See [[org.apache.spark.scheduler.Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 */
private[spark] class FutureTask[U](
    task: Task[U])
  extends Task[U](task.stageId, task.stageAttemptId, task.partitionId, 
    task.internalAccumulators) {

  override def prepTask(context: TaskContext) = {
    task.prepTask(context)
  }

  override def runTask(context: TaskContext): U = {
    // This needs to be changed, for now just called the inner tasks runTask. We
    // should change this so that FutureTask deserializes the actual task etc.
    task.runTask(context)
  }

  override def preferredLocations: Seq[TaskLocation] = task.preferredLocations

  override def toString: String = "FutureTask(%s)".format(task.toString)
}
