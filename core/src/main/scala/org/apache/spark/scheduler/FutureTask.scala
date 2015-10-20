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
import scala.collection.mutable.{HashSet, Stack}

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

  override def prepTask() = {
    task.prepTask()
  }

  override def runTask(context: TaskContext): U = {
    // This needs to be changed, for now just called the inner tasks runTask. We
    // should change this so that FutureTask deserializes the actual task etc.
    task.runTask(context)
  }

  override def preferredLocations: Seq[TaskLocation] = task.preferredLocations

  override def toString: String = "FutureTask(%s)".format(task.toString)

  // Returns the earliest shuffle dependency encountered while walking up the DAG
  // from the RDD being computed by the Task.
  // Returns None if no such dependency exists

  // NOTE: prepTask should be called before this method ?
  // Or should we just call prepTask inside this method ?
  def getFirstShuffleDep: Option[ShuffleDependency[_, _, _]] = {
    val rddToCompute = task match {
      case smt: ShuffleMapTask => smt.rdd
      case rt: ResultTask[_, _] => rt.rdd
      case _ => return null
    }

    if (rddToCompute == null) {
      return None
    }

    val visited = new HashSet[RDD[_]]
    var shuffleDep: Option[ShuffleDependency[_, _, _]] = None

    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              shuffleDep = Some(shufDep)
            case _ =>
          }
          waitingForVisit.push(dep.rdd)
        }
      }
    }

    waitingForVisit.push(rddToCompute)
    // Wait only for the first shuffleDep
    while (waitingForVisit.nonEmpty && shuffleDep.isEmpty) {
      visit(waitingForVisit.pop())
    }
    shuffleDep
  }

}
