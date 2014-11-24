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

package org.apache.spark.monotasks.network

import scala.util.Success
import scala.util.Failure

import org.apache.spark._
import org.apache.spark.network.{BufferMessage, ConnectionManagerId}
import org.apache.spark.storage.{BlockManagerId, BlockId, BlockMessage, BlockMessageArray,
  GetBlock, MonotaskResultBlockId, ShuffleBlockId}
import org.apache.spark.util.Utils
import org.apache.spark.monotasks.Monotask


/* TODO: currently this describes a shuffle -- not a more general network monotask.
* @param remoteAddress remote BlockManager to fetch from.
* @param blocks        Sequence of tuples, where the first element is the block id,
*                      and the second element is the estimated size, used to calculate
*                      bytesInFlight.
* @param partitionId   Partition id for the macrotask corresponding to this network monotask.
*                      Used only when there are errors (to report them back to the master).
*/
private[spark] class NetworkMonotask(
    val goop: TaskGoop,
    val remoteAddress: BlockManagerId,
    val blocks: Seq[(BlockId, Long)],
    val partitionId: Int)
  extends Monotask(goop.localDagScheduler) with Logging {

  val size = blocks.map(_._2).sum
  val connectionManagerId = new ConnectionManagerId(remoteAddress.host, remoteAddress.port)
  // TODO: this fromGetBlock stuff is crap...fix it.
  val blockMessageArray = new BlockMessageArray(blocks.map {
    case (blockId, size) => BlockMessage.fromGetBlock(GetBlock(blockId))
  })

  def execute() {
    logDebug(s"Sending request for ${blocks.size} blocks (${Utils.bytesToString(size)}}) " +
      s"from $remoteAddress")
    val future = goop.env.blockManager.connectionManager.sendMessageReliably(
        connectionManagerId, blockMessageArray.toBufferMessage)

    implicit val futureExecContext = goop.env.blockManager.futureExecContext
    future.onComplete {
      case Success(message) => {
        // TODO: make new putValue method in blockmanager
        goop.env.blockManager.memoryStore.putValue(
          new MonotaskResultBlockId(taskId), message.asInstanceOf[BufferMessage])
        localDagScheduler.handleTaskCompletion(this)
      }

      case Failure(exception) => {
        if (!blocks.isEmpty) {
          logError(s"Could not get block(s) from $connectionManagerId: $exception")
          // Consistent with the old Spark code, only report the first failed block.
          // TODO: Report all failed blocks, since there could have been other mappers on the same
          //       machine that failed.
          val failedBlockId = blocks(0)._1
          failedBlockId match {
            case ShuffleBlockId(shuffleId, mapId, _) =>
              val failureReason = FetchFailed(remoteAddress, shuffleId, mapId, partitionId)
              val serializedFailureReason =
                goop.env.closureSerializer.newInstance().serialize(failureReason)
              /* TODO: propagate this back to local dag scheduler -- which will need to fail other
               *       monotasks that depend on this one, and should handle notifying the executor
               *       backend of the failure. */
              goop.executorBackend.statusUpdate(
                goop.taskAttemptId, TaskState.FAILED, serializedFailureReason)

            case _ =>
              val exception = new SparkException(
                s"Failed to get block $failedBlockId, which is not a shuffle block")
              val reason = new ExceptionFailure(
                exception.getClass.getName,
                exception.getMessage,
                exception.getStackTrace,
                Some(goop.context.taskMetrics))
              val serializedReason = goop.env.closureSerializer.newInstance().serialize(reason)
              goop.executorBackend.statusUpdate(taskId, TaskState.FAILED, serializedReason)
          }
        }
      }
    }
  }
}