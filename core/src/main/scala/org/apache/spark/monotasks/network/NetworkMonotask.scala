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

package org.apache.spark.monotasks.network

import scala.util.{Success, Failure}

import org.apache.spark._
import org.apache.spark.monotasks.Monotask
import org.apache.spark.network.{BufferMessage, ConnectionManagerId}
import org.apache.spark.storage.{BlockManagerId, BlockId, BlockMessage, BlockMessageArray,
  GetBlock, MonotaskResultBlockId, ShuffleBlockId}
import org.apache.spark.util.Utils


/**
 * A monotask that uses the network to fetch shuffle data.  This monotask handles only fetching
 * data, and does not deserialize it.
 *
 * @param remoteAddress remote BlockManager to fetch from.
 * @param blocks        Sequence of tuples, where the first element is the block id,
 *                      and the second element is the estimated size, used to calculate
 *                      bytesInFlight.
 * @param partitionId   Partition id for the macrotask corresponding to this network monotask.
 *                      Used only when there are errors (to report them back to the master).
 */
private[spark] class NetworkMonotask(
    context: TaskContext,
    val remoteAddress: BlockManagerId,
    val blocks: Seq[(BlockId, Long)],
    val partitionId: Int)
  extends Monotask(context) with Logging {

  val resultBlockId = new MonotaskResultBlockId(taskId)

  val size = blocks.map(_._2).sum
  val connectionManagerId = new ConnectionManagerId(remoteAddress.host, remoteAddress.port)
  // TODO: this fromGetBlock stuff is crap (why make a GetBlock just to call fromGetBlock?). Fix it.
  val blockMessageArray = new BlockMessageArray(blocks.map {
    case (blockId, size) => BlockMessage.fromGetBlock(GetBlock(blockId))
  })

  def execute() {
    logDebug(s"Sending request for ${blocks.size} blocks (${Utils.bytesToString(size)}}) " +
      s"from $remoteAddress")
    val future = context.env.blockManager.connectionManager.sendMessageReliably(
        connectionManagerId, blockMessageArray.toBufferMessage)

    // TODO: This execution context should not go through the block manager (should be handled by
    // the network monotask scheduler -- since it is the thread used to execute the network
    // callbacks).  Or consider integrating this with compute monotasks -- since this is
    // computation?
    implicit val futureExecContext = context.env.blockManager.futureExecContext
    future.onComplete {
      case Success(message) => {
        context.env.blockManager.memoryStore.putValue(
          resultBlockId, message.asInstanceOf[BufferMessage])
        context.localDagScheduler.handleTaskCompletion(this)
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
                context.env.closureSerializer.newInstance().serialize(failureReason)
              context.localDagScheduler.handleTaskFailure(this, serializedFailureReason)

            case _ =>
              val exception = new SparkException(
                s"Failed to get block $failedBlockId, which is not a shuffle block")
              val reason = new ExceptionFailure(
                exception.getClass.getName,
                exception.getMessage,
                exception.getStackTrace,
                Some(context.taskMetrics))
              val serializedFailureReason =
                context.env.closureSerializer.newInstance().serialize(reason)
              context.localDagScheduler.handleTaskFailure(this, serializedFailureReason)
          }
        }
      }
    }
  }
}
