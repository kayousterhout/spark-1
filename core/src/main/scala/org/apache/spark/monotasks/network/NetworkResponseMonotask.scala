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

import com.google.common.base.Throwables

import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener}

import org.apache.spark.{Logging, TaskContextImpl}
import org.apache.spark.network.protocol.{BlockFetchFailure, BlockFetchSuccess, Encodable}
import org.apache.spark.storage.BlockId
import org.apache.spark.monotasks.{TaskFailure, TaskSuccess}

/**
 * A monotask that sends data over the network in response to a request from a remote executor.
 */
private[spark] class NetworkResponseMonotask(
    blockId: BlockId, channel: Channel, context: TaskContextImpl)
  extends NetworkMonotask(context) with Logging {

  override def execute(scheduler: NetworkScheduler): Unit = {
    try {
      // TODO: what happens if the disk read fails? This won't get propagated in the correct way?
      // Have a different context implementation for remote things that makes sure the error is
      // propagated correctly?
      val buffer = context.env.blockManager.getBlockData(blockId)
      respond(new BlockFetchSuccess(blockId.toString(), buffer))
    } catch {
      case t: Throwable =>
        respond(new BlockFetchFailure(blockId.toString(), Throwables.getStackTraceAsString(t)))
      // TODO: make a real failure reason here! Need to pass on the throwable t.
    }
  }

  /**
   * Responds to a single message with some Encodable object. If a failure occurs while sending,
   * it will be logged and the channel closed.
   */
  private def respond(result: Encodable) {
    val remoteAddress: String = channel.remoteAddress.toString
    channel.writeAndFlush(result).addListener(new ChannelFutureListener {
      def operationComplete(future: ChannelFuture) {
        if (future.isSuccess) {
          logInfo(s"Sent result $result to client $remoteAddress")
          result match {
            case _: BlockFetchSuccess =>
              context.localDagScheduler.post(TaskSuccess(NetworkResponseMonotask.this, None))
            case _: BlockFetchFailure =>
              // TODO: make proper exception here
              context.localDagScheduler.post(TaskFailure(NetworkResponseMonotask.this, null))
          }
        }
        else {
          logInfo(
            s"Error sending result $result to $remoteAddress; closing connection", future.cause)
          // TODO: make proper exception using future.cause!
          context.localDagScheduler.post(TaskFailure(NetworkResponseMonotask.this, null))
          channel.close
        }
      }
    })
  }
}
