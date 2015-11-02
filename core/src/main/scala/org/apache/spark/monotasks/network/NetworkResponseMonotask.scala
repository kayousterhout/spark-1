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

import scala.util.control.NonFatal

import com.google.common.base.Throwables

import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener}

import org.apache.spark.{Logging, SparkEnv, TaskContextImpl}
import org.apache.spark.monotasks.{TaskFailure, TaskSuccess}
import org.apache.spark.network.protocol.{BlockFetchFailure, BlockFetchSuccess, Encodable}
import org.apache.spark.storage.BlockId

/**
 * A monotask that sends data over the network in response to a request from a remote executor.
 *
 * @param blockId Block to send over the network. This block should be stored in-memory on the local
 *                BlockManager.
 * @param channel Channel on which to send the block (this will be closed if we can't send data
 *                on it).
 * @param context TaskContextImpl for the monotask.
 */
private[spark] class NetworkResponseMonotask(
    blockId: BlockId,
    channel: Channel,
    context: TaskContextImpl)
  extends NetworkMonotask(context) with Logging {

  val creationTime = System.nanoTime()
  var initiatedResponseTime = 0L

  /**
   * Failure message to respond to the remote executor with. Set only when the request to fetch
   * data has failed.
   */
  private var failureMessage: Option[String] = None

  /**
   * Marks this monotask to respond to the remote executor with a BlockFetchFailure, using the
   * given message as the error string.
   */
  def markAsFailed(message: String): Unit = {
    failureMessage.foreach ( originalMessage  =>
      logWarning(s"Overriding failure message $originalMessage with $message"))
    failureMessage = Some(message)
  }

  override def execute(scheduler: NetworkScheduler): Unit = {
    // If failureMessage has been set, respond with a failure; otherwise, try to fetch the block
    // from in-memory and respond with that.
    failureMessage match {
      case Some(message) =>
        respond(new BlockFetchFailure(blockId.toString(), message), scheduler)

      case None =>
        try {
          val buffer = SparkEnv.get.blockManager.getBlockData(blockId)
          logInfo(s"Responding to ${channel.remoteAddress()} with block $blockId of size " +
            s"${buffer.size()} at ${System.currentTimeMillis()}")
          initiatedResponseTime = System.nanoTime()
          respond(new BlockFetchSuccess(blockId.toString(), buffer), scheduler)
        } catch {
          case NonFatal(t) =>
            respond(
              new BlockFetchFailure(blockId.toString(), Throwables.getStackTraceAsString(t)),
              scheduler)
        }
    }
  }

  /**
   * Responds to a single message with some Encodable object. If a failure occurs while sending,
   * it will be logged and the channel closed.
   */
  private def respond(result: Encodable, scheduler: NetworkScheduler): Unit = {
    val remoteAddress = channel.remoteAddress.toString
    scheduler.addOutstandingBytesToSend(result.encodedLength())
    channel.writeAndFlush(result).addListener(new ChannelFutureListener {
      def operationComplete(future: ChannelFuture) {
        scheduler.addOutstandingBytesToSend(-result.encodedLength())
        if (future.isSuccess) {
          val creationToRequestStart = (initiatedResponseTime - creationTime).toDouble / 1.0e6
          val timeForRequest = (System.nanoTime - initiatedResponseTime).toDouble / 1.0e6
          logInfo(s"Sent result $result (block $blockId) to client $remoteAddress" +
            s"($creationToRequestStart until initiation; $timeForRequest to actually issue; at " +
            s"${System.currentTimeMillis()})")
          // Regardless of whether we responded with BlockFetchSuccess or BlockFetchFailure,
          // from the perspective of the LocalDagScheduler, this monotask has succeeded at its
          // job of responding to the remote executor.
          localDagScheduler.post(TaskSuccess(NetworkResponseMonotask.this, None))
        } else {
          logError(
            s"Error sending result $result to $remoteAddress; closing connection", future.cause)
          localDagScheduler.post(TaskFailure(NetworkResponseMonotask.this, None))
          channel.close
        }
      }
    })
  }
}
