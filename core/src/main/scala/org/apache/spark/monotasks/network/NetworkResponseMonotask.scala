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

import org.apache.spark.{Logging, TaskContextImpl}
import org.apache.spark.network.server.TransportRequestHandler
import org.apache.spark.storage.BlockId
import org.apache.spark.monotasks.{TaskFailure, TaskSuccess}

/**
 * A monotask that sends data over the network in response to a request from a remote executor.
 */
private[spark] class NetworkResponseMonotask(
    blockId: BlockId, handler: TransportRequestHandler, context: TaskContextImpl)
  extends NetworkMonotask(context) with Logging {

  override def execute(scheduler: NetworkScheduler): Unit = {
    try {
      // TODO: what happens if the disk read fails? This won't get propagated in the correct way?
      // Have a different context implementation for remote things that makes sure the error is
      // propagated correctly?
      val buffer = context.env.blockManager.getBlockData(blockId)
      handler.sendBlockFetchSuccess(blockId.toString(), buffer)
      // TODO: need to wait to delete data until it's actually been sent over the network -- so
      // need to move the block fetch success code back here.
      context.localDagScheduler.post(TaskSuccess(this, None))
    } catch {
      case t: Throwable =>
        handler.sendBlockFetchFailure(blockId.toString(), t)
      // TODO: make a real failure reason here.
      context.localDagScheduler.post(TaskFailure(this, null))
    }
  }
}
