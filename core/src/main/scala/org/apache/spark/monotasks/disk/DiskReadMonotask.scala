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

package org.apache.spark.monotasks.disk

import java.io.{DataInputStream, FileInputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import com.google.common.io.ByteStreams
import org.apache.spark.{Logging, TaskContextImpl}
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.storage.{BlockId, ShuffleBlockId, StorageLevel}
import org.apache.spark.util.Utils

/**
 * Contains the parameters and logic necessary to read a block from disk. The execute() method reads
 * a block from disk and stores the resulting serialized data in the MemoryStore.
 */
private[spark] class DiskReadMonotask(
    context: TaskContextImpl, blockId: BlockId, val diskId: String)
  extends DiskMonotask(context, blockId) with Logging {

  resultBlockId = Some(blockId)

  override def execute(): Unit = {
    val file = blockManager.blockFileManager.getBlockFile(blockId, diskId).getOrElse(
      throw new IllegalStateException(
        s"Could not read block $blockId from disk $diskId because its file could not be found."))
    val channel = new RandomAccessFile(file, "r").getChannel

    blockId match {
      case ShuffleBlockId(shuffleId, mapId, reduceId) =>
        try {
          val offsetAndSize = blockManager.getOffsetAndSize(shuffleId, mapId, reduceId)

          channel.position(offsetAndSize._1)

          readAndCacheData(channel, offsetAndSize._2)
        } finally {
          // do nothing.
        }

      case _ =>
        readAndCacheData(channel, file.length().toInt)
    }
  }

  /**
   * Reads the given amount of data from the given steam and saves the result in-memory using the
   * BlockManager.
   *
   * The number of bytes to read needs to be specified as an Int (rather than a Long) because we
   * store the data in a ByteBuffer, and a ByteBuffer cannot be larger than Integer.MAX_VALUE.
   */
  private def readAndCacheData(channel: FileChannel, bytesToRead: Int): Unit = {
    val buffer = ByteBuffer.allocate(bytesToRead)

    try {
      val startTimeNanos = System.nanoTime()
      channel.read(buffer)
      val totalTime = System.nanoTime() - startTimeNanos
      context.taskMetrics.incDiskReadNanos(totalTime)
      logDebug(s"Block $blockId (size: ${Utils.bytesToString(bytesToRead)}) read from " +
        s"disk $diskId in ${totalTime / 1.0e6} ms into $buffer")
    } finally {
      channel.close()
    }

    buffer.flip()
    // The master should never be told about blocks read by a DiskReadMonotask, because they're
    // only read into memory temporarily, and will be dropped from memory as soon as all of
    // this task's dependents finish.
    blockManager.cacheBytes(
      getResultBlockId(), buffer, StorageLevel.MEMORY_ONLY_SER, tellMaster = false)
    context.taskMetrics.getInputMetricsForReadMethod(DataReadMethod.Disk).incBytesRead(bytesToRead)
  }
}
