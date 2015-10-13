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

import java.io.{DataInputStream, FileInputStream}
import java.nio.ByteBuffer

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
    val stream = new FileInputStream(file)

    blockId match {
      case ShuffleBlockId(shuffleId, mapId, reduceId) =>
        // Need to read the index data to find the correct location.
        val in = new DataInputStream(stream)
        try {
          val indexOffset = reduceId * 4
          ByteStreams.skipFully(in, indexOffset)
          val offset = in.readInt()
          val nextOffset = in.readInt()

          // The offset describes the offset of the shuffle data, relative to the beginning of the
          // file. We need to subtract all of the data we've already read (or skipped), which
          // includes the data skipped to get to the index information, plus 8 bytes for the two
          // integers we read describing the offset.
          val bytesToSkip = offset - indexOffset - 8
          ByteStreams.skipFully(stream, bytesToSkip)

          readAndCacheData(stream, nextOffset - offset)
        } finally {
          in.close()
        }

      case _ =>
        readAndCacheData(stream, file.length().toInt)
    }
  }

  /**
   * Reads the given amount of data from the given steam and saves the result in-memory using the
   * BlockManager.
   */
  private def readAndCacheData(stream: FileInputStream, bytesToRead: Long) = {
    val channel = stream.getChannel()
    // Assume that the size of the shuffle block is small enough that an Int can describe the
    // length (we need to do this because a ByteBuffer cannot be larger than Integer.MAX_VALUE).
    val buffer = ByteBuffer.allocate(bytesToRead.toInt)

    try {
      val startTimeMillis = System.currentTimeMillis()
      channel.read(buffer)
      logDebug(s"Block $blockId (size: ${Utils.bytesToString(bytesToRead)}) read from " +
        s"disk $diskId in ${System.currentTimeMillis - startTimeMillis} ms into $buffer")
    } finally {
      channel.close()
      stream.close()
    }

    buffer.flip()
    blockManager.cacheBytes(getResultBlockId(), buffer, StorageLevel.MEMORY_ONLY_SER, true)
    context.taskMetrics.getInputMetricsForReadMethod(DataReadMethod.Disk).incBytesRead(bytesToRead)
  }
}
