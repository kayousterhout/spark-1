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

package org.apache.spark.shuffle.memory

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.ByteBuffer

import org.apache.spark.{Logging, ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.{SerializationStream, Serializer}
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleWriter}
import org.apache.spark.storage.{BlockManager, ShuffleBlockId, StorageLevel}

/** A ShuffleWriter that stores all shuffle data in memory using the block manager. */
private[spark] class MemoryShuffleWriter[K, V](
    shuffleBlockManager: MemoryShuffleBlockManager,
    handle: BaseShuffleHandle[K, V, _],
    mapId: Int,
    context: TaskContext) extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency

  // Create a different writer for each output bucket.
  private val blockManager = SparkEnv.get.blockManager
  private val numBuckets = dep.partitioner.numPartitions
  logInfo(s"KMZ: MemoryShuffleWriter writing output data to $numBuckets")
  private val shuffleData = Array.tabulate[SerializedObjectWriter](numBuckets) {
    bucketId =>
      new SerializedObjectWriter(blockManager, dep, mapId, bucketId)
  }

  private val shuffleWriteMetrics = new ShuffleWriteMetrics()
  context.taskMetrics().shuffleWriteMetrics = Some(shuffleWriteMetrics)

  override def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    val iter = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        dep.aggregator.get.combineValuesByKey(records, context)
      } else {
        records
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      records
    }

    // Write the data to the appropriate bucket.
    for (elem <- iter) {
      val bucketId = dep.partitioner.getPartition(elem._1)
      shuffleData(bucketId).write(elem)
      shuffleWriteMetrics.incShuffleRecordsWritten(1)
    }
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    // Store the shuffle data in the block manager (if the shuffle was successful) and update the
    // bytes written in ShuffleWriteMetrics.
    val sizes = shuffleData.map { shuffleWriter =>
      val bytesWritten = shuffleWriter.close(success)
      shuffleWriteMetrics.incShuffleBytesWritten(bytesWritten)
      bytesWritten
    }
    if (success) {
      shuffleBlockManager.addShuffleOutput(dep.shuffleId, mapId, numBuckets)
      Some(MapStatus(SparkEnv.get.blockManager.blockManagerId, sizes))
    } else {
      None
    }
  }
}

/** Serializes and optionally compresses data into an in-memory byte stream. */
private[spark] class SerializedObjectWriter(
    blockManager: BlockManager, dep: ShuffleDependency[_,_,_], partitionId: Int, bucketId: Int)
  extends Logging {

  /**
   * A ByteArrayOutputStream that will convert the underlying byte array to a byte buffer without
   * copying all of the data. This is to avoid calling the ByteArrayOutputStream.toByteArray
   * method, because that method makes a copy of the byte array.
   */
  private class ByteArrayOutputStreamWithZeroCopyByteBuffer extends ByteArrayOutputStream {
    def getByteBuffer(): ByteBuffer = ByteBuffer.wrap(buf, 0, size())
  }

  private val byteOutputStream = new ByteArrayOutputStreamWithZeroCopyByteBuffer()
  private val ser = Serializer.getSerializer(dep.serializer.getOrElse(null))
  private val shuffleId = dep.shuffleId
  private val blockId = ShuffleBlockId(shuffleId, partitionId, bucketId)

  /* Only initialize compressionStream and serializationStream if some bytes are written, otherwise
   * 16 bytes will always be written to the byteOutputStream (and those bytes will be unnecessarily
   * transferred to reduce tasks). */
  private var initialized = false
  private var compressionStream: OutputStream = _
  private var serializationStream: SerializationStream = _

  def open() {
    compressionStream = blockManager.wrapForCompression(blockId, byteOutputStream)
    System.out.println(s"Compression stream: $compressionStream")
    serializationStream = ser.newInstance().serializeStream(compressionStream)
    initialized = true
  }

  def write(value: Any) {
    if (!initialized) {
      open()
    }
    serializationStream.writeObject(value)
  }

  def close(saveToBlockManager: Boolean): Long = {
    if (initialized) {
      serializationStream.flush()
      serializationStream.close()
      if (saveToBlockManager) {
        val result = blockManager.cacheBytes(
          blockId,
          byteOutputStream.getByteBuffer(),
          StorageLevel.MEMORY_ONLY_SER,
          tellMaster = false)
        return result.size
      }
    }
    return 0
  }
}
