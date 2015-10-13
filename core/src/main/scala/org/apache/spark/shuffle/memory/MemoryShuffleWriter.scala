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

import java.io.OutputStream
import java.nio.ByteBuffer

import org.apache.spark.{ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.{SerializationStream, Serializer}
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleWriter}
import org.apache.spark.storage.{BlockManager, MultipleShuffleBlocksId, ShuffleBlockId,
  StorageLevel}
import org.apache.spark.util.ByteArrayOutputStreamWithZeroCopyByteBuffer

/** A ShuffleWriter that stores all shuffle data in memory using the block manager. */
private[spark] class MemoryShuffleWriter[K, V](
    shuffleBlockManager: MemoryShuffleBlockManager,
    handle: BaseShuffleHandle[K, V, _],
    private val mapId: Int,
    context: TaskContext) extends ShuffleWriter[K, V] {

  private val dep = handle.dependency

  // Create a different writer for each output bucket.
  private val blockManager = SparkEnv.get.blockManager
  private val numBuckets = dep.partitioner.numPartitions
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

  /**
   * Writes information about where each shuffle block is located within the single file that holds
   * all of the shuffle data.  For shuffle block i, the Int at byte 4*i describes the offset
   * of the first byte of the block, and the Int at byte 4*(i+1) describes the offset of the
   * first byte of the next block.
   */
  private def writeIndexInformation(buffer: ByteBuffer, sizes: Seq[Int], indexSize: Int): Unit = {
    var offset = indexSize
    buffer.putInt(offset)
    sizes.foreach { size =>
      offset += size
      buffer.putInt(offset)
    }
  }

  /**
   * Stops writing shuffle data by storing the shuffle data in the block manager (if the shuffle
   * was successful) and updating the bytes written in the task's ShuffleWriteMetrics.
   */
  override def stop(success: Boolean): Option[MapStatus] = {
    val byteBuffers = shuffleData.map(_.closeAndGetBytes())
    val sizes = byteBuffers.map(_.limit)
    val totalDataSize = sizes.sum
    shuffleWriteMetrics.incShuffleBytesWritten(totalDataSize)

    if (success) {
      // Create a new buffer that first has index information, and then has all of the shuffle
      // data. Use a direct byte buffer, so that when a disk monotask writes this data
      // to disk, it can avoid copying the data out of the JVM, which can be time consuming
      // (e.g., hundreds of milliseconds for buffers with ~100MB).
      // TODO: Could wait and write this (relatively small) index data at the beginning of the
      //       disk write monotask, which would allow the disk scheduler to combine multiple
      //       outputs from different map tasks into a single file (or alternately, could just
      //       store the index data in-memory).
      val indexSize = (sizes.length + 1) * 4
      val bufferSize = indexSize + totalDataSize
      val directBuffer = ByteBuffer.allocateDirect(bufferSize)

      writeIndexInformation(directBuffer, sizes, indexSize)

      // Write all of the shuffle data.
      byteBuffers.foreach(directBuffer.put(_))

      // Store the data in the block manager.
      blockManager.cacheBytes(
        MultipleShuffleBlocksId(dep.shuffleId, mapId),
        directBuffer,
        StorageLevel.MEMORY_ONLY_SER,
        tellMaster = false)

      shuffleBlockManager.addShuffleOutput(dep.shuffleId, mapId, numBuckets)
      Some(MapStatus(SparkEnv.get.blockManager.blockManagerId, sizes.map(_.toLong)))
    } else {
      None
    }
  }
}

/** Serializes and optionally compresses data into an in-memory byte stream. */
private[spark] class SerializedObjectWriter(
    blockManager: BlockManager, dep: ShuffleDependency[_,_,_], partitionId: Int, bucketId: Int) {

  private val byteOutputStream = new ByteArrayOutputStreamWithZeroCopyByteBuffer()
  private val ser = Serializer.getSerializer(dep.serializer.getOrElse(null))
  private val shuffleId = dep.shuffleId
  val blockId = ShuffleBlockId(shuffleId, partitionId, bucketId)

  /* Only initialize compressionStream and serializationStream if some bytes are written, otherwise
   * 16 bytes will always be written to the byteOutputStream (and those bytes will be unnecessarily
   * transferred to reduce tasks). */
  private var initialized = false
  private var compressionStream: OutputStream = _
  private var serializationStream: SerializationStream = _

  def open() {
    compressionStream = blockManager.wrapForCompression(blockId, byteOutputStream)
    serializationStream = ser.newInstance().serializeStream(compressionStream)
    initialized = true
  }

  def write(value: Any) {
    if (!initialized) {
      open()
    }
    serializationStream.writeObject(value)
  }

  /**
   * Closes the byte stream and returns a ByteBuffer containing the serialized data. Returns None
   * if no data was written.
   */
  def closeAndGetBytes(): ByteBuffer = {
    if (initialized) {
      serializationStream.flush()
      serializationStream.close()
      Some(byteOutputStream.getByteBuffer())
    }
    byteOutputStream.getByteBuffer()
  }
}
