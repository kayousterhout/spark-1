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

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import java.io.ByteArrayOutputStream
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockManager, ShuffleBlockId, StorageLevel}
import org.apache.spark.executor.ShuffleWriteMetrics

/**
* A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
* specified in the ShuffleDependency).
*
* See [[org.apache.spark.scheduler.Task]] for more information.
*
 * @param stageId id of the stage this task belongs to
 * @param taskBinary broadcast version of of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation])
  extends Task[MapStatus](stageId, partition.index) with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) {
    this(0, null, new Partition { override def index = 0 }, null)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, depRaw) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    val dep = depRaw.asInstanceOf[ShuffleDependency[Any, Any, _]]

    metrics = Some(context.taskMetrics)

    // Optionally do map-side combining before outputting shuffle files.
    val uncombinedIterator =
      rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]]
    val iter = if (dep.aggregator.isDefined && dep.mapSideCombine) {
        dep.aggregator.get.combineValuesByKey(uncombinedIterator, context)
    } else if (dep.aggregator.isEmpty && dep.mapSideCombine) {
      throw new IllegalStateException("Aggregator is empty for map-side combine")
    } else {
      uncombinedIterator
    }

    // Create a different writer for each output bucket.
    val blockManager = SparkEnv.get.blockManager
    val shuffleData = Array.tabulate[SerializedObjectWriter](dep.partitioner.numPartitions) {
      bucketId =>
        new SerializedObjectWriter(blockManager, dep, partitionId, bucketId)
    }
    for (elem <- iter) {
      val bucketId = dep.partitioner.getPartition(elem._1)
      shuffleData(bucketId).write(elem)
    }

    // Store the shuffle data in the block manager and get the sizes.
    val shuffleWriteMetrics = new ShuffleWriteMetrics()
    context.taskMetrics.shuffleWriteMetrics = Some(shuffleWriteMetrics)
    val compressedSizes = shuffleData.map { shuffleWriter =>
      val bytesWritten = shuffleWriter.put()
      shuffleWriteMetrics.shuffleBytesWritten += bytesWritten
      MapOutputTracker.compressSize(bytesWritten)
    }
    context.markTaskCompleted()

    new MapStatus(SparkEnv.get.blockManager.blockManagerId, compressedSizes)
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}

private class SerializedObjectWriter(
    blockManager: BlockManager, dep: ShuffleDependency[_,_,_], partitionId: Int, bucketId: Int) {
  private val byteOutputStream = new ByteArrayOutputStream()
  private val ser = Serializer.getSerializer(dep.serializer.getOrElse(null))
  private val shuffleId = dep.shuffleId
  private val blockId = ShuffleBlockId(shuffleId, partitionId, bucketId)
  private val compressionStream = blockManager.wrapForCompression(blockId, byteOutputStream)
  private val serializationStream = ser.newInstance().serializeStream(compressionStream)

  def write(value: Any) {
    serializationStream.writeObject(value)
  }

  def put(): Long = {
    serializationStream.flush()
    serializationStream.close()
    /* TODO: toByteArray creates a copy of byteOutputStream; change MemoryStore to store streams
     * so that we can avoid this copy (ByteBuffer.wrap does not cause a copy).
     * See http://stackoverflow.com/questions/2716596/
     *   how-to-put-data-from-an-outputstream-into-a-bytebuffer
     * for a way to do this that involves subclassing ByteArrayOutputStream. */
    /* TODO: Need to delete this data after the reduce task completes! */
     val result = blockManager.putBytes(
      blockId, ByteBuffer.wrap(byteOutputStream.toByteArray), StorageLevel.MEMORY_ONLY_SER)
    return result.size
  }
}
