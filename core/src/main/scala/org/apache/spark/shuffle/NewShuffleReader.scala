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

package org.apache.spark.shuffle

import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark._
import org.apache.spark.monotasks.Monotask
import org.apache.spark.monotasks.network.NetworkMonotask
import org.apache.spark.network.BufferMessage
import org.apache.spark.storage._

/**
 * Handles reading shuffle data over the network and deserializing, aggregating, and sorting the
 * result.
 *
 * TODO
 * @param shuffleDependency
 * @param reduceId The partition ID corresponding to the reduce task.
 * @param context
 */
class NewShuffleReader[K, V, C](
    shuffleDependency: ShuffleDependency[K, V, C],
    reduceId: Int,
    context: TaskContext)
  extends Logging {

  private val localBlockIds = new ArrayBuffer[BlockId]()

  def getReadMonotasks(): Seq[Monotask] = {
    // Get the locations of the map output.
    // TODO: Should this fetching of server statuses happen in a network monotask? Could
    //       involve network (to send message to the master); this should be measured.
    val startTime = System.currentTimeMillis
    val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(
      shuffleDependency.shuffleId, reduceId)
    logDebug("Fetching map output location for shuffle %d, reduce %d took %d ms".format(
      shuffleDependency.shuffleId, reduceId, System.currentTimeMillis - startTime))

    // Organize the blocks based on the block manager address.
    val blocksByAddress = new HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long)]]
    for (((address, size), index) <- statuses.zipWithIndex) {
      val blockId = new ShuffleBlockId(shuffleDependency.shuffleId, index, reduceId)
      blocksByAddress.getOrElseUpdate(address, ArrayBuffer()) += ((blockId, size))
    }

    // Split local and remote blocks.
    val fetchMonotasks = new ArrayBuffer[NetworkMonotask]
    var totalBlocks = 0
    for ((address, blockInfos) <- blocksByAddress) {
      totalBlocks += blockInfos.size
      val nonEmptyBlocks = blockInfos.filter(_._2 != 0)
      if (address == context.env.blockManager.blockManagerId) {
        // Filter out zero-sized blocks
        localBlockIds ++= nonEmptyBlocks.map(_._1)
      } else {
        val networkMonotask = new NetworkMonotask(context, address, nonEmptyBlocks, reduceId)
        localBlockIds.append(new MonotaskResultBlockId(networkMonotask.taskId))
        fetchMonotasks.append(networkMonotask)
      }
    }
    fetchMonotasks
  }

  def getDeserializedAggregatedSortedData(): Iterator[(K, C)] = {
    val iter = localBlockIds.iterator.flatMap {
      case shuffleBlockId: ShuffleBlockId =>
        // The map task was run on this machine, so the memory store has the deserialized shuffle
        // data.  The block manager transparently handles deserializing the data.
        // TODO: handle the case where the shuffle data doesn't exist due to an error.
        context.env.blockManager.get(shuffleBlockId).get.data

      case monotaskResultBlockId: MonotaskResultBlockId =>
        // TODO: handle case where the block doesn't exist.
        val bufferMessage = context.env.blockManager.memoryStore.getValue(monotaskResultBlockId).get
          .asInstanceOf[BufferMessage]
        val blockMessageArray = BlockMessageArray.fromBufferMessage(bufferMessage)
        blockMessageArray.flatMap { blockMessage =>
          if (blockMessage.getType != BlockMessage.TYPE_GOT_BLOCK) {
            // TODO: exceptions are evil. instead tell scheduler that task failed. Also right now
            //       exceptions will be lost in the abyss (because won't interrupt the executor
            //       thread).
            // TODO: log here where the exception came from (which block manager)
            throw new SparkException(
              "Unexpected message " + blockMessage.getType + " received")
          }
          val blockId = blockMessage.getId
          val networkSize = blockMessage.getData.limit()
          //readMetrics.remoteBytesRead += networkSize
          //readMetrics.remoteBlocksFetched += 1
          // TODO: is block manager the best place for this deserialization code?
          // THIS IS LAZY. Just returns an iterator.
          val deserializedData = context.env.blockManager.dataDeserialize(
            blockId,
            blockMessage.getData,
            shuffleDependency.serializer.getOrElse(context.env.serializer))
          deserializedData
        }

      case _ =>
        // TODO: don't throw an exception!!!
        throw new SparkException("Unexpected type of shuffle ID!")
    }

    getMaybeSortedIterator(getMaybeAggregatedIterator(iter))
  }

  /** If an aggregator is defined for the shuffle, returns an aggregated iterator. */
  private def getMaybeAggregatedIterator(iterator: Iterator[Any]): Iterator[Product2[K, C]] = {
    if (shuffleDependency.aggregator.isDefined) {
      if (shuffleDependency.mapSideCombine) {
        new InterruptibleIterator(
          context,
          shuffleDependency.aggregator.get.combineCombinersByKey(
            iterator.asInstanceOf[Iterator[_ <: Product2[K, C]]]))
      } else {
        new InterruptibleIterator(
          context,
          shuffleDependency.aggregator.get.combineValuesByKey(
            iterator.asInstanceOf[Iterator[_ <: Product2[K, V]]]))
      }
    } else if (shuffleDependency.aggregator.isEmpty && shuffleDependency.mapSideCombine) {
      throw new IllegalStateException("Aggregator is empty for map-side combine")
    } else {
      // Convert the Product2s to pairs since this is what downstream RDDs currently expect
      iterator.asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
    }
  }

  /** If an ordering is defined, returns a sorted version of the iterator. */
  private def getMaybeSortedIterator(iterator: Iterator[Product2[K, C]]): Iterator[(K, C)] = {
    val sortedIter = shuffleDependency.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Define a Comparator for the whole record based on the key ordering.
        val cmp = new Ordering[Product2[K, C]] {
          override def compare(o1: Product2[K, C], o2: Product2[K, C]): Int = {
            keyOrd.compare(o1._1, o2._1)
          }
        }
        val sortBuffer: Array[Product2[K, C]] = iterator.toArray
        scala.util.Sorting.quickSort(sortBuffer)(cmp)
        sortBuffer.iterator
      case None =>
        iterator
    }
    sortedIter.asInstanceOf[Iterator[(K, C)]]
  }
}
