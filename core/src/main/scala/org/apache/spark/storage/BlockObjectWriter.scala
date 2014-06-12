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

package org.apache.spark.storage

import util.Random

import scala.collection.mutable.ArrayBuffer

import java.io.{FileOutputStream, File, OutputStream}
import java.nio.channels.FileChannel

import it.unimi.dsi.fastutil.io.FastBufferedOutputStream

import org.apache.spark.{Logging}
import org.apache.spark.serializer.{SerializationStream, Serializer}
import org.apache.spark.executor.TaskMetrics

/**
 * An interface for writing JVM objects to some underlying storage. This interface allows
 * appending data to an existing block, and can guarantee atomicity in the case of faults
 * as it allows the caller to revert partial writes.
 *
 * This interface does not support concurrent writes.
 */
private[spark] abstract class BlockObjectWriter(val blockId: BlockId) {

  def open(): BlockObjectWriter

  def close()

  def isOpen: Boolean

  /**
   * Flush the partial writes and commit them as a single atomic block. Return the
   * number of bytes written for this commit.
   */
  def commit(): Long

  /**
   * Reverts writes that haven't been flushed yet. Callers should invoke this function
   * when there are runtime exceptions.
   */
  def revertPartialWrites()

  /**
   * Writes an object.
   */
  def write(value: Any)

  /**
   * Returns the file segment of committed data that this Writer has written.
   */
  def fileSegment(): FileSegment

  /**
   * Cumulative time spent performing blocking writes, in ns.
   */
  def timeWriting(): Long

  /**
   * Total values serialized.
   */
  def itemsSerialized(): Int

  /**
   * Sampled times to serialize.
   */
  def serializationSamples(): ArrayBuffer[Long]

  /**
   * Number of bytes written so far
   */
  def bytesWritten: Long
}

/** BlockObjectWriter which writes directly to a file on disk. Appends to the given file. */
private[spark] class DiskBlockObjectWriter(
    blockId: BlockId,
    file: File,
    serializer: Serializer,
    bufferSize: Int,
    compressStream: OutputStream => OutputStream,
    syncWrites: Boolean)
  extends BlockObjectWriter(blockId)
  with Logging
{

  /** Intercepts write calls and tracks total time spent writing. Not thread safe. */
  private class TimeTrackingOutputStream(out: OutputStream) extends OutputStream {
    def timeWriting = _timeWriting
    private var _timeWriting = 0L

    private def callWithTiming(f: => Unit) = {
      val start = System.nanoTime()
      f
      _timeWriting += (System.nanoTime() - start)
    }

    def write(i: Int): Unit = callWithTiming(out.write(i))
    override def write(b: Array[Byte]) = callWithTiming(out.write(b))
    override def write(b: Array[Byte], off: Int, len: Int) = callWithTiming(out.write(b, off, len))
    override def close() = out.close()
    override def flush() = out.flush()
  }

  /** The file channel, used for repositioning / truncating the file. */
  private var channel: FileChannel = null
  private var bs: OutputStream = null
  private var fos: FileOutputStream = null
  private var ts: TimeTrackingOutputStream = null
  private var objOut: SerializationStream = null
  private val initialPosition = file.length()
  private var lastValidPosition = initialPosition
  private var initialized = false
  private var _timeWriting = 0L
  private var totalValuesSerialized = 0
  private var _serializationSamples = new ArrayBuffer[Long]
  private var sampleProbability = TaskMetrics.getSerializeSampleProbability()

  override def open(): BlockObjectWriter = {
    fos = new FileOutputStream(file, true)
    ts = new TimeTrackingOutputStream(fos)
    channel = fos.getChannel()
    lastValidPosition = initialPosition
    bs = compressStream(new FastBufferedOutputStream(ts, bufferSize))
    objOut = serializer.newInstance().serializeStream(bs)
    initialized = true
    this
  }

  override def close() {
    if (initialized) {
      if (syncWrites) {
        // Force outstanding writes to disk and track how long it takes
        objOut.flush()
        val start = System.nanoTime()
        fos.getFD.sync()
        _timeWriting += System.nanoTime() - start
      }
      objOut.close()

      _timeWriting += ts.timeWriting

      channel = null
      bs = null
      fos = null
      ts = null
      objOut = null
      initialized = false
    }
  }

  override def isOpen: Boolean = objOut != null

  override def commit(): Long = {
    if (initialized) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      objOut.flush()
      bs.flush()
      val prevPos = lastValidPosition
      lastValidPosition = channel.position()
      lastValidPosition - prevPos
    } else {
      // lastValidPosition is zero if stream is uninitialized
      lastValidPosition
    }
  }

  override def revertPartialWrites() {
    if (initialized) {
      // Discard current writes. We do this by flushing the outstanding writes and
      // truncate the file to the last valid position.
      objOut.flush()
      bs.flush()
      channel.truncate(lastValidPosition)
    }
  }

  override def write(value: Any) {
    if (!initialized) {
      open()
    }

    totalValuesSerialized += 1

    if (Random.nextDouble() < sampleProbability) {
      // Read the object and track how long it takes.
      val start = System.nanoTime()
      val startTimeWriting = ts.timeWriting
      objOut.writeObject(value)
      val totalTimeElapsed = System.nanoTime() - start
      // To get the serialization time, need to deduct time spent writing output to disk.
      val serializationTime = totalTimeElapsed - (ts.timeWriting - startTimeWriting)
      _serializationSamples += serializationTime
    } else {
      objOut.writeObject(value)
    }
  }

  override def fileSegment(): FileSegment = {
    new FileSegment(file, initialPosition, bytesWritten)
  }

  // Only valid if called after close()
  override def timeWriting() = _timeWriting

  override def serializationSamples() = _serializationSamples

  override def itemsSerialized() = totalValuesSerialized

  // Only valid if called after commit()
  override def bytesWritten: Long = {
    lastValidPosition - initialPosition
  }
}
