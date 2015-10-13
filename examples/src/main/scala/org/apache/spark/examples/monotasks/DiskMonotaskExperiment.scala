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

package org.apache.spark.examples.monotasks

import java.io.{File, FileOutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import org.apache.spark.performance_logging.{CpuCounters, CpuUtilization, DiskCounters,
  DiskUtilization}
import org.apache.spark.util.Utils

/**
 * This runs experiments to measure how well the disk is utilized when writing different types of
 * byte buffers to disk.
 */
object DiskMonotaskExperiment {
  def main(args: Array[String]) {
    val baseDirectory = args(0)
    val targetMegabytes = args(1).toInt
    val totalDataSizeBytes = targetMegabytes * 1024 * 1024
    val numTrials = 5

    // Write data from a regular byte buffer.
    printHeader("Non-direct byte buffer")
    val buffer = ByteBuffer.allocate(totalDataSizeBytes)
    // Fill the buffer with a bunch of data.
    (0 until (totalDataSizeBytes / 4)).foreach { i =>
      buffer.putInt(i)
    }
    (0 until 2*numTrials).foreach { i =>
      val startCpuCounters = new CpuCounters()
      val file = new File(baseDirectory, s"ByteBuffer_$i")
      writeData(file, buffer)
      printCpuUtilization(startCpuCounters)
    }

    // Write data from lots of small byte buffers.
    printHeader("1024 small non-direct byte buffers")
    val bufferSize = totalDataSizeBytes / 1024
    val buffers = (1 until 1024).map { i =>
      val buffer = ByteBuffer.allocate(bufferSize)
      (0 until bufferSize / 4).foreach(buffer.putInt(_))
      buffer
    }
    (0 until numTrials).foreach { i =>
      val file = new File(baseDirectory, s"ByteBuffer_smalls_$i")
      val stream = new FileOutputStream(file)
      val channel = stream.getChannel()

      val countersBeforeWrite = new DiskCounters()
      buffers.foreach { buf =>
        buf.rewind()
        channel.write(buf)
      }

      forceDataToDisk(channel, stream, countersBeforeWrite)
    }

    // Write data by first copying it into a direct byte buffer, and then writing it to disk.
    printHeader("Copy to direct byte buffer")
    (0 until numTrials).foreach { i =>
      val file = new File(baseDirectory, s"DirectByteBuffer_$i")
      writeDataWithDirectCopy(file, buffer)
    }


    // Write data from a direct byte buffer.
    printHeader("Direct byte buffer")
    val directBuffer = ByteBuffer.allocateDirect(totalDataSizeBytes)
    // Fill the buffer with a bunch of data.
    (0 until (totalDataSizeBytes / 4)).foreach { i =>
      directBuffer.putInt(i)
    }
    (0 until numTrials).foreach { i =>
      val startCpuCounters = new CpuCounters()
      val file = new File(baseDirectory, s"DirectByteBuffer_$i")
      writeData(file, directBuffer)
      printCpuUtilization(startCpuCounters)
    }

    // Write data from a direct byte buffer without going through the function.
    printHeader(s"Direct byte buffer, no function")
    (0 until numTrials).foreach { i =>
      val file = new File(baseDirectory, s"Direct2ByteBuffer_$i")
      val stream = new FileOutputStream(file)
      val channel = stream.getChannel()
      val dataCopy = directBuffer.duplicate()
      dataCopy.rewind()

      val countersBeforeWrite = new DiskCounters()
      channel.write(dataCopy)

      forceDataToDisk(channel, stream, countersBeforeWrite)
    }

    // Write data from a direct byte buffer without duplicating the buffer.
    printHeader(s"Direct byte buffer, no function or duplication")
    (0 until numTrials).foreach { i =>
      val file = new File(baseDirectory, s"Direct2ByteBuffer_$i")
      val stream = new FileOutputStream(file)
      val channel = stream.getChannel()
      directBuffer.rewind()

      val countersBeforeWrite = new DiskCounters()
      channel.write(directBuffer)

      forceDataToDisk(channel, stream, countersBeforeWrite)
    }

    printHeader("Mapped byte buffer")
    (0 until numTrials).foreach { i =>
      val file = new File(baseDirectory, s"MappedByteBuffer_$i")
      writeDataWithMappedBuffer(file, directBuffer)
    }

    printHeader("CPU Utilization while doing nothing")
    val startCpu = new CpuCounters()
    Thread.sleep(1000)
    printCpuUtilization(startCpu)
  }

  def printHeader(header: String): Unit = {
    System.out.println(s"*********************** $header **************************")
  }

  def writeData(file: File, data: ByteBuffer): Unit = {
    val stream = new FileOutputStream(file)
    val channel = stream.getChannel()
    val dataCopy = data.duplicate()
    dataCopy.rewind()

    val countersBeforeWrite = new DiskCounters()
    val cpuCountersBeforeWrite = new CpuCounters()
    System.out.println(s"Writing ${dataCopy.limit() - dataCopy.position()} bytes to file")
    channel.write(dataCopy)
    printCpuUtilization(cpuCountersBeforeWrite)
    forceDataToDisk(channel, stream, countersBeforeWrite)
  }

  def forceDataToDisk(
      channel: FileChannel,
      stream: FileOutputStream,
      countersBeforeWrite: DiskCounters): Unit = {
    val countersBeforeForce = new DiskCounters()
    System.out.println(
      s"After write but before force: " +
      s"${getUtilizationString(countersBeforeWrite, countersBeforeForce)}")
    channel.force(true)

    val countersBeforeChannelClose = new DiskCounters()
    System.out.println(
      s"After force but before stream close: " +
      s"${getUtilizationString(countersBeforeForce, countersBeforeChannelClose)}")
    channel.close()

    val countersBeforeStreamClose = new DiskCounters()
    System.out.println(
      s"After channel close but before stream close: " +
      s"${getUtilizationString(countersBeforeChannelClose, countersBeforeStreamClose)}")
    stream.close()

    val countersAtEnd = new DiskCounters()
    System.out.println(
      s"After stream close: " +
        s"${getUtilizationString(countersBeforeStreamClose, countersAtEnd)}")
    System.out.println(s"Overall: ${getUtilizationString(countersBeforeWrite, countersAtEnd)}")
  }

  def writeDataWithDirectCopy(file: File, data: ByteBuffer): Unit = {
    val stream = new FileOutputStream(file)
    val channel = stream.getChannel()
    data.rewind()
    val timeBeforeCopy = System.currentTimeMillis
    val dataCopy = ByteBuffer.allocateDirect(data.limit())
    dataCopy.put(data)
    dataCopy.rewind()
    val timeAfterCopy = System.currentTimeMillis()
    System.out.println(
      s"Copying data into direct byte buffer took ${timeAfterCopy - timeBeforeCopy} ms")

    val countersBeforeWrite = new DiskCounters()
    System.out.println(s"Writing ${dataCopy.limit() - dataCopy.position()} bytes to file")
    channel.write(dataCopy)

    forceDataToDisk(channel, stream, countersBeforeWrite)
  }

  def writeDataWithMappedBuffer(file: File, data: ByteBuffer): Unit = {
    val channel = new RandomAccessFile(file, "rw").getChannel()
    data.rewind()
    val countersBeforeCopy = new DiskCounters()
    val startCpuCounters = new CpuCounters()
    val dataCopy = channel.map(FileChannel.MapMode.READ_WRITE, 0, data.limit())
    dataCopy.put(data)
    dataCopy.rewind()

    val countersBeforeWrite = new DiskCounters()
    System.out.println(
      s"After copy: ${getUtilizationString(countersBeforeCopy, countersBeforeWrite)}")
    System.out.println(s"Writing ${dataCopy.limit() - dataCopy.position()} bytes to file")
    channel.write(dataCopy)

    val countersBeforeForce = new DiskCounters()
    System.out.println(
      s"After write but before force: " +
        s"${getUtilizationString(countersBeforeWrite, countersBeforeForce)}")
    channel.force(true)

    val countersBeforeMapForce = new DiskCounters()
    System.out.println(
      s"After channel force but before map force: " +
        s"${getUtilizationString(countersBeforeForce, countersBeforeMapForce)}")
    dataCopy.force()

    val countersBeforeChannelClose = new DiskCounters()
    System.out.println(
      s"After map force: " +
        s"${getUtilizationString(countersBeforeMapForce, countersBeforeChannelClose)}")
    channel.close()

    val countersAtEnd = new DiskCounters()
    System.out.println(
      s"After channel close: " +
        s"${getUtilizationString(countersBeforeChannelClose, countersAtEnd)}")
    System.out.println(s"Overall: ${getUtilizationString(countersBeforeWrite, countersAtEnd)}")
    printCpuUtilization(startCpuCounters)
  }

  def getUtilizationString(startCounters: DiskCounters, endCounters: DiskCounters): String = {
    val utilization = DiskUtilization(startCounters, endCounters)
    val utilizationString = utilization.deviceNameToUtilization.get("xvdb").map { blockUtil =>
      s"${blockUtil.diskUtilization.toString} " +
        s"(${Utils.bytesToString(blockUtil.writeThroughput.toLong)}/s)"
    }.mkString(", ")
    s"Elapsed time: ${utilization.elapsedMillis}; utilization: $utilizationString"
  }

  def printCpuUtilization(startCpuCounters: CpuCounters): Unit = {
    val endCpuCounters = new CpuCounters()
    val utilization = new CpuUtilization(startCpuCounters, endCpuCounters)
    System.out.println(s"CPU utilization: system ${utilization.totalSystemUtilization}, " +
      s"user: ${utilization.totalUserUtilization}")
  }
}
