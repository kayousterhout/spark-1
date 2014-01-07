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

package org.apache.spark

import akka.util.Duration

import java.io._
import java.lang.management.ManagementFactory
import java.util.concurrent.TimeUnit

import scala.io.Source

/**
 * Extracts information from the process info pseudo-filesystem at /proc.
 *
 * Based on https://code.google.com/p/djmonitor/source/browse/src/parser/ProcParser.java
 */
class ProcParser extends Logging {

  val CPU_TOTALS_FILENAME = "/proc/stat"

  // 0-based index in /proc/pid/stat file of the user CPU time. Not necessarily portable.
  val UTIME_INDEX = 13
  val STIME_INDEX = 14
  // This is the correct value for most linux systems, and for the default Spark AMI.
  val JIFFIES_PER_SECOND = 100

  var previousCpuLogTime = 0L
  var previousUtime = -1
  var previousStime = -1
  var previousTotalCpuTime = -1

  // 0-based index within the list of numbers in /proc/pid/net/dev file of the received and
  // transmitted bytes/packets. Not necessarily portable.
  val RECEIVED_BYTES_INDEX = 0
  val RECEIVED_PACKETS_INDEX = 1
  val TRANSMITTED_BYTES_INDEX = 8
  val TRANSMITTED_PACKETS_INDEX = 9

  // We need to store the bytes/packets recorded at the last time so that we can compute the delta.
  var previousNetworkLogTime = 0L
  var previousReceivedBytes = 0L
  var previousReceivedPackets = 0L
  var previousTransmittedBytes = 0L
  var previousTransmittedPackets = 0L

  // Chars read is the sm of bytes pased to read() and pread() -- so it includes tty IO,
  // for example, and overestimtes the number of bytes written to physical disk. The bytes
  // are an attempt to count the number of bytes fetched from the storage layer.
  // (See http://stackoverflow.com/questions/3633286/understanding-the-counters-in-proc-pid-io)
  val CHARS_READ_PREFIX = "rchar: "
  val CHARS_WRITTEN_PREFIX = "wchar: "
  val BYTES_READ_PREFIX = "read_bytes: "
  val BYTES_WRITTEN_PREFIX = "write_bytes: "

  var previousDiskLogTime = 0L
  var previousCharsRead = 0L
  var previousCharsWritten = 0L
  var previousBytesRead = 0L
  var previousBytesWritten = 0L

  val LOG_INTERVAL_MILLIS = Duration(50, TimeUnit.MILLISECONDS)

  // Beware that the name returned by getName() is not guaranteed to keep following the pid@X
  // format.
  val PID = ManagementFactory.getRuntimeMXBean().getName().split("@")(0)

  def start(env: SparkEnv) {  
    logInfo("Starting ProcParser CPU logging")
    env.actorSystem.scheduler.schedule(
      Duration(0, TimeUnit.MILLISECONDS),
      LOG_INTERVAL_MILLIS) {
      logCpuUsage()
      logNetworkUsage()
      logDiskUsage()
    }
  }
  
  /**
   * Logs the CPU utilization during the period of time since the last log message.
   */
  def logCpuUsage() {
    val file = new File("/proc/%s/stat".format(PID))
    val fileReader = new FileReader(file)
    val bufferedReader = new BufferedReader(fileReader)
    val line = bufferedReader.readLine()
    val values = line.split(" ")
    val currentUtime = values(UTIME_INDEX).toInt
    val currentStime = values(STIME_INDEX).toInt

    // Read the total number of jiffies that have elapsed since the last time we read the CPU info.
    var currentTotalCpuTime = -1
    Source.fromFile(CPU_TOTALS_FILENAME).getLines().foreach { line =>
      if (line.startsWith("cpu")) {
        currentTotalCpuTime = line.substring(5, line.length ).split(" ").map(_.toInt).sum
      }
    }
    if (currentTotalCpuTime == -1) {
      logError("Couldn't find line beginning with 'cpu' in file %s".format(CPU_TOTALS_FILENAME))
      return
    }

    val currentTime = System.currentTimeMillis
    val elapsedCpuTime = currentTotalCpuTime - previousTotalCpuTime
    if (previousUtime != -1 && elapsedCpuTime > 0) {
      val userUtil = (currentUtime - previousUtime) * 1.0 / elapsedCpuTime
      val sysUtil = (currentStime - previousStime) * 1.0 / elapsedCpuTime
      val totalUtil = userUtil + sysUtil
      logInfo("%s CPU utilization (relative metric): user: %s sys: %s total: %s"
        .format(currentTime, userUtil, sysUtil, totalUtil))
    }

    // Log alternate CPU utilization metric: the CPU counters are measured in jiffies,
    // so log the elapsed jiffies / jiffies per sec / seconds since last measurement.
    if (previousCpuLogTime > 0) {
      val elapsedTimeMillis = currentTime - previousCpuLogTime
      val elapsedJiffies = JIFFIES_PER_SECOND * (elapsedTimeMillis / 1000)
      val userUtil = (currentUtime - previousUtime) / elapsedJiffies
      val sysUtil = (currentUtime - previousUtime) / elapsedJiffies
      logInfo("%s CPU utilization (jiffie-based): user: %s sys: %s total: %s"
        .format(currentTime, userUtil, sysUtil, userUtil + sysUtil))
    }

    // Log absolute counters to make it easier to compute utilization over the entire experiment.
    logInfo("%s CPU counters: user: %s sys: %s".format(currentTime, currentUtime, currentStime)) 

    previousUtime = currentUtime
    previousStime = currentStime
    previousTotalCpuTime = currentTotalCpuTime
    previousCpuLogTime = currentTime
  }

  def logNetworkUsage() {
    val currentTime = System.currentTimeMillis
    var totalTransmittedBytes = 0L
    var totalTransmittedPackets = 0L
    var totalReceivedBytes = 0L
    var totalReceivedPackets = 0L
    Source.fromFile("/proc/%s/net/dev".format(PID)).getLines().foreach { line =>
      if (line.contains(":") && !line.contains("lo")) {
        val counts = line.split(":")(1).split(" ").filter(_.length > 0).map(_.toLong)
        totalTransmittedBytes += counts(TRANSMITTED_BYTES_INDEX)
        totalTransmittedPackets += counts(TRANSMITTED_PACKETS_INDEX)
        totalReceivedBytes += counts(RECEIVED_BYTES_INDEX)
        totalReceivedPackets += counts(RECEIVED_PACKETS_INDEX)
      }
    }
    logInfo("%s Current totals: trans: %s bytes, %s packets, recv %s bytes %s packets".format(
      currentTime,
      totalTransmittedBytes,
      totalTransmittedPackets,
      totalReceivedBytes,
      totalReceivedPackets))
    if (previousNetworkLogTime > 0) {
      val timeDeltaSeconds = (currentTime - previousNetworkLogTime) / 1000.0
      val transmittedBytesRate = ((totalTransmittedBytes - previousTransmittedBytes) * 1.0 /
        timeDeltaSeconds)
      val transmittedPacketsRate = ((totalTransmittedPackets - previousTransmittedPackets) * 1.0 /
        timeDeltaSeconds)
      val receivedBytesRate = ((totalReceivedBytes - previousReceivedBytes) * 1.0 / timeDeltaSeconds)
      val receivedPacketsRate = ((totalTransmittedPackets - previousTransmittedPackets) * 1.0 /
        timeDeltaSeconds)
      logInfo("%s trans rates: %s bytes, %s packets; Recv rates: %s bytes, %s packets".format(
        currentTime,
        transmittedBytesRate,
        transmittedPacketsRate,
        receivedBytesRate,
        receivedPacketsRate))
    }
    previousReceivedBytes = totalReceivedBytes
    previousReceivedPackets = totalReceivedPackets
    previousTransmittedBytes = totalTransmittedBytes
    previousTransmittedPackets = totalTransmittedPackets
    previousNetworkLogTime = currentTime
  }

  def logDiskUsage() {
    val currentTime = System.currentTimeMillis

    var totalCharsRead = 0L
    var totalCharsWritten = 0L
    var totalBytesRead = 0L
    var totalBytesWritten = 0L
    Source.fromFile("/proc/%s/io".format(PID)).getLines().foreach { line =>
      if (line.startsWith(CHARS_READ_PREFIX)) {
        totalCharsRead = line.substring(CHARS_READ_PREFIX.length).toLong
      } else if (line.startsWith(CHARS_WRITTEN_PREFIX)) {
        totalCharsWritten = line.substring(CHARS_WRITTEN_PREFIX.length).toLong
      } else if (line.startsWith(BYTES_READ_PREFIX)) {
        totalBytesRead = line.substring(BYTES_READ_PREFIX.length).toLong
      } else if (line.startsWith(BYTES_WRITTEN_PREFIX)) {
        totalBytesWritten = line.substring(BYTES_WRITTEN_PREFIX.length).toLong
      }
    }

    val timeDeltaSeconds = (currentTime - previousDiskLogTime) / 1000.0
    if (previousDiskLogTime > 0 && timeDeltaSeconds > 0) {
      val charsReadRate = (totalCharsRead - previousCharsRead) * 1.0 / timeDeltaSeconds
      val charsWrittenRate = (totalCharsWritten - previousCharsWritten) * 1.0 / timeDeltaSeconds
      val bytesReadRate = (totalBytesRead - previousBytesRead) * 1.0 / timeDeltaSeconds
      val bytesWrittenRate = (totalBytesWritten - previousBytesWritten) * 1.0 / timeDeltaSeconds
      logInfo("%s rchar rate: %s wchar rate: %s rbytes rate: %s wbytes rate: %s".format(
        currentTime, charsReadRate, charsWrittenRate, bytesReadRate, bytesWrittenRate))
    }
    previousDiskLogTime = currentTime
    previousCharsRead = totalCharsRead
    previousCharsWritten = totalCharsWritten
    previousBytesRead = totalBytesRead
    previousBytesWritten = totalBytesWritten
  }

  def logMemoryUsage() {
  }
}
