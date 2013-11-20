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
  var previousNetworkLogTime = -1L
  var previousReceivedBytes = 0
  var previousReceivedPackets = 0
  var previousTransmittedBytes = 0
  var previousTransmittedPackets = 0

  val LOG_INTERVAL_MILLIS = Duration(500, TimeUnit.MILLISECONDS)

  // Beware that the name returned by getName() is not guaranteed to keep following the pid@X
  // format.
  val PID = ManagementFactory.getRuntimeMXBean().getName().split("@")(0)

  def start(sc: SparkContext) {  
    logInfo("Starting ProcParser CPU logging")
    sc.env.actorSystem.scheduler.schedule(
      Duration(0, TimeUnit.MILLISECONDS),
      LOG_INTERVAL_MILLIS) {
      logCpuUsage()
      logNetworkUsage()
    }
  }

  def logCpuUsage() {
    val file = new File("/proc/%s/stat".format(PID))
    val fileReader = new FileReader(file)
    val bufferedReader = new BufferedReader(fileReader)
    val line = bufferedReader.readLine()
    val values = line.split(" ")
    val currentUtime = values(UTIME_INDEX).toInt
    val currentStime = values(STIME_INDEX).toInt

    // Read the total number of jiffies that have elapsed since the last time we read the CPU info.
    val cpuTotalsBufferedReader = new BufferedReader(new FileReader(new File(CPU_TOTALS_FILENAME)))
    var currentTotalCpuTime = -1
    while (currentTotalCpuTime == -1) {
      val line = cpuTotalsBufferedReader.readLine()
      if (line == null) {
        logError("Couldn't find line begining with 'cpu' in file %s".format(CPU_TOTALS_FILENAME))
        return
      }
      if (line.startsWith("cpu")) {
        currentTotalCpuTime = line.substring(5, line.length ).split(" ").map(_.toInt).sum
      }
    }

    if (previousUtime != -1) {
      val elapsedCpuTime = currentTotalCpuTime - previousTotalCpuTime
      val userUtil = (currentUtime - previousUtime) * 1.0 / elapsedCpuTime
      val sysUtil = (currentStime - previousStime) * 1.0 / elapsedCpuTime
      val totalUtil = userUtil + sysUtil
      logInfo("CPU utilization: user: %s, sys: %s, total: %s".format(userUtil, sysUtil, totalUtil))
    }

    previousUtime = currentUtime
    previousStime = currentStime
    previousTotalCpuTime = currentTotalCpuTime
  }

  def logNetworkUsage() {
    val currentTime = System.currentTimeMillis
    // TODO: change logCpuUsage() to use scala-y files
    var totalTransmittedBytes = 0
    var totalTransmittedPackets = 0
    var totalReceivedBytes = 0
    var totalReceivedPackets = 0
    Source.fromFile("/proc/%s/net/dev".format(PID)).getLines().foreach { line =>
      logInfo("read network line: %s".format(line))
      if (line.contains(":") && !line.contains("lo")) {
        val counts = line.split(":")(1).split(" ").filter(_.length > 0).map(_.toInt)
        totalTransmittedBytes += counts(TRANSMITTED_BYTES_INDEX)
        totalTransmittedPackets += counts(TRANSMITTED_PACKETS_INDEX)
        totalReceivedBytes += counts(RECEIVED_BYTES_INDEX)
        totalReceivedPackets += counts(RECEIVED_PACKETS_INDEX)
      }
    }
    logInfo("Current totals: trans: %s bytes, %s packets, recv %s bytes %s packets".format(
      totalTransmittedBytes, totalTransmittedPackets, totalReceivedBytes, totalReceivedPackets))
    if (previousNetworkLogTime != -1) {
      val timeDelta = previousNetworkLogTime - currentTime
      val transmittedBytesRate = ((totalTransmittedBytes - previousTransmittedBytes) / timeDelta)
      val transmittedPacketsRate = ((totalTransmittedPackets - previousTransmittedPackets) /
        timeDelta)
      val receivedBytesRate = ((totalReceivedBytes - previousReceivedBytes) / timeDelta)
      val receivedPacketsRate = ((totalTransmittedPackets - previousTransmittedPackets) / timeDelta)
      logInfo("%s: trans rates: %s bytes, %s packets; Recv rates: %s bytes, %s packets".format(
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
    Source.fromFile("/proc/%s/io".format(PID)).getLines().foreach { line => System.out.println(line)}
  }

  def logMemoryUsage() {
  }
}
