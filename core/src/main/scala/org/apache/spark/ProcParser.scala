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

  val LOG_INTERVAL_MILLIS = Duration(500, TimeUnit.MILLISECONDS)

  var previousUtime = -1
  var previousStime = -1
  var previousTotalCpuTime = -1

  def start(sc: SparkContext) {  
    logInfo("Starting ProcParser CPU logging")
    sc.env.actorSystem.scheduler.schedule(
      Duration(0, TimeUnit.MILLISECONDS),
      LOG_INTERVAL_MILLIS) {
      logCpuUsage()
    }
  }

  def logCpuUsage() {
    // Beware that the name returned by getName() is not guaranteed to keep following the pid@X
    // format.
    val myPid = ManagementFactory.getRuntimeMXBean().getName().split("@")(0)
    val file = new File("/proc/%s/stat".format(myPid))
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
}
