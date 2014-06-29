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

package org.apache.spark.performance_logging

object CpuUtilization {
  // This is the correct value for most linux systems, and for the default Spark AMI.
  val JIFFIES_PER_SECOND = 100
}

class CpuUtilization(startCounters: CpuCounters, endCounters: CpuCounters) {
  val elapsedMillis = endCounters.timeMillis - startCounters.timeMillis
  val elapsedJiffies = CpuUtilization.JIFFIES_PER_SECOND * (elapsedMillis * 1.0 / 1000)

  val processUserUtilization =
    ((endCounters.processUserJiffies - startCounters.processUserJiffies).toFloat / elapsedJiffies)
  val processSystemUtilization =
    ((endCounters.processSystemJiffies - startCounters.processSystemJiffies).toFloat /
      elapsedJiffies)
  val totalUserUtilization =
    ((endCounters.totalUserJiffies - startCounters.totalUserJiffies).toFloat / elapsedJiffies)
  val totalSystemUtilization =
    ((endCounters.totalSystemJiffies - startCounters.totalSystemJiffies).toFloat / elapsedJiffies)

  def this(startCounters: CpuCounters) = this(startCounters, new CpuCounters())
}