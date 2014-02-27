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

/**
 * Stores information about the fetch of a single block.
 *
 * Deserialization is not included here because the deserialization happens after the fetch completes,
 * as part of the application processing the data.
 *
 * @param bytes Amount of data fetched.
 * @param fetchStartTime Time (in milliseconds) when the fetch started.
 * @param fetchEndTime Time (in milliseconds) when the fetch ended.
 * @param processingEndTime Time (in milliseconds) when the fetch was done being processed.
 * @param diskReadTimeNanos Nanoseconds taken to read the data from disk on the remote machine.
 */
case class FetchInfo(bytes: Long, fetchStartTime: Long, fetchEndTime: Long, processingEndTime: Long,
    diskReadTimeNanos: Long)
