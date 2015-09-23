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

package org.apache.spark.util

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.Writable

import org.apache.spark.Logging

class LongArrayWritable extends Writable with Logging {
  private var data: Option[Array[Long]] = None

  def this(data: Array[Long]) = {
    this()
    this.data = Some(data)
  }

  def get(): Array[Long] = {
    data.get
  }

  override def write(out: DataOutput): Unit = {
    val length = data.map(_.size).getOrElse(0)

    out.writeInt(length)

    data.map(_.foreach(out.writeLong(_)))
  }

  override def readFields(in: DataInput): Unit = {
    val length = in.readInt()
    data = Some(Array.fill(length)(in.readLong()))
  }
}
