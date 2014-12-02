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

package org.apache.spark.monotasks.compute

import scala.reflect.ClassTag

import org.apache.spark.{Logging, Partition, TaskContext}
import org.apache.spark.rdd.RDD

/**
 * A task that sends back a result (based on the input RDD) to the driver application.
 */
private[spark] class ResultMonotask[T, U: ClassTag](
    context: TaskContext,
    rdd: RDD[T],
    split: Partition,
    val func: (TaskContext, Iterator[T]) => U)
  extends ExecutionMonotask[T, U](context, rdd, split) with Logging {

  override def getResult(): U = {
    func(context, rdd.iterator(split, context))
  }
}
