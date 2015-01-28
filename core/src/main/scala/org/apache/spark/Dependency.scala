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

package org.apache.spark

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.{HashMap, HashSet}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.monotasks.Monotask
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleReader

/**
 * :: DeveloperApi ::
 * Base class for dependencies.
 */
@DeveloperApi
abstract class Dependency[T] extends Serializable {
  val id = Dependency.newId()

  def rdd: RDD[T]

  /**
   * Returns the monotasks that need to be run to construct the data for this dependency.
   *
   * Current, all implementations assume that the monotasks for a dependency do not depend
   * on one another, and that the only dependency is that the compute monotask for the RDD
   * being computed depends on the monotasks for its dependencies.
   */
  def getMonotasks(
    partition: Partition,
    dependencyIdToPartitions: HashMap[Long, HashSet[Partition]],
    context: TaskContext)
    : Seq[Monotask]
}


/**
 * :: DeveloperApi ::
 * Base class for dependencies where each partition of the child RDD depends on a small number
 * of partitions of the parent RDD. Narrow dependencies allow for pipelined execution.
 */
@DeveloperApi
abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T] {
  /**
   * Get the parent partitions for a child partition.
   * @param partitionId a partition of the child RDD
   * @return the partitions of the parent RDD that the child partition depends upon
   */
  def getParents(partitionId: Int): Seq[Int]

  override def rdd: RDD[T] = _rdd

  override def getMonotasks(
    partition: Partition,
    dependencyIdToPartitions: HashMap[Long, HashSet[Partition]],
    context: TaskContext)
    : Seq[Monotask] = {
    // For each of the parent partitions, get the input monotasks to generate that partition.
    val partitions = dependencyIdToPartitions.get(this.id)
    if (partitions.isEmpty) {
      throw new SparkException("Missing parent partition information for partition " +
        s"${partition.index} of dependency $this (should have been set in DAGScheduler)")
    } else {
      partitions.get.toArray.flatMap { parentPartition =>
        rdd.getInputMonotasks(parentPartition, dependencyIdToPartitions, context)
      }
    }
  }
}


/**
 * :: DeveloperApi ::
 * Represents a dependency on the output of a shuffle stage. Note that in the case of shuffle,
 * the RDD is transient since we don't need it on the executor side.
 *
 * @param _rdd the parent RDD
 * @param partitioner partitioner used to partition the shuffle output
 * @param serializer [[org.apache.spark.serializer.Serializer Serializer]] to use. If set to None,
 *                   the default serializer, as specified by `spark.serializer` config option, will
 *                   be used.
 */
@DeveloperApi
class ShuffleDependency[K, V, C](
    @transient _rdd: RDD[_ <: Product2[K, V]],
    val partitioner: Partitioner,
    val serializer: Option[Serializer] = None,
    val keyOrdering: Option[Ordering[K]] = None,
    val aggregator: Option[Aggregator[K, V, C]] = None,
    val mapSideCombine: Boolean = false)
  extends Dependency[Product2[K, V]] {

  override def rdd = _rdd.asInstanceOf[RDD[Product2[K, V]]]

  val shuffleId: Int = _rdd.context.newShuffleId()

  _rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))

  /** Helps with reading the shuffle data associated with this dependency. Set by getMonotasks(). */
  var shuffleReader: Option[ShuffleReader[K, V, C]] = None

  override def getMonotasks(
    partition: Partition,
    dependencyIdToPartitions: HashMap[Long, HashSet[Partition]],
    context: TaskContext)
    : Seq[Monotask] = {
    // TODO: should the shuffle reader code just be part of the dependency?
    shuffleReader = Some(new ShuffleReader(this, partition.index, context))
    shuffleReader.get.getReadMonotasks()
  }
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between partitions of the parent and child RDDs.
 */
@DeveloperApi
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int) = List(partitionId)
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between ranges of partitions in the parent and child RDDs.
 * @param rdd the parent RDD
 * @param inStart the start of the range in the parent RDD
 * @param outStart the start of the range in the child RDD
 * @param length the length of the range
 */
@DeveloperApi
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {

  override def getParents(partitionId: Int) = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
}

private[spark] object Dependency {
  val nextId = new AtomicLong(0)

  def newId(): Long = nextId.getAndIncrement()
}
