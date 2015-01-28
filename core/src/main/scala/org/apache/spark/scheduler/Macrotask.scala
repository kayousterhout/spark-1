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

package org.apache.spark.scheduler

import java.io.{DataInputStream, DataOutputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer

import scala.collection.mutable.{HashMap, HashSet}

import org.apache.spark._
import org.apache.spark.monotasks.Monotask
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.ByteBufferInputStream

/**
 * A unit of execution. Spark has two kinds of Macrotasks:
 * - [[org.apache.spark.scheduler.ShuffleMapMacrotask]]
 * - [[org.apache.spark.scheduler.ResultMacrotask]]
 *
 * Macrotasks are used to ship information from the scheduler to the executor, and are responsible
 * for constructing the monotasks needed to complete the Macrotask.
 *
 * A spark job consists of one or more stages. The very last stage in a job consists of multiple
 * ResultMacrotasks, while earlier stages consist of ShuffleMapMacrotasks. A ResultMacroTask
 * executes the task and sends the task output back to the driver application. A
 * ShuffleMapMacroTask executes the task and divides the task output to multiple buckets (based on
 * the task's partitioner).
 */
private[spark] abstract class Macrotask[T](val stageId: Int, val partition: Partition,
    rdd: RDD[_])
  extends Serializable with Logging {
  def preferredLocations: Seq[TaskLocation] = Nil

  /**
   * Maps NarrowDependencies to the partitions of the parent RDD for that dependency. This should
   * include all NarrowDependencies that will be traversed as part of completing this macrotask and
   * all associated partitions for each NarrowDependency. This is necessary for
   * computing the monotasks for this macrotask: in order to compute the monotasks, we need to walk
   * through all of the rdds and associated partitions that will be computed as part of this task.
   * The NarrowDependency includes a pointer to the associated parent RDD, but the pointer to the
   * parent Partition is stored as part of the child Partition in formats specific to the Partition
   * subclasses, hence the need for this additional mapping.
   *
   * Indexed on the Dependency id rather than directly on the dependency because of the way
   * serialization happens. This object is serialized separately from the RDD object, so if
   * Dependency objects were used as keys here, they will end up being different than the
   * Dependency objects in the RDD class, so this mapping would no longer be valid once
   * deserialized.
   */
  val dependencyIdToPartitions = new HashMap[Long, HashSet[Partition]]()
  constructDependencyToPartitionsMap(rdd, partition.index)

  private def constructDependencyToPartitionsMap(rdd: RDD[_], partitionIndex: Int) {
    rdd.dependencies.foreach {
      case narrowDependency: NarrowDependency[_] =>
        val parentPartitions = dependencyIdToPartitions.getOrElseUpdate(
          narrowDependency.id, new HashSet[Partition])

        narrowDependency.getParents(partitionIndex).foreach { parentPartitionIndex =>
          val parentPartition = narrowDependency.rdd.partitions(parentPartitionIndex)
          if (parentPartitions.add(parentPartition)) {
            constructDependencyToPartitionsMap(narrowDependency.rdd, parentPartitionIndex)
          }
        }
    }
  }

  // Map output tracker epoch. Will be set by TaskScheduler.
  var epoch: Long = -1

  /**
   * Returns the monotasks that need to be run in order to execute this macrotask. This is run
   * within a compute monotask, so should not use network or disk.
   */
  def getMonotasks(context: TaskContext): Seq[Monotask]
}

/**
 * Handles transmission of tasks and their dependencies, because this can be slightly tricky. We
 * need to send the list of JARs and files added to the SparkContext with each task to ensure that
 * worker nodes find out about it, but we can't make it part of the Task because the user's code in
 * the task might depend on one of the JARs. Thus we serialize each task as multiple objects, by
 * first writing out its dependencies.
 */
private[spark] object Macrotask {
  /**
   * Serialize a task and the current app dependencies (files and JARs added to the SparkContext)
   */
  def serializeWithDependencies(
    task: Macrotask[_],
    currentFiles: HashMap[String, Long],
    currentJars: HashMap[String, Long],
    serializer: SerializerInstance)
    : ByteBuffer = {

    val out = new ByteArrayOutputStream(4096)
    val dataOut = new DataOutputStream(out)

    // Write currentFiles
    dataOut.writeInt(currentFiles.size)
    for ((name, timestamp) <- currentFiles) {
      dataOut.writeUTF(name)
      dataOut.writeLong(timestamp)
    }

    // Write currentJars
    dataOut.writeInt(currentJars.size)
    for ((name, timestamp) <- currentJars) {
      dataOut.writeUTF(name)
      dataOut.writeLong(timestamp)
    }

    // Write the task itself and finish
    dataOut.flush()
    val taskBytes = serializer.serialize(task).array()
    out.write(taskBytes)
    ByteBuffer.wrap(out.toByteArray)
  }

  /**
   * Deserialize the list of dependencies in a task serialized with serializeWithDependencies,
   * and return the task itself as a serialized ByteBuffer. The caller can then update its
   * ClassLoaders and deserialize the task.
   *
   * @return (taskFiles, taskJars, taskBytes)
   */
  def deserializeWithDependencies(serializedTask: ByteBuffer)
  : (HashMap[String, Long], HashMap[String, Long], ByteBuffer) = {

    val in = new ByteBufferInputStream(serializedTask)
    val dataIn = new DataInputStream(in)

    // Read task's files
    val taskFiles = new HashMap[String, Long]()
    val numFiles = dataIn.readInt()
    for (i <- 0 until numFiles) {
      taskFiles(dataIn.readUTF()) = dataIn.readLong()
    }

    // Read task's JARs
    val taskJars = new HashMap[String, Long]()
    val numJars = dataIn.readInt()
    for (i <- 0 until numJars) {
      taskJars(dataIn.readUTF()) = dataIn.readLong()
    }

    // Create a sub-buffer for the rest of the data, which is the serialized Task object
    val subBuffer = serializedTask.slice()  // ByteBufferInputStream will have read just up to task
    (taskFiles, taskJars, subBuffer)
  }
}