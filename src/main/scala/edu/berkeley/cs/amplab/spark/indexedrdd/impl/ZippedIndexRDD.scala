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

package edu.berkeley.cs.amplab.spark.indexedrdd.impl

import java.io.{IOException, ObjectOutputStream}
import scala.reflect.ClassTag
import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.util.Utils
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.ReadableIndex
import org.apache.spark.Dependency
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDDPartition
import org.apache.spark.NarrowDependency
import edu.berkeley.cs.amplab.spark.indexedrdd.LazyPartition
import edu.berkeley.cs.amplab.spark.indexedrdd.KeySerializer

/**
 * Based on ZippedPartitionsPartition
 */
private[indexedrdd] class ZippedIndexRDDPartition(
    val index: Int,
    rdd: RDD[_],
    indexedRDD: IndexedRDD[_, _])
  extends Partition {

  var rddPartition = rdd.partitions(index)
  var indexedPartitions = indexedRDD.partitions

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    rddPartition = rdd.partitions(index)
    indexedPartitions = indexedRDD.partitions
    oos.defaultWriteObject()
  }

  override def hashCode = index
}

private[indexedrdd] class NeedsWholeRDD[T]
  (rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  def getParents(partitionId: Int): Seq[Int] = Range(0, rdd.partitions.size) 
}

private[indexedrdd] class ZippedIndexRDD[K: ClassTag, V: ClassTag, V2: ClassTag, W: ClassTag](
    sc: SparkContext,
    var indexedRDD: IndexedRDD[K, V],
    var rdd: RDD[V2],
    var f: (V2, ReadableIndex[K, V]) => W,
    val preservesPartitioning: Boolean = false)
    (implicit kSer: KeySerializer[K])
  extends RDD[W](sc, Seq(new OneToOneDependency(rdd), new NeedsWholeRDD(indexedRDD))) {

  override val partitioner =
    if (preservesPartitioning) rdd.partitioner else None

  override def getPartitions: Array[Partition] =
    Range(0, rdd.partitions.size).
      map(new ZippedIndexRDDPartition(_, rdd, indexedRDD)).
      toArray

  /**
   * Prefer to be where the source RDD partition is, ignoring where the indexed RDD
   * locations are.
   */
  override def getPreferredLocations(s: Partition): Seq[String] = {
    val i = s.asInstanceOf[ZippedIndexRDDPartition].index
    rdd.preferredLocations(rdd.partitions(i))
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd = null
    indexedRDD = null
    f = null
  }

  override def compute(s: Partition, context: TaskContext): Iterator[W] = {
    val partition = s.asInstanceOf[ZippedIndexRDDPartition]
    val lhs = rdd.iterator(partition.rddPartition, context)
    val rhsPartitions = partition.indexedPartitions.map(indexedRDD.indexedPartition(_, context))
    val rhs = LazyPartition(rhsPartitions)
    lhs.map(f(_, rhs))
  }
}
