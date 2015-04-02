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

package edu.berkeley.cs.amplab.spark.indexedrdd

import scala.collection.immutable.LongMap
import scala.language.higherKinds
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import edu.berkeley.cs.amplab.spark.indexedrdd.impl._

/**
 * An RDD of key-value `(K, V)` pairs that pre-indexes the entries for fast lookups, joins, and
 * optionally updates. To construct an `IndexedRDD`, use one of the constructors in the
 * [[edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD$ IndexedRDD object]].
 *
 * @tparam K the key associated with each entry in the set.
 * @tparam V the value associated with each entry in the set.
 */
class IndexedRDD[K: ClassTag, V: ClassTag](
    /** The underlying representation of the IndexedRDD as an RDD of partitions. */
    protected val partitionsRDD: RDD[IndexedRDDPartition[K, V]])
  extends RDD[(K, V)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  require(partitionsRDD.partitioner.isDefined)

  override val partitioner = partitionsRDD.partitioner

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] =
    partitionsRDD.preferredLocations(s)

  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    this
  }

  override def count(): Long = {
    partitionsRDD.map(_.size).reduce(_ + _)
  }

  /** Provides the `RDD[(K, V)]` equivalent output. */
  override def compute(part: Partition, context: TaskContext): Iterator[(K, V)] = {
    firstParent[IndexedRDDPartition[K, V]].iterator(part, context).next.iterator
  }

  /** Gets the value corresponding to the specified key, if any. */
  def get(k: K): Option[V] = multiget(Array(k)).get(k)

  /** Gets the values corresponding to the specified keys, if any. */
  def multiget(ks: Array[K]): Map[K, V] = {
    val ksByPartition = ks.groupBy(k => partitioner.get.getPartition(k))
    val partitions = ksByPartition.keys.toSeq
    // TODO: avoid sending all keys to all partitions by creating and zipping an RDD of keys
    val results: Array[Array[(K, V)]] = context.runJob(partitionsRDD,
      (context: TaskContext, partIter: Iterator[IndexedRDDPartition[K, V]]) => {
        if (partIter.hasNext && ksByPartition.contains(context.partitionId)) {
          val part = partIter.next()
          val ksForPartition = ksByPartition.get(context.partitionId).get
          part.multiget(ksForPartition.iterator).toArray
        } else {
          Array.empty
        }
      }, partitions, allowLocal = true)
    results.flatten.toMap
  }

  /**
   * Unconditionally updates the specified key to have the specified value. Returns a new IndexedRDD
   * that reflects the modification.
   */
  def put(k: K, v: V): IndexedRDD[K, V] = multiput(Map(k -> v))

  /**
   * Unconditionally updates the keys in `kvs` to their corresponding values. Returns a new
   * IndexedRDD that reflects the modification.
   */
  def multiput(kvs: Map[K, V]): IndexedRDD[K, V] = multiput[V](kvs, identity, (id, a, b) => a)

  /**
   * Updates the keys in `kvs` to their corresponding values, running `merge` on old and new values
   * if necessary. Returns a new IndexedRDD that reflects the modification.
   */
  def multiput[U: ClassTag](kvs: Map[K, U], z: U => V, f: (K, U, V) => V): IndexedRDD[K, V] = {
    val updates = context.parallelize(kvs.toSeq).partitionBy(partitioner.get)
    zipPartitionsWithOther(updates)(new MultiputZipper(z, f))
  }

  /** Deletes the specified keys. Returns a new IndexedRDD that reflects the deletions. */
  def delete(ks: Array[K]): IndexedRDD[K, V] = {
    val deletions = context.parallelize(ks.map(k => (k, ()))).partitionBy(partitioner.get)
    zipPartitionsWithOther(deletions)(new DeleteZipper)
  }

  /** Applies a function to each partition of this IndexedRDD. */
  protected def mapIndexedRDDPartitions[K2: ClassTag, V2: ClassTag](
      f: IndexedRDDPartition[K, V] => IndexedRDDPartition[K2, V2]): IndexedRDD[K2, V2] = {
    val newPartitionsRDD = partitionsRDD.mapPartitions(_.map(f), preservesPartitioning = true)
    new IndexedRDD(newPartitionsRDD)
  }

  /** Applies a function to corresponding partitions of `this` and another IndexedRDD. */
  protected def zipIndexedRDDPartitions[V2: ClassTag, V3: ClassTag](other: IndexedRDD[K, V2])
      (f: ZipPartitionsFunction[V2, V3]): IndexedRDD[K, V3] = {
    assert(partitioner == other.partitioner)
    val newPartitionsRDD = partitionsRDD.zipPartitions(other.partitionsRDD, true)(f)
    new IndexedRDD(newPartitionsRDD)
  }

  /** Applies a function to corresponding partitions of `this` and a pair RDD. */
  protected def zipPartitionsWithOther[V2: ClassTag, V3: ClassTag](other: RDD[(K, V2)])
      (f: OtherZipPartitionsFunction[V2, V3]): IndexedRDD[K, V3] = {
    val partitioned = other.partitionBy(partitioner.get)
    val newPartitionsRDD = partitionsRDD.zipPartitions(partitioned, true)(f)
    new IndexedRDD(newPartitionsRDD)
  }

  /**
   * Restricts the entries to those satisfying the given predicate. This operation preserves the
   * index for efficient joins with the original IndexedRDD and is implemented using soft deletions.
   *
   * @param pred the user defined predicate, which takes a tuple to conform to the `RDD[(K, V)]`
   * interface
   */
  override def filter(pred: Tuple2[K, V] => Boolean): IndexedRDD[K, V] =
    this.mapIndexedRDDPartitions(_.filter(Function.untupled(pred)))

  /** Maps each value, preserving the index. */
  def mapValues[V2: ClassTag](f: V => V2): IndexedRDD[K, V2] =
    this.mapIndexedRDDPartitions(_.mapValues((vid, attr) => f(attr)))

  /** Maps each value, supplying the corresponding key and preserving the index. */
  def mapValues[V2: ClassTag](f: (K, V) => V2): IndexedRDD[K, V2] =
    this.mapIndexedRDDPartitions(_.mapValues(f))

  /**
   * Joins `this` with `other`, running `f` on the values of all keys in both sets. Note that for
   * efficiency `other` must be an IndexedRDD, not just a pair RDD. Use [[aggregateUsingIndex]] to
   * construct an IndexedRDD co-partitioned with `this`.
   */
  def fullOuterJoin[V2: ClassTag, W: ClassTag]
      (other: IndexedRDD[K, V2])
      (f: (K, Option[V], Option[V2]) => W): IndexedRDD[K, W] = {
    require(partitioner == other.partitioner)
    this.zipIndexedRDDPartitions(other)(new FullOuterJoinZipper(f))
  }

  /** Inner joins `this` with `other`, running `f` on the values of corresponding keys. */
  def innerJoin[V2: ClassTag, V3: ClassTag](other: RDD[(K, V2)])
      (f: (K, V, V2) => V3): IndexedRDD[K, V3] = other match {
    case other: IndexedRDD[K, V2] if partitioner == other.partitioner =>
      this.zipIndexedRDDPartitions(other)(new InnerJoinZipper(f))
    case _ =>
      this.zipPartitionsWithOther(other)(new OtherInnerJoinZipper(f))
  }

  // The following functions could have been anonymous, but we name them to work around a Scala
  // compiler bug related to specialization.

  protected type ZipPartitionsFunction[V2, V3] =
    Function2[Iterator[IndexedRDDPartition[K, V]], Iterator[IndexedRDDPartition[K, V2]],
      Iterator[IndexedRDDPartition[K, V3]]]

  protected type OtherZipPartitionsFunction[V2, V3] =
    Function2[Iterator[IndexedRDDPartition[K, V]], Iterator[(K, V2)],
      Iterator[IndexedRDDPartition[K, V3]]]

  private class MultiputZipper[U](z: U => V, f: (K, U, V) => V)
      extends OtherZipPartitionsFunction[U, V] with Serializable {
    def apply(thisIter: Iterator[IndexedRDDPartition[K, V]], otherIter: Iterator[(K, U)])
      : Iterator[IndexedRDDPartition[K, V]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.multiput(otherIter, z, f))
    }
  }

  private class DeleteZipper extends OtherZipPartitionsFunction[Unit, V] with Serializable {
    def apply(thisIter: Iterator[IndexedRDDPartition[K, V]], otherIter: Iterator[(K, Unit)])
      : Iterator[IndexedRDDPartition[K, V]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.delete(otherIter.map(_._1)))
    }
  }

  private class FullOuterJoinZipper[V2: ClassTag, W: ClassTag](f: (K, Option[V], Option[V2]) => W)
      extends ZipPartitionsFunction[V2, W] with Serializable {
    def apply(
        thisIter: Iterator[IndexedRDDPartition[K, V]], otherIter: Iterator[IndexedRDDPartition[K, V2]])
        : Iterator[IndexedRDDPartition[K, W]] = {
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.fullOuterJoin(otherPart)(f))
    }
  }

  private class InnerJoinZipper[V2: ClassTag, V3: ClassTag](f: (K, V, V2) => V3)
      extends ZipPartitionsFunction[V2, V3] with Serializable {
    def apply(
        thisIter: Iterator[IndexedRDDPartition[K, V]], otherIter: Iterator[IndexedRDDPartition[K, V2]])
      : Iterator[IndexedRDDPartition[K, V3]] = {
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.innerJoin(otherPart)(f))
    }
  }

  private class OtherInnerJoinZipper[V2: ClassTag, V3: ClassTag](f: (K, V, V2) => V3)
      extends OtherZipPartitionsFunction[V2, V3] with Serializable {
    def apply(thisIter: Iterator[IndexedRDDPartition[K, V]], otherIter: Iterator[(K, V2)])
      : Iterator[IndexedRDDPartition[K, V3]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.innerJoin(otherIter)(f))
    }
  }
}

object IndexedRDD {
  /** Constructs an IndexedRDD from an RDD of pairs. */
  def updatable[K: ClassTag, U: ClassTag, V: ClassTag]
      (elems: RDD[(K, U)], z: U => V, f: (V, U) => V, partitioner: Partitioner)
      (implicit kSer: KeySerializer[K]): IndexedRDD[K, V] = {
    val partitions = elems.partitionBy(partitioner).mapPartitions[IndexedRDDPartition[K, V]](
      iter => Iterator(PARTPartition(iter, z, f)),
      preservesPartitioning = true)
    new IndexedRDD(partitions)
  }

  implicit val longSer = new LongSerializer
}
