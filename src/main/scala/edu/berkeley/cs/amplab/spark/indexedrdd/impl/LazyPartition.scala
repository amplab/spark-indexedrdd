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

import scala.reflect.ClassTag
import scala.collection.Traversable
import org.apache.spark.Logging
import edu.berkeley.cs.amplab.spark.indexedrdd.impl.PARTPartition

/**
 * A wrapper around several IndexedRDDPartition that avoids rebuilding
 * the index for the combined partitions. Instead, each operation probes
 * the nested partitions and merges the results.
 * 
 * @param reducer a reduction function; at least one of the given V
 *  Options will not be None.
 */

private[indexedrdd] class LazyPartition[K, V]
    (val partitions: Seq[IndexedRDDPartition[K, V]],
     val reducer: (K, Option[V], Option[V]) => V)
    (override implicit val kTag: ClassTag[K],
     override implicit val vTag: ClassTag[V])
  extends IndexedRDDPartition[K, V] {

  @transient private lazy val cached: IndexedRDDPartition[K, V] =
    partitions.reduce((a, b) => a.fullOuterJoin(b)(reducer))
  
  /**
   * We need to index the combined partitions in case any have duplicates
   * we need to reduce.
   */
  def size: Long = cached.size

  /** Return the value for the given key. */
  def apply(k: K): Option[V] =
    partitions.
      map(_(k)).
      reduce((a, b) => (a, b) match {
        case (None, None) => None
        case _ => Option(reducer(k, a, b))
      })

  override def isDefined(k: K): Boolean =
    partitions.find(_.isDefined(k)).isDefined

  def iterator: Iterator[(K, V)] =
    cached.iterator

  /**
   * Query each partition independently, then merge the results by key. This
   * could be more efficient if multiget returned ordered results!
   */
  def multiget(ks: Array[K]): Iterator[(K, V)] =
    partitions.
      flatMap(_.multiget(ks)).
      groupBy(_._1).
      map {
        case (k, vs) =>
          val v = vs.map(_._2).reduce((v1, v2) => reducer(k, Some(v1), Some(v2)))
          (k, v)
      }.
      iterator

  /**
   * We have to re-index as we don't know how to reduce the mapped values.
   */
  def mapValues[V2: ClassTag](f: (K, V) => V2): IndexedRDDPartition[K, V2] =
    cached.mapValues(f)

  def filter(pred: (K, V) => Boolean): IndexedRDDPartition[K, V] =
    new LazyPartition(partitions.map(_.filter(pred)), reducer)

  def diff(other: IndexedRDDPartition[K, V]): IndexedRDDPartition[K, V] =
    cached.diff(other)

  def diff(other: Iterator[(K, V)]): IndexedRDDPartition[K, V] =
    cached.diff(other)

  def fullOuterJoin[V2: ClassTag, W: ClassTag]
      (other: IndexedRDDPartition[K, V2])
      (f: (K, Option[V], Option[V2]) => W): IndexedRDDPartition[K, W] =
    cached.fullOuterJoin(other)(f)

  def fullOuterJoin[V2: ClassTag, W: ClassTag]
      (other: Iterator[(K, V2)])
      (f: (K, Option[V], Option[V2]) => W): IndexedRDDPartition[K, W] =
    cached.fullOuterJoin(other)(f)

  def join[U: ClassTag]
      (other: IndexedRDDPartition[K, U])
      (f: (K, V, U) => V): IndexedRDDPartition[K, V] =
    cached.join(other)(f)

  def join[U: ClassTag]
      (other: Iterator[(K, U)])
      (f: (K, V, U) => V): IndexedRDDPartition[K, V] =
    cached.join(other)(f)

  def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: IndexedRDDPartition[K, V2])
      (f: (K, V, Option[V2]) => V3): IndexedRDDPartition[K, V3] =
    cached.leftJoin(other)(f)

  def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: Iterator[(K, V2)])
      (f: (K, V, Option[V2]) => V3): IndexedRDDPartition[K, V3] =
    cached.leftJoin(other)(f)

  def innerJoin[U: ClassTag, V2: ClassTag]
      (other: IndexedRDDPartition[K, U])
      (f: (K, V, U) => V2): IndexedRDDPartition[K, V2] =
    cached.innerJoin(other)(f)

  def innerJoin[U: ClassTag, V2: ClassTag]
      (other: Iterator[(K, U)])
      (f: (K, V, U) => V2): IndexedRDDPartition[K, V2] =
    cached.innerJoin(other)(f)

  def createUsingIndex[V2: ClassTag](elems: Iterator[(K, V2)]): IndexedRDDPartition[K, V2] =
    cached.createUsingIndex(elems)

  def aggregateUsingIndex[V2: ClassTag](
      elems: Iterator[(K, V2)], reduceFunc: (V2, V2) => V2): IndexedRDDPartition[K, V2] =
    cached.aggregateUsingIndex(elems, reduceFunc)

  /**
   * Forces the partitions to re-index, and rebuilds the combined index.
   */
  def reindex(): IndexedRDDPartition[K, V] =
    partitions.map(_.reindex).reduce((a, b) => a.fullOuterJoin(b)(reducer))

  /**
   * Re-index before serialization.
   */
  private def writeReplace(): Object = cached
}

private[indexedrdd] object LazyPartition {
  /**
   * For use when we don't expect collisions here (as we can guarantee the key universes
   * in each partition are disjoint), so doesn't lose data:
   */
  def apply[K: ClassTag, V: ClassTag]
      (bits: Seq[IndexedRDDPartition[K, V]])
      (implicit kSer: KeySerializer[K]) = bits.size match {
    case 0 => PARTPartition(Seq.empty[(K, V)].iterator)
    case 1 => bits.head
    case _ => new LazyPartition(
      bits.flatMap(_ match {
        case it: LazyPartition[K, V] => it.partitions
        case it => Seq(it)
      }),
      (key: K, lhs: Option[V], rhs: Option[V]) => lhs.getOrElse(rhs.get))
  }
}
