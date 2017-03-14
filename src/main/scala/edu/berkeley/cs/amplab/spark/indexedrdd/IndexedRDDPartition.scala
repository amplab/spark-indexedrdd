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

/**
 * A map of key-value `(K, V)` pairs that enforces key uniqueness and pre-indexes the entries for
 * fast lookups, joins, and optionally updates. To construct an `IndexedRDDPartition`, use one of
 * the constructors in the [[edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDDPartition$
 * IndexedRDDPartition object]].
 *
 * @tparam K the key associated with each entry in the set.
 * @tparam V the value associated with each entry in the set.
 */
private[indexedrdd] abstract class IndexedRDDPartition[K, V] extends Serializable {

  protected implicit def kTag: ClassTag[K]
  protected implicit def vTag: ClassTag[V]

  def size: Long

  /** Return the value for the given key. */
  def apply(k: K): Option[V]

  def isDefined(k: K): Boolean =
    apply(k).isDefined

  def iterator: Iterator[(K, V)]

  /**
   * Gets the values corresponding to the specified keys, if any.
   */
  def multiget(ks: Array[K]): Iterator[(K, V)]

  /**
   * Updates the keys in `kvs` to their corresponding values generated by running `f` on old and new
   * values, if an old value exists, or `z` otherwise. Returns a new IndexedRDDPartition that
   * reflects the modification.
   */
  def multiput[U](
      kvs: Iterator[(K, U)], z: (K, U) => V, f: (K, V, U) => V): IndexedRDDPartition[K, V] =
    throw new UnsupportedOperationException("modifications not supported")

  /** Deletes the specified keys. Returns a new IndexedRDDPartition that reflects the deletions. */
  def delete(ks: Iterator[K]): IndexedRDDPartition[K, V] =
    throw new UnsupportedOperationException("modifications not supported")

  /** Maps each value, supplying the corresponding key and preserving the index. */
  def mapValues[V2: ClassTag](f: (K, V) => V2): IndexedRDDPartition[K, V2]

  /**
   * Restricts the entries to those satisfying the given predicate.
   */
  def filter(pred: (K, V) => Boolean): IndexedRDDPartition[K, V]

  /**
   * Intersects `this` and `other` and keeps only elements with differing values. For these
   * elements, keeps the values from `this`.
   */
  def diff(other: IndexedRDDPartition[K, V]): IndexedRDDPartition[K, V]

  /**
   * Intersects `this` and `other` and keeps only elements with differing values. For these
   * elements, keeps the values from `this`.
   */
  def diff(other: Iterator[(K, V)]): IndexedRDDPartition[K, V]

  /** Joins `this` with `other`, running `f` on the values of all keys in both sets. */
  def fullOuterJoin[V2: ClassTag, W: ClassTag]
      (other: IndexedRDDPartition[K, V2])
      (f: (K, Option[V], Option[V2]) => W): IndexedRDDPartition[K, W]

  /** Joins `this` with `other`, running `f` on the values of all keys in both sets. */
  def fullOuterJoin[V2: ClassTag, W: ClassTag]
      (other: Iterator[(K, V2)])
      (f: (K, Option[V], Option[V2]) => W): IndexedRDDPartition[K, W]

  /**
   * Left outer joins `this` with `other`, running `f` on the values of corresponding keys. Because
   * values in `this` with no corresponding entries in `other` are preserved, `f` cannot change the
   * value type.
   */
  def join[U: ClassTag]
      (other: IndexedRDDPartition[K, U])
      (f: (K, V, U) => V): IndexedRDDPartition[K, V]

  /**
   * Left outer joins `this` with `other`, running `f` on the values of corresponding keys. Because
   * values in `this` with no corresponding entries in `other` are preserved, `f` cannot change the
   * value type.
   */
  def join[U: ClassTag]
      (other: Iterator[(K, U)])
      (f: (K, V, U) => V): IndexedRDDPartition[K, V]

  /** Left outer joins `this` with `other`, running `f` on all values of `this`. */
  def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: IndexedRDDPartition[K, V2])
      (f: (K, V, Option[V2]) => V3): IndexedRDDPartition[K, V3]

  /** Left outer joins `this` with `other`, running `f` on all values of `this`. */
  def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: Iterator[(K, V2)])
      (f: (K, V, Option[V2]) => V3): IndexedRDDPartition[K, V3]

  /** Right outer joins `this` with `other`, running `f` on all values of `other`. */
  def rightJoin[V2: ClassTag, V3: ClassTag]
      (other: IndexedRDDPartition[K, V2])
      (f: (K, Option[V], V2) => V3): IndexedRDDPartition[K, V3] = {
    other.leftJoin(this) { (k, a, bOpt) => f(k, bOpt, a) }
  }

  /** Right outer joins `this` with `other`, running `f` on all values of `other`. */
  def rightJoin[V2: ClassTag, V3: ClassTag]
      (other: Iterator[(K, V2)])
      (f: (K, Option[V], V2) => V3): IndexedRDDPartition[K, V3]

  /** Inner joins `this` with `other`, running `f` on the values of corresponding keys. */
  def innerJoin[U: ClassTag, V2: ClassTag]
      (other: IndexedRDDPartition[K, U])
      (f: (K, V, U) => V2): IndexedRDDPartition[K, V2]

  /** Inner joins `this` with `other`, running `f` on the values of corresponding keys. */
  def innerJoin[U: ClassTag, V2: ClassTag]
      (other: Iterator[(K, U)])
      (f: (K, V, U) => V2): IndexedRDDPartition[K, V2]

  /**
   * Creates a new partition with values from `elems` that may share an index with `this`,
   * merging duplicate keys in `elems` arbitrarily.
   */
  def createUsingIndex[V2: ClassTag](elems: Iterator[(K, V2)]): IndexedRDDPartition[K, V2]

  /** Creates a new partition with values from `elems` that shares an index with `this`. */
  def aggregateUsingIndex[V2: ClassTag](
      elems: Iterator[(K, V2)], reduceFunc: (V2, V2) => V2): IndexedRDDPartition[K, V2]

  /**
   * Optionally rebuilds the indexes of this partition. Depending on the implementation, this may
   * remove tombstoned entries and the resulting partition may support efficient joins with the
   * original one.
   */
  def reindex(): IndexedRDDPartition[K, V]
}
