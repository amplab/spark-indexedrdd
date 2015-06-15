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

import scala.reflect.ClassTag
import scala.collection.JavaConversions._

import org.apache.spark.Logging

import edu.berkeley.cs.amplab.spark.indexedrdd._

import com.ankurdave.part.ArtTree

private[indexedrdd] class PARTPartition[K, V]
    (protected val map: ArtTree)
    (override implicit val kTag: ClassTag[K],
     override implicit val vTag: ClassTag[V],
     implicit val kSer: KeySerializer[K])
  extends IndexedRDDPartition[K, V] with Logging {

  protected def withMap[V2: ClassTag]
      (map: ArtTree): PARTPartition[K, V2] = {
    new PARTPartition(map)
  }

  override def size: Long = map.size()

  override def apply(k: K): V = map.search(kSer.toBytes(k)).asInstanceOf[V]

  override def isDefined(k: K): Boolean = map.search(kSer.toBytes(k)) != null

  override def iterator: Iterator[(K, V)] =
    map.iterator.map(kv => (kSer.fromBytes(kv._1), kv._2.asInstanceOf[V]))

  private def rawIterator: Iterator[(Array[Byte], V)] =
    map.iterator.map(kv => (kv._1, kv._2.asInstanceOf[V]))

  override def multiget(ks: Iterator[K]): Iterator[(K, V)] =
    ks.flatMap { k => Option(this(k)).map(v => (k, v)) }

  override def multiput[U](
      kvs: Iterator[(K, U)], z: (K, U) => V, f: (K, V, U) => V): IndexedRDDPartition[K, V] = {
    val newMap = map.snapshot()
    for (ku <- kvs) {
      val kBytes = kSer.toBytes(ku._1)
      val oldV = newMap.search(kBytes).asInstanceOf[V]
      val newV = if (oldV == null) z(ku._1, ku._2) else f(ku._1, oldV, ku._2)
      newMap.insert(kBytes, newV)
    }
    this.withMap[V](newMap)
  }

  override def delete(ks: Iterator[K]): IndexedRDDPartition[K, V] = {
    val newMap = map.snapshot()
    for (k <- ks) {
      newMap.delete(kSer.toBytes(k))
    }
    this.withMap[V](newMap)
  }

  override def mapValues[V2: ClassTag](f: (K, V) => V2): IndexedRDDPartition[K, V2] = {
    val newMap = new ArtTree
    for (kv <- rawIterator) newMap.insert(kv._1, f(kSer.fromBytes(kv._1), kv._2))
    this.withMap[V2](newMap)
  }

  override def filter(pred: (K, V) => Boolean): IndexedRDDPartition[K, V] = {
    val newMap = new ArtTree
    for (kv <- rawIterator if pred(kSer.fromBytes(kv._1), kv._2)) {
      newMap.insert(kv._1, kv._2)
    }
    this.withMap[V](newMap)
  }

  override def diff(other: IndexedRDDPartition[K, V]): IndexedRDDPartition[K, V] = other match {
    case other: PARTPartition[K, V] =>
      val newMap = new ArtTree
      for (kv <- rawIterator) {
        val otherV = other.map.search(kv._1).asInstanceOf[V]
        if (otherV != null && otherV != kv._2) {
          newMap.insert(kv._1, kv._2)
        }
      }
      this.withMap[V](newMap)

    case _ =>
      diff(other.iterator)
  }

  override def diff(other: Iterator[(K, V)]): IndexedRDDPartition[K, V] =
    diff(PARTPartition(other))

  override def fullOuterJoin[V2: ClassTag, W: ClassTag]
      (other: IndexedRDDPartition[K, V2])
      (f: (K, Option[V], Option[V2]) => W): IndexedRDDPartition[K, W] = other match {
    case other: PARTPartition[K, V2] =>
      val newMap = new ArtTree
      // Scan `this` and probe `other`, adding all elements in `this`
      for (kv <- rawIterator) {
        val newV = f(
          kSer.fromBytes(kv._1),
          Some(kv._2),
          Option(other.map.search(kv._1).asInstanceOf[V2]))
        newMap.insert(kv._1, newV)
      }
      // Scan `other` and probe `this`, adding only the elements present in `other` but not `this`
      for (kv <- other.rawIterator) {
        if (this.map.search(kv._1) == null) {
          val newV = f(
            kSer.fromBytes(kv._1),
            None,
            Some(kv._2))
          newMap.insert(kv._1, newV)
        }
      }
      this.withMap[W](newMap)

    case _ =>
      fullOuterJoin(other.iterator)(f)
  }

  override def fullOuterJoin[V2: ClassTag, W: ClassTag]
      (other: Iterator[(K, V2)])
      (f: (K, Option[V], Option[V2]) => W): IndexedRDDPartition[K, W] =
    fullOuterJoin(PARTPartition(other))(f)

  override def join[U: ClassTag]
      (other: IndexedRDDPartition[K, U])
      (f: (K, V, U) => V): IndexedRDDPartition[K, V] = join(other.iterator)(f)

  override def join[U: ClassTag]
      (other: Iterator[(K, U)])
      (f: (K, V, U) => V): IndexedRDDPartition[K, V] = {
    val newMap = map.snapshot()
    for (ku <- other) {
      val kBytes = kSer.toBytes(ku._1)
      val oldV = newMap.search(kBytes).asInstanceOf[V]
      if (oldV != null) {
        val newV = f(ku._1, oldV, ku._2)
        newMap.insert(kBytes, newV)
      }
    }
    this.withMap[V](newMap)
  }

  override def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: IndexedRDDPartition[K, V2])
      (f: (K, V, Option[V2]) => V3): IndexedRDDPartition[K, V3] = other match {
    case other: PARTPartition[K, V2] =>
      // Scan `this` and probe `other`
      val newMap = new ArtTree
      for (kv <- rawIterator) {
        val newV = f(kSer.fromBytes(kv._1), kv._2, Option(other.map.search(kv._1).asInstanceOf[V2]))
        newMap.insert(kv._1, newV)
      }
      this.withMap[V3](newMap)

    case _ =>
      leftJoin(other.iterator)(f)
  }

  override def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: Iterator[(K, V2)])
      (f: (K, V, Option[V2]) => V3): IndexedRDDPartition[K, V3] =
    leftJoin(PARTPartition(other))(f)

  override def innerJoin[U: ClassTag, V2: ClassTag]
      (other: IndexedRDDPartition[K, U])
      (f: (K, V, U) => V2): IndexedRDDPartition[K, V2] = other match {
    case other: PARTPartition[K, U] =>
      // Scan `this` and probe `other`
      val newMap = new ArtTree
      for (kv <- rawIterator) {
        val otherV = other.map.search(kv._1).asInstanceOf[U]
        if (otherV != null) newMap.insert(kv._1, f(kSer.fromBytes(kv._1), kv._2, otherV))
      }
      this.withMap[V2](newMap)

    case _ =>
      innerJoin(other.iterator)(f)
  }

  override def innerJoin[U: ClassTag, V2: ClassTag]
      (other: Iterator[(K, U)])
      (f: (K, V, U) => V2): IndexedRDDPartition[K, V2] =
    innerJoin(PARTPartition(other))(f)

  override def createUsingIndex[V2: ClassTag](elems: Iterator[(K, V2)]): IndexedRDDPartition[K, V2] =
    PARTPartition(elems)

  override def aggregateUsingIndex[V2: ClassTag](
      elems: Iterator[(K, V2)], reduceFunc: (V2, V2) => V2): IndexedRDDPartition[K, V2] =
    PARTPartition[K, V2, V2](elems, (id, a) => a, (id, a, b) => reduceFunc(a, b))

  override def reindex(): IndexedRDDPartition[K, V] = this
}

private[indexedrdd] object PARTPartition {
  def apply[K: ClassTag, V: ClassTag]
      (iter: Iterator[(K, V)])(implicit kSer: KeySerializer[K]) =
    apply[K, V, V](iter, (id, a) => a, (id, a, b) => b)

  def apply[K: ClassTag, U: ClassTag, V: ClassTag]
      (iter: Iterator[(K, U)], z: (K, U) => V, f: (K, V, U) => V)
      (implicit kSer: KeySerializer[K]): PARTPartition[K, V] = {
    val map = new ArtTree
    iter.foreach { ku =>
      val kBytes = kSer.toBytes(ku._1)
      val oldV = map.search(kBytes).asInstanceOf[V]
      val newV = if (oldV == null) z(ku._1, ku._2) else f(ku._1, oldV, ku._2)
      map.insert(kBytes, newV)
    }
    new PARTPartition(map)
  }
}
