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
import scala.reflect.ClassTag
import org.apache.spark.HashPartitioner

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

abstract class IndexedRDDSuite extends FunSuite with SharedSparkContext {

  def create[V: ClassTag](elems: RDD[(Long, V)]): IndexedRDD[Long, V]

  def pairs(sc: SparkContext, n: Int, partitions: Int = 5) = {
    create(sc.parallelize((0 to n).map(x => (x.toLong, x)), partitions))
  }

  test("get, multiget") {
    val n = 100
    val ps = pairs(sc, n).cache()
    assert(ps.multiget(Array(-1L, 0L, 1L, 98L)) === LongMap(0L -> 0, 1L -> 1, 98L -> 98))
    assert(ps.get(-1L) === None)
    assert(ps.get(97L) === Some(97))
    val evens = ps.filter(q => ((q._2 % 2) == 0)).cache()
    assert(evens.multiget(Array(-1L, 0L, 1L, 98L)) === LongMap(0L -> 0, 98L -> 98))
    assert(evens.get(97L) === None)
  }

  test("filter") {
    val n = 100
    val ps = pairs(sc, n)
    val evens = ps.filter(q => ((q._2 % 2) == 0))
    assert(evens.count === (0 to n).filter(_ % 2 == 0).size)
  }

  test("mapValues") {
    val n = 100
    val ps = pairs(sc, n)
    val negatives = ps.mapValues(x => -x).cache() // Allow joining b with a derived RDD of b
    assert(negatives.count === n + 1)
  }

  test("diff") {
    val n = 100
    val ps = pairs(sc, n).cache()
    val flipEvens = ps.mapValues(x => if (x % 2 == 0) -x else x).cache()
    // diff should keep only the changed values
    assert(ps.diff(flipEvens).map(_._2).collect().toSet === (2 to n by 2).toSet)
  }

  test("diff with pair RDD") {
    val n = 100
    val ps = pairs(sc, n).cache()
    val flipEvens: RDD[(Long, Int)] =
      sc.parallelize(0L to 100L)
        .map(id => if (id % 2 == 0) (id, -id.toInt) else (id, id.toInt)).cache()
    // diff should keep only the changed values
    assert(ps.diff(flipEvens).map(_._2).collect().toSet === (2 to n by 2).toSet)
  }

  test("diff with non-equal number of partitions") {
    val a = create(sc.parallelize(0 until 24, 3).map(i => (i.toLong, 0)))
    val b = create(sc.parallelize(8 until 16, 2).map(i => (i.toLong, 1)))
    assert(a.partitions.size != b.partitions.size)
    val c = b.diff(a)
    assert(c.map(_._1).collect.toSet === (8 until 16).toSet)
  }

  test("fullOuterJoin") {
    Seq(true, false).foreach { maybeLazy =>
      val n = 200
      val bStart = 50
      val aEnd = 100
      val common = create(sc.parallelize((0 until n).map(x => (x.toLong, x)), 5)).cache()
      val a = common.filter(kv => kv._1 < aEnd).cache()
      val b = common.filter(kv => kv._1 >= bStart).cache()
      val sum = a.fullOuterJoin(b, maybeLazy) { (id, aOpt, bOpt) => aOpt.getOrElse(0) + bOpt.getOrElse(0) }
      val expected = ((0 until bStart).map(x => (x.toLong, x)) ++
        (bStart until aEnd).map(x => (x.toLong, x * 2)) ++
        (aEnd until n).map(x => (x.toLong, x))).toSet
  
      // fullOuterJoin with another IndexedRDD with the same index
      assert(sum.collect.toSet === expected)
  
      // fullOuterJoin with another IndexedRDD with a different index
      val b2 = create(b.map(identity))
      val sum2 = a.fullOuterJoin(b2, maybeLazy) { (id, aOpt, bOpt) => aOpt.getOrElse(0) + bOpt.getOrElse(0) }
      assert(sum2.collect.toSet === expected)
    }
  }

  test("leftJoin") {
    val n = 100
    val ps = pairs(sc, n).cache()
    val evens = ps.filter(q => ((q._2 % 2) == 0)).cache()
    // leftJoin with another IndexedRDD
    assert(ps.leftJoin(evens) { (id, a, bOpt) => a - bOpt.getOrElse(0) }.collect.toSet ===
      (0 to n by 2).map(x => (x.toLong, 0)).toSet ++ (1 to n by 2).map(x => (x.toLong, x)).toSet)
    // leftJoin with an RDD
    val evensRDD = evens.map(identity)
    assert(ps.leftJoin(evensRDD) { (id, a, bOpt) => a - bOpt.getOrElse(0) }.collect.toSet ===
      (0 to n by 2).map(x => (x.toLong, 0)).toSet ++ (1 to n by 2).map(x => (x.toLong, x)).toSet)
  }

  test("leftJoin vertices with non-equal number of partitions") {
    val a = create(sc.parallelize(0 until 100, 2).map(i => (i.toLong, 1)))
    val b = create(
      a.filter(v => v._1 % 2 == 0).partitionBy(new HashPartitioner(3)))
    assert(a.partitions.size != b.partitions.size)
    val c = a.leftJoin(b) { (vid, old, newOpt) =>
      old - newOpt.getOrElse(0)
    }
    assert(c.filter(v => v._2 != 0).map(_._1).collect.toSet == (1 to 99 by 2).toSet)
  }

  test("join") {
    val n = 100
    val ps = pairs(sc, n).cache()
    val evens = ps.filter(q => ((q._2 % 2) == 0)).cache()
    // join with another IndexedRDD
    assert(ps.join(evens) { (id, a, b) => a - b }.collect.toSet ===
      (0 to n by 2).map(x => (x.toLong, 0)).toSet ++ (1 to n by 2).map(x => (x.toLong, x)).toSet)
    // join with an RDD
    val evensRDD = evens.map(identity)
    assert(ps.join(evensRDD) { (id, a, b) => a - b }.collect.toSet ===
      (0 to n by 2).map(x => (x.toLong, 0)).toSet ++ (1 to n by 2).map(x => (x.toLong, x)).toSet)
  }

  test("innerJoin") {
    val n = 100
    val ps = pairs(sc, n).cache()
    val evens = ps.filter(q => ((q._2 % 2) == 0)).cache()
    // innerJoin with another IndexedRDD
    assert(ps.innerJoin(evens) { (id, a, b) => a - b }.collect.toSet ===
      (0 to n by 2).map(x => (x.toLong, 0)).toSet)
    // innerJoin with an RDD
    val evensRDD = evens.map(identity)
    assert(ps.innerJoin(evensRDD) { (id, a, b) => a - b }.collect.toSet ===
     (0 to n by 2).map(x => (x.toLong, 0)).toSet)
  }

  test("innerJoin with non-equal number of partitions") {
    val a = create(sc.parallelize(0 until 100, 2).map(i => (i.toLong, 1)))
    val b = create(
      a.filter(v => v._1 % 2 == 0).partitionBy(new HashPartitioner(3)))
    assert(a.partitions.size != b.partitions.size)
    val c = a.innerJoin(b) { (vid, old, newVal) =>
      old - newVal
    }
    assert(c.filter(v => v._2 == 0).map(_._1).collect.toSet == (0 to 98 by 2).toSet)
  }

  test("aggregateUsingIndex") {
    val n = 100
    val ps = pairs(sc, n)
    val messageTargets = (0 to n) ++ (0 to n by 2)
    val messages = sc.parallelize(messageTargets.map(x => (x.toLong, 1)))
    assert(ps.aggregateUsingIndex[Int](messages, _ + _).collect.toSet ===
      (0 to n).map(x => (x.toLong, if (x % 2 == 0) 2 else 1)).toSet)

    val messagesWithNew = List((0L, 1), (-1L, 1))
    assert(ps.aggregateUsingIndex[Int](sc.parallelize(messagesWithNew), _ + _).collect.toSet ===
      messagesWithNew.toSet)
  }

  test("zipWithOther sparse partitioning") {
    val m = 5L
    val partitions = 20
    val n = 4

    val ps = pairs(sc, n, partitions)
    val myNumbers = sc.parallelize((0L to m), partitions)
    val zipped = ps.zipWithOther(myNumbers, true) {
      case (num, part) => 
        part(num).getOrElse(17)
    }
    assert(zipped.collect.toSeq.sorted === Seq(0, 1, 2, 3, 4, 17))
  }
  
  test("zipWithOther dense partitioning") {
    val m = 5L
    val partitions = 1
    val n = 4

    val ps = pairs(sc, n, partitions)
    val myNumbers = sc.parallelize((0L to m), partitions)
    val zipped = ps.zipWithOther(myNumbers, true) {
    case (num, part) => 
      part(num).getOrElse(17)
    }
    assert(zipped.collect.toSeq.sorted === Seq(0, 1, 2, 3, 4, 17))
  }
}

class UpdatableIndexedRDDSuite extends IndexedRDDSuite {
  override def create[V: ClassTag](elems: RDD[(Long, V)]): IndexedRDD[Long, V] = {
    import IndexedRDD._
    IndexedRDD.updatable(elems)
  }

  test("put, multiput") {
    val n = 100
    val ps = pairs(sc, n).cache()
    assert(ps.multiput[Int](Map(0L -> 1, 1L -> 1), (id, a) => a, SumFunction).collect.toSet ===
      Set(0L -> 1, 1L -> 2) ++ (2 to n).map(x => (x.toLong, x)).toSet)
    assert(ps.multiput[Int](Map(-1L -> -1, 0L -> 1), (id, a) => a, SumFunction).collect.toSet ===
      Set(-1L -> -1, 0L -> 1) ++ (1 to n).map(x => (x.toLong, x)).toSet)
    assert(ps.multiput(Map(-1L -> -1, 0L -> 1, 1L -> 1)).collect.toSet ===
      Set(-1L -> -1, 0L -> 1, 1L -> 1) ++ (2 to n).map(x => (x.toLong, x)).toSet)
    assert(ps.put(-1L, -1).collect.toSet ===
      Set(-1L -> -1) ++ (0 to n).map(x => (x.toLong, x)).toSet)
    assert(ps.put(0L, 1).collect.toSet ===
      Set(0L -> 1) ++ (1 to n).map(x => (x.toLong, x)).toSet)
  }

  test("delete") {
    val n = 100
    val ps = pairs(sc, n).cache()
    assert(ps.delete(Array(0L)).collect.toSet === (1 to n).map(x => (x.toLong, x)).toSet)
    assert(ps.delete(Array(-1L)).collect.toSet === (0 to n).map(x => (x.toLong, x)).toSet)
  }
}

// Declared outside of test suite to avoid closure capture
private object SumFunction extends Function3[Long, Int, Int, Int] with Serializable {
  def apply(id: Long, a: Int, b: Int) = a + b
}
