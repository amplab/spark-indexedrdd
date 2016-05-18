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
import edu.berkeley.cs.amplab.spark.indexedrdd._

import org.apache.spark.SparkConf
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.serializer.KryoSerializer
import org.scalatest.FunSuite

abstract class IndexedRDDPartitionSuite extends FunSuite {

  def create[V: ClassTag](iter: Iterator[(Long, V)]): IndexedRDDPartition[Long, V]

  test("serialization") {
    val elems = Set((0L, 1), (1L, 1), (2L, 1))
    val vp = create(elems.iterator)
    val javaSer = new JavaSerializer(new SparkConf())
    val kryoSer = new KryoSerializer(new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))

    for (ser <- List(javaSer, kryoSer); s = ser.newInstance()) {
      val vpSer: IndexedRDDPartition[Long, Int] = s.deserialize(s.serialize(vp))
      assert(vpSer.iterator.toSet === elems)
    }
  }
  
  test("get") {
    val elems = Set((0L, 1), (1L, 1), (2L, 1))
    val vp = create(elems.iterator)
    assert(vp(0L) == Some(1))
    assert(vp(1L) == Some(1))
    assert(vp(2L) == Some(1))
    assert(vp(3L) == None)
    
    assert(vp.multiget(Array(1L, 2L, 3L)).size == 2)
  }
}

class PARTPartitionSuite extends IndexedRDDPartitionSuite {
  override def create[V: ClassTag](iter: Iterator[(Long, V)]) = {
    import IndexedRDD._
    PARTPartition(iter)
  }
}

class LazyPartitionSuite extends IndexedRDDPartitionSuite {
  override def create[V: ClassTag](iter: Iterator[(Long, V)]) = {
    import IndexedRDD._
    val it = iter.toSeq
    new LazyPartition(
      Seq(PARTPartition(it.iterator), PARTPartition(it.iterator)),
      (id, a, b) => (a ++ b).headOption.getOrElse(null.asInstanceOf[V]))
  }
}
