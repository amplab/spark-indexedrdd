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

  def pairs(n: Int) = {
    create((0 to n).map(x => (x.toLong, x)).iterator)
  }

  test("filter") {
    val vp = pairs(50)
    val vp2 = vp.filter((k, v) => k % 2 == 0)
    assert(vp2.size < vp.size)
  }

  // test("serialization") {
  //   val elems = Set((0L, 1), (1L, 1), (2L, 1))
  //   val vp = create(elems.iterator)
  //   val javaSer = new JavaSerializer(new SparkConf())
  //   val kryoSer = new KryoSerializer(new SparkConf()
  //     .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))

  //   for (ser <- List(javaSer, kryoSer); s = ser.newInstance()) {
  //     val vpSer: IndexedRDDPartition[Long, Int] = s.deserialize(s.serialize(vp))
  //     assert(vpSer.iterator.toSet === elems)
  //   }
  // }
}

class PARTPartitionSuite extends IndexedRDDPartitionSuite {
  override def create[V: ClassTag](iter: Iterator[(Long, V)]) = {
    import IndexedRDD._
    PARTPartition(iter)
  }
}
