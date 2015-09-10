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

import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalacheck.Arbitrary

class KeySerializerSuite extends FunSuite with GeneratorDrivenPropertyChecks with Matchers {

  test("long") {
    val ser = new LongSerializer
    forAll { (a: Long) =>
      ser.fromBytes(ser.toBytes(a)) should be === a
    }
  }

  test("string") {
    val ser = new StringSerializer

    forAll { (a: String) =>
      ser.fromBytes(ser.toBytes(a)) should be === a
    }

    forAll { (a: String, b: String) =>
      whenever (a != b) {
        val aSer = ser.toBytes(a)
        val bSer = ser.toBytes(b)
        assert(!aSer.startsWith(bSer))
        assert(!bSer.startsWith(aSer))
      }
    }
  }

  def tuple2Test[A: Arbitrary, B: Arbitrary](
      aSer: KeySerializer[A], bSer: KeySerializer[B]): Unit = {
    val ser = new Tuple2Serializer[A, B]()(aSer, bSer)

    forAll { (a: A, b: B) =>
      ser.fromBytes(ser.toBytes(Tuple2(a, b))) should be === (a, b)
    }

    forAll { (a: (A, B), b: (A, B)) =>
      whenever (a != b) {
        val aSer = ser.toBytes(a)
        val bSer = ser.toBytes(b)
        assert(!aSer.startsWith(bSer))
        assert(!bSer.startsWith(aSer))
      }
    }
  }

  test("Tuple2") {
    val stringSer = new StringSerializer
    val longSer = new LongSerializer

    tuple2Test[Long, Long](longSer, longSer)
    tuple2Test[String, Long](stringSer, longSer)
    tuple2Test[Long, String](longSer, stringSer)
    tuple2Test[String, String](stringSer, stringSer)
  }
}
