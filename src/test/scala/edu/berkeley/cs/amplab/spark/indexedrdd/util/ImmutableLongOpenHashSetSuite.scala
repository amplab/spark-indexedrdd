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

package edu.berkeley.cs.amplab.spark.indexedrdd.util

import org.scalatest.FunSuite
import org.scalatest.Matchers

class ImmutableLongOpenHashSetSuite extends FunSuite with Matchers {

  test("primitive long") {
    var set = ImmutableLongOpenHashSet.empty
    assert(set.size === 0)
    assert(!set.contains(10L))
    assert(!set.contains(50L))
    assert(!set.contains(999L))
    assert(!set.contains(10000L))

    set = set.add(10L)
    assert(set.size === 1)
    assert(set.contains(10L))
    assert(!set.contains(50L))
    assert(!set.contains(999L))
    assert(!set.contains(10000L))

    set = set.add(50L)
    assert(set.size === 2)
    assert(set.contains(10L))
    assert(set.contains(50L))
    assert(!set.contains(999L))
    assert(!set.contains(10000L))

    set = set.add(999L)
    assert(set.size === 3)
    assert(set.contains(10L))
    assert(set.contains(50L))
    assert(set.contains(999L))
    assert(!set.contains(10000L))

    set = set.add(50L)
    assert(set.size === 3)
    assert(set.contains(10L))
    assert(set.contains(50L))
    assert(set.contains(999L))
    assert(!set.contains(10000L))
  }

  test("primitive set growth") {
    var set = ImmutableLongOpenHashSet.empty
    for (i <- 1 to 1000) {
      set = set.add(i.toLong)
    }
    assert(set.size === 1000)
    assert(set.capacity > 1000)
    for (i <- 1 to 100) {
      set = set.add(i.toLong)
    }
    assert(set.size === 1000)
    assert(set.capacity > 1000)
  }
}
