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

/**
 * Serializer for storing arbitrary key types as byte arrays for PART.
 *
 * If serialized keys may be of variable length, they should be terminated with a unique value,
 * because keys in PART cannot be prefixes of other keys.
 */
trait KeySerializer[K] extends Serializable {
  def toBytes(k: K): Array[Byte]
  def fromBytes(b: Array[Byte]): K
}

class LongSerializer extends KeySerializer[Long] {
  override def toBytes(k: Long) = Array(
    ((k >> 56) & 0xFF).toByte,
    ((k >> 48) & 0xFF).toByte,
    ((k >> 40) & 0xFF).toByte,
    ((k >> 32) & 0xFF).toByte,
    ((k >> 24) & 0xFF).toByte,
    ((k >> 16) & 0xFF).toByte,
    ((k >>  8) & 0xFF).toByte,
    ( k        & 0xFF).toByte)

  override def fromBytes(b: Array[Byte]): Long =
    ( (b(0).toLong << 56) & (0xFFL << 56) |
      (b(1).toLong << 48) & (0xFFL << 48) |
      (b(2).toLong << 40) & (0xFFL << 40) |
      (b(3).toLong << 32) & (0xFFL << 32) |
      (b(4).toLong << 24) & (0xFFL << 24) |
      (b(5).toLong << 16) & (0xFFL << 16) |
      (b(6).toLong <<  8) & (0xFFL <<  8) |
       b(7).toLong        &  0xFFL)
}

class StringSerializer extends KeySerializer[String] {
  override def toBytes(k: String) = {
    val result = new Array[Byte](k.length * 2 + 4)

    var i = 0
    while (i < k.length) {
      result(2 * i)     = ((k(i) >> 8) & 0xFF).toByte
      result(2 * i + 1) = ( k(i)       & 0xFF).toByte
      i += 1
    }

    // Append the string length to ensure no key is a prefix of any other
    result(k.length * 2    ) = ((k.length >> 24) & 0xFF).toByte
    result(k.length * 2 + 1) = ((k.length >> 16) & 0xFF).toByte
    result(k.length * 2 + 2) = ((k.length >>  8) & 0xFF).toByte
    result(k.length * 2 + 3) = ( k.length        & 0xFF).toByte

    result
  }

  override def fromBytes(b: Array[Byte]): String = {
    val result = new Array[Char]((b.length - 4) / 2)

    var i = 0
    while (i < result.length) {
      result(i) =
        ((b(2 * i) << 8) & (0xFF << 8) |
         (b(2 * i + 1)   &  0xFF)).toChar
      i += 1
    }

    new String(result)
  }
}
