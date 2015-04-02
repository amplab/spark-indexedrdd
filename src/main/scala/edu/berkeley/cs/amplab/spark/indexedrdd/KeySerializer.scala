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
    ( (b(0) << 56) & (0xFF << 56) |
      (b(1) << 48) & (0xFF << 48) |
      (b(2) << 40) & (0xFF << 40) |
      (b(3) << 32) & (0xFF << 32) |
      (b(4) << 24) & (0xFF << 24) |
      (b(5) << 16) & (0xFF << 16) |
      (b(6) <<  8) & (0xFF <<  8) |
       b(7)        &  0xFF)
}
