/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.store.util

import com.treode.pickle.Pickler
import com.treode.store.Bytes

/** A Froster describes how to serialize an object to and from [[com.treode.store.Bytes Bytes]]. It
  * works with [[TableDescriptor]] to make reading and writing the database convenient.
  *
  * The companion object can construct Frosters from Treode picklers, however you can create one
  * using your favorite serialization package (Kryo, Chill, Protobuf, Thrift, Jackson/Smile, etc).
  */
trait Froster [T] {

  def freeze (v: T): Bytes

  def thaw (b: Bytes): T

  def thaw (b: Option [Bytes]): Option [T] =
    b.map (thaw (_))
}

object Froster {

  /** Construct a [[Bytes]] object which sorts identically to the Int. */
  val int: Froster [Int] =
    new Froster [Int] {
      def freeze (v: Int): Bytes = Bytes (v)
      def thaw (v: Bytes): Int = v.int
    }

  /** Construct a [[Bytes]] object which sorts identically to the Long. */
  val long: Froster [Long] =
    new Froster [Long] {
      def freeze (v: Long): Bytes = Bytes (v)
      def thaw (v: Bytes): Long = v.long
    }

  /** Construct a [[Bytes]] object which sorts identically to the String. */
  val string: Froster [String] =
    new Froster [String] {
      def freeze (v: String): Bytes = Bytes (v)
      def thaw (v: Bytes): String = v.string
    }

  def apply [T] (p: Pickler [T]): Froster [T] =
    new Froster [T] {
      def freeze (v: T): Bytes = Bytes (p, v)
      def thaw (v: Bytes): T = v.unpickle (p)
    }}
