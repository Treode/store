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

package com.treode.store

import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}
import java.util.Arrays

import com.google.common.primitives.UnsignedBytes
import com.google.common.hash.{HashCode, HashFunction, Hashing}
import com.treode.buffer.ArrayBuffer
import com.treode.pickle.{Pickler, Picklers, PickleContext, UnpickleContext}

/** Convenience wrapper for an array of bytes; sorts lexigraphically. */
class Bytes private (val bytes: Array [Byte]) extends Ordered [Bytes] {

  def length = bytes.length

  def unpickle [A] (p: Pickler [A]): A =
    p.fromByteArray (bytes)

  def hash (hashf: HashFunction): HashCode =
    hashf.hashBytes (bytes)

  def murmur32: Int =
    hash (Hashing.murmur3_32) .asInt

  def murmur128: (Long, Long) = {
    val b = ArrayBuffer.readable (hash (Hashing.murmur3_128) .asBytes)
    (b.readLong(), b.readLong())
  }

  /** Only applies if this was created using `Bytes (String, Charset)`. */
  def string (cs: Charset): String = {
    val b = ByteBuffer.wrap (bytes)
    cs.decode (b) .toString
  }

  /** Only applies if this was created using `Bytes (String)`. */
  def string: String =
    string (StandardCharsets.UTF_8)

  /** Only applies if this was created using `Bytes (Int)`. */
  def int: Int = unpickle (Picklers.fixedInt)

  /** Only applies if this was created using `Bytes (Long)`. */
  def long: Long = unpickle (Picklers.fixedLong)

  def toHexString: String =
    BigInt (1, bytes) .toString (16)

  def compare (that: Bytes): Int =
    UnsignedBytes.lexicographicalComparator.compare (this.bytes, that.bytes)

  override def equals (other: Any) =
    other match {
      case that: Bytes => Arrays.equals (this.bytes, that.bytes)
      case _ => false
    }

  override def hashCode = Arrays.hashCode (bytes)

  override def toString = "Bytes:%08X" format hashCode
}

object Bytes extends Ordering [Bytes] {

  val MinValue = new Bytes (new Array (0))

  val empty = MinValue

  def apply (bytes: Array [Byte]): Bytes =
    new Bytes (bytes)

  def apply [A] (pk: Pickler [A], v: A): Bytes =
    new Bytes (pk.toByteArray (v))

  /** Yield a Bytes object directly from the string. */
  def apply (s: String, cs: Charset = StandardCharsets.UTF_8): Bytes =
    new Bytes (s.getBytes (cs))

  /** Yield a Bytes object that will sort identically to the int. */
  def apply (n: Int): Bytes =
    Bytes (Picklers.fixedInt, n)

  /** Yield a Bytes object that will sort identically to the long. */
  def apply (n: Long): Bytes =
    Bytes (Picklers.fixedLong, n)

  def fromHexString (s: String): Bytes =
    Bytes (BigInt (s, 16) .toByteArray)

  def compare (x: Bytes, y: Bytes): Int =
    x compare y

  val pickler = {
    new Pickler [Bytes] {

      def p (v: Bytes, ctx: PickleContext) {
        ctx.writeVarUInt (v.bytes.length)
        ctx.writeBytes (v.bytes, 0, v.bytes.length)
      }

      def u (ctx: UnpickleContext): Bytes = {
        val length = ctx.readVarUInt()
        val bytes = new Array [Byte] (length)
        ctx.readBytes (bytes, 0, length)
        new Bytes (bytes)
      }}}}
