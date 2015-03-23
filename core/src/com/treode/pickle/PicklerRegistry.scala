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

package com.treode.pickle

import java.util.concurrent.ConcurrentHashMap
import scala.reflect.ClassTag

import com.treode.buffer.{ArrayBuffer, InputBuffer, OutputBuffer, PagedBuffer}

import PicklerRegistry._

/** How to read objects of various types using a long to identify the type.
  *
  * When we know the type of a value that we want to deserialize, we can reach directly for the
  * correct pickler. However when the value may be any type, we need a means to identify which
  * pickler to use. The PicklerRegistry uses long (8 byte) tags to identify types.
  *
  * When we know the type of the value we are deserializing, we know its interface and can use the
  * value immediately. However when the value may be any type, we need to hide it behind a common
  * interface, so that we can treat the different types identically. Each of the types that can be
  * deserialized must have a converter that performs this encapsulation. See the `register` method.
  *
  * Serialize values of various types by writing a long tag, then the value itself; see the
  * `frame` methods in the companion object. Deseralize values by reading the long tag, then
  * looking up the pickler to deserialize the value itself; see the `unpickle` methods.
  *
  * @tparam T The type that encapsulates deserialized values.
  * @param default The function to compute
  */
class PicklerRegistry [T] private (default: Long => T) {

  private val openers = new ConcurrentHashMap [Long, Opener [T]]

  /** Register a pickler to deserialize a tagged value.
    * @param id The tag to identify the pickler.
    * @param p The pickler to deserialize values identify by `id`.
    * @param read The function to encapsulate the deserialized value.
    */
  def register [P] (id: Long, p: Pickler [P]) (read: P => T) {
    val u1 = Opener (p, id, read)
    val u0 = openers.putIfAbsent (id, u1)
    require (u0 == null, f"$id%X already registered")
  }

  /** Find an unsed tag, and register a pickler on it.
    * @param p The pickler to deserialize values identified by the new tag.
    * @return The new tag.
    */
  def open [P] (p: Pickler [P]) (random: (Long, P => T)): Long = {
    var r = random
    var u1 = Opener (p, r._1, r._2)
    var u0 = openers.putIfAbsent (r._1, u1)
    while (u0 != null) {
      r = random
      u1 = Opener (p, r._1, r._2)
      u0 = openers.putIfAbsent (r._1, u1)
    }
    r._1
  }

  /** Unregister a pickler.
    * @param id The tag to identify the pickler.
    */
  def unregister (id: Long): Unit =
    openers.remove (id)

  private def _unpickle (id: Long, buf: InputBuffer): T = {
    val u = openers.get (id)
    if (u == null)
      default (id)
    else
      u.read (buf)
  }

  /** Lookup the pickler and deserialize the value.
    * @param id The tag to identify the pickler.
    * @param buf The buffer to deserialize from.
    * @param len The length that the pickler should consume from the buffer.
    * @throws FrameBoundsException If the pickler consumes the incorrect length from the buffer.
    */
  def unpickle (id: Long, buf: InputBuffer, len: Int): T =
    try {
      val end = buf.readPos + len
      val u = openers.get (id)
      if (u == null) {
        buf.readPos = end
        return default (id)
      }
      val v = u.read (buf)
      if (buf.readPos != end)
        throw new FrameBoundsException
      v
    } catch {
      case t: Throwable =>
        buf.readPos = len
        throw t
    }

  /** Read the tag from the buffer, then lookup the pickler and deserialize the value.
    * @param buf The buffer to deserialize from.
    * @param len The length that the pickler should consume from the buffer.
    * @throws FrameBoundsException If the pickler consumes the incorrect length from the buffer.
    */
  def unpickle (buf: InputBuffer, len: Int): T =
    try {
      val end = buf.readPos + len
      val v = _unpickle (buf.readLong(), buf)
      if (buf.readPos != end)
        throw new FrameBoundsException
      v
    } catch {
      case t: Throwable =>
        buf.readPos = len
        throw t
    }

  /** Read the tag from the buffer, then lookup the pickler and deserialize the value, then repeat.
    * @param buf The buffer to deserialize from.
    * @param len The length that the pickler should consume from the buffer.
    * @param n The number of values to read.
    * @throws FrameBoundsException If the picklers consume the incorrect length from the buffer.
    */
  def unpickle (buf: InputBuffer, len: Int, n: Int): Seq [T] =
    try {
      val end = buf.readPos + len
      val vs = Seq.fill (n) (_unpickle (buf.readLong(), buf))
      if (buf.readPos != end)
        throw new FrameBoundsException
      vs
    } catch {
      case t: Throwable =>
        buf.readPos = len
        throw t
    }

  /** Lookup the pickler and deserialize the value.
    * @param id The tag to identify the pickler.
    * @param bytes The bytes to deserialize from.
    * @throws FrameBoundsException If the pickler does not consume exactly the given bytes.
    */
  def unpickle (id: Long, bytes: Array [Byte]): T =
    unpickle (id, ArrayBuffer.readable (bytes), bytes.length)

  /** Use the given pickler to serialize the value, and then lookup the pickler to deserialize the
    * value.
    *
    * @param p The pickler to serialize the value.
    * @param id The tag to identify the pickler for deserialization.
    * @param v The value to convert.
    * @throws FrameBoundsException If the deserializing pickler does not consume exactly the
    * bytes produced by the serializing pickler.
    */
  def loopback [P] (p: Pickler [P], id: Long, v: P): T = {
    val buf = PagedBuffer (12)
    p.pickle (v, buf)
    unpickle (id, buf, buf.writePos)
  }}

/**
  * @define Frame
  * Allow four bytes for the length, and write `v` after that. Once `v` has been written, compute
  * its byte length and write that to the first four bytes. The length is that of the value only;
  * it does not include the length of itself. This starts writing the length at `writePos`, and it
  * leaves `writePos` at the end of `v`.
  *
  * @define MatedDeframe
  * The mated `deframe` method lives in [[com.treode.async.io.File File]] and
  * [[com.treode.async.io.Socket Socket]].
  */
object PicklerRegistry {

  def apply [T] (default: Long => T): PicklerRegistry [T] =
    new PicklerRegistry (default)

  def apply [T] (name: String): PicklerRegistry [T] =
    new PicklerRegistry (id => throw new InvalidTagException (name, id))

  /** Write a frame with its own length to the buffer.
    *
    * $Frame
    *
    * $MatedDeframe
    */
  def frame (v: PickledValue, buf: OutputBuffer) {
    val start = buf.writePos
    buf.writePos += 4
    buf.writeLong (v.id)
    v.pickle (buf)
    val end = buf.writePos
    buf.writePos = start
    buf.writeInt (end - start - 4)
    buf.writePos = end
  }

  /** Write a frame with its own length to the buffer.
    *
    * $Frame
    *
    * $MatedDeframe
    */
  def frame (vs: Seq [PickledValue], buf: OutputBuffer) {
    val start = buf.writePos
    buf.writePos += 4
    buf.writeInt (vs.size)
    for (v <- vs) {
      buf.writeLong (v.id)
      v.pickle (buf)
    }
    val end = buf.writePos
    buf.writePos = start
    buf.writeInt (end - start - 4)
    buf.writePos = end
  }

  private trait Opener [T] {

    def read (buf: InputBuffer): T
  }

  private object Opener {

    def apply [ID, P, T] (p: Pickler [P], id: Long, reader: P => T): Opener [T] =
      new Opener [T] {

        def read (buf: InputBuffer): T =
          reader (p.unpickle (buf))

        override def toString = f"Opener($id%X)"
    }}}
