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

import scala.util.control.ControlThrowable

import com.google.common.hash.{HashCode, HashFunction, Hashing}
import com.treode.buffer.{ArrayBuffer, Buffer, Input, PagedBuffer, Output, OutputBuffer}

/** How to read and write an object of a particular type.
  *
  * @define Frame
  * Allow four bytes for the length, and write `v` after that. Once `v` has been written, compute
  * its byte length and write that to the first four bytes. This begins writing the length at
  * `writePos` and leaves `writePos` at the end of `v`.
  *
  * @define FrameChecksum
  * Allow some number of bytes for the checksum, depedning on `hashf`, next allow four bytes for
  * the length, and then write `v` after that. Once `v` has been written, compute its checksum and
  * byte length and write that at the beginning. This begins writing the checksum and length at
  * `writePos` and leaves `writePos` at the end of `v`.
  *
  * @define MatedDeframe
  * The mated `deframe` method lives in [[com.treode.async.io.File File]] and
  * [[com.treode.async.io.Socket Socket]].
  */
trait Pickler [A] {

  def p (v: A, ctx: PickleContext)

  def u (ctx: UnpickleContext): A

  def pickle (v: A, b: Output): Unit =
    p (v, new BufferPickleContext (b))

  def unpickle (b: Input): A =
    try {
      u (new BufferUnpickleContext (b))
    } catch {
      case t: ControlThrowable => throw t
      case t: Throwable => throw new UnpicklingException (this, t)
    }

  def byteSize (v: A): Int = {
    val sizer = new SizingPickleContext
    p (v, sizer)
    sizer.result
  }

  def hash (v: A, hashf: HashFunction): HashCode = {
    val hasher = hashf.newHasher
    p (v, new HashingPickleContext (hasher))
    hasher.hash
  }

  def murmur32 (v: A): Int =
    hash (v, Hashing.murmur3_32) .asInt

  def murmur128 (v: A): (Long, Long) = {
    val b = ArrayBuffer (hash (v, Hashing.murmur3_128) .asBytes)
    (b.readLong(), b.readLong())
  }

  def toByteArray (v: A): Array [Byte] = {
    val buf = PagedBuffer (12)
    pickle (v, buf)
    val bytes = new Array [Byte] (buf.readableBytes)
    buf.readBytes (bytes, 0, bytes.length)
    bytes
  }

  def fromByteArray (bytes: Array [Byte]): A = {
    val buf = ArrayBuffer (bytes)
    val v = unpickle (buf)
    require (buf.readableBytes == 0, "Bytes remain after unpickling.")
    v
  }

  /** Write a frame with its own length to the buffer.
    *
    * $Frame
    *
    * $MatedDeframe
    */
  def frame (v: A, buf: OutputBuffer) {
    val start = buf.writePos
    buf.writePos += 4
    pickle (v, buf)
    val end = buf.writePos
    buf.writePos = start
    buf.writeInt (end - start - 4)
    buf.writePos = end
  }

  /** Write a frame with its own checksum and length to the buffer.
    *
    * $FrameChecksum
    *
    * $MatedDeframe
    */
  def frame (hashf: HashFunction, v: A, buf: Buffer) {
    val start = buf.writePos
    val head = 4 + (hashf.bits >> 3)
    buf.writePos += head
    pickle (v, buf)
    val end = buf.writePos
    val hash = buf.hash (start + head, end - start - head, hashf) .asBytes
    buf.writePos = start
    buf.writeInt (end - start - head)
    buf.writeBytes (hash, 0, hash.length)
    buf.writePos = end
  }}
