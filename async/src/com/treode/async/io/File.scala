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

package com.treode.async.io

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.file.{OpenOption, Path}
import java.nio.file.attribute.FileAttribute
import scala.collection.JavaConversions._

import com.google.common.hash.{HashCode, HashFunction}
import com.treode.async.{Async, Callback, Scheduler}
import com.treode.buffer.PagedBuffer

import Async.{async, guard, when}

/** A file that has useful behavior (flush/fill) and that can be mocked.
  *
  * @define Aligned
  * This will align the end point of the file read to a boundary of <code>2^align</code> bytes.
  * It requires the starting position is already aligned, which means
  * <ul>
  * <li>`input.writePos` is aligned, and</li>
  * <li>`input.readPos + input.readableBytes` is aligned, and</li>
  * <li>`pos + input.readableBytes` is aligned.</li>
  * </ul>
  *
  * @define MatedFrame
  * The mated method `frame` lives in [[com.treode.pickle.Pickler Pickler]].
  *
  * @define Deframe
  * Ensure `input` has at least four readable bytes, reading from the file if necessary.
  * Interpret those as the length of bytes needed.  Read from the file again if necessary,
  * until `input` has at least that many additional readable bytes.
  *
  * @define DeframeChecksum
  * Ensure `input` has at enough bytes for the checksum and integer length, reading from the file
  * if necessary. Read from the file again if necessary, until `input` has enough readable bytes
  * for the frame length. Finally, hash the frame bytes and check it against the checksum.
  */
class File private [io] (file: AsynchronousFileChannel) (implicit scheduler: Scheduler) {

  private def read (dst: ByteBuffer, pos: Long): Async [Int] = {
    require (file != null)
    require (dst != null)
    async (file.read (dst, pos, _, Callback.IntHandler))
  }

  private def write (src: ByteBuffer, pos: Long): Async [Int] =
    async (file.write (src, pos, _, Callback.IntHandler))

  private def whilst (p: => Boolean) (f: => Async [Unit]): Async [Unit] =
    scheduler.whilst (p) (f)

  protected def _fill (input: PagedBuffer, pos: Long, len: Int): Async [Unit] =
    guard {
      input.capacity (input.readPos + len)
      var _pos = pos + input.readableBytes
      var _buf = input.buffer (input.writePos, input.writeableBytes)
      whilst (input.readableBytes < len) {
        for (result <- read (_buf, _pos)) yield {
          if (result < 0)
            throw new Exception ("End of file reached.")
          input.writePos = input.writePos + result
          _pos += result
          if (_buf.remaining == 0 && input.readableBytes < len)
            _buf = input.buffer (input.writePos, input.writeableBytes)
        }}}

  /** Read from the file until `input` has at least `len` readable bytes.  If `input` already has
    * that many readable bytes, this will immediately queue the callback on the scheduler.
    */
  def fill (input: PagedBuffer, pos: Long, len: Int): Async [Unit] =
    when (input.readableBytes < len) (_fill (input, pos, len))

  /** Read from the file until `input` has at least `len` readable bytes, respecting alignment. If
    * `input` already has that many readable bytes, this will immediately queue the callback on the
    * scheduler.
    *
    * $Aligned
    */
  def fill (input: PagedBuffer, pos: Long, len: Int, align: Int): Async [Unit] =
    when (input.readableBytes < len) {
      require (align <= input.pageBits, "Buffer page size must accomodate alignment.")
      val bytes = 1 << align
      val mask = bytes-1
      require ((input.writePos & mask) == 0, "Buffer writePos must be aligned.")
      _fill (input, pos, len)
    }

  /** Read a frame with its own length from the file; return the length.
    *
    * $Deframe
    *
    * $MatedFrame
    */
  def deframe (input: PagedBuffer, pos: Long): Async [Int] = {
    for {
      _ <- fill (input, pos, 4)
      len = input.readInt()
      _ <- fill (input, pos+4, len)
    } yield len
  }

  /** Read a frame with its own length from the file, respecting alignment; return the length.
    *
    * $Deframe
    *
    * $Aligned
    *
    * $MatedFrame
    */
  def deframe (input: PagedBuffer, pos: Long, align: Int): Async [Int] =
    guard {
      for {
        _ <- fill (input, pos, 4, align)
        len = input.readInt()
        _ <- fill (input, pos+4, len, align)
      } yield len
    }

  private def verify (hashf: HashFunction, input: PagedBuffer, len: Int) {
    val bytes = new Array [Byte] (hashf.bits >> 3)
    input.readBytes (bytes, 0, bytes.length)
    val expected = HashCode.fromBytes (bytes)
    val found = input.hash (input.readPos, len, hashf)
    if (found != expected) {
      input.readPos += len
      throw new HashMismatchException
    }}

  /** Read a frame with its own checksum and length from the file; return the length.
    *
    * $DeframeChecksum
    *
    * $MatedFrame
    */
  def deframe (hashf: HashFunction, input: PagedBuffer, pos: Long): Async [Int] = {
    val head = 4 + (hashf.bits >> 3)
    for {
      _ <- fill (input, pos, head)
      len = input.readInt()
      _ <- fill (input, pos+head, len)
    } yield {
      verify (hashf, input, len)
      len
    }}

  /** Read a frame with its own checksum and length from the file, respecting alignment; return the
    * length.
    *
    * $DeframeChecksum
    *
    * $Aligned
    *
    * $MatedFrame
    */
  def deframe (hashf: HashFunction, input: PagedBuffer, pos: Long, align: Int): Async [Int] =
    guard {
      val head = 4 + (hashf.bits >> 3)
      for {
        _ <- fill (input, pos, head, align)
        len = input.readInt()
        _ <- fill (input, pos + head, len, align)
      } yield {
        verify (hashf, input, len)
        len
      }}

  /** Write all readable bytes from `output` to the file at `pos`. */
  def flush (output: PagedBuffer, pos: Long): Async [Unit] =
    when (output.readableBytes > 0) {
      var _pos = pos
      var _buf = output.buffer (output.readPos, output.readableBytes)
      whilst (output.readableBytes > 0) {
        for (result <- write (_buf, _pos)) yield {
          if (result < 0)
            throw new Exception ("File write failed.")
          output.readPos = output.readPos + result
          _pos += result
          if (_buf.remaining == 0 && output.readableBytes > 0)
            _buf = output.buffer (output.readPos, output.readableBytes)
        }}}

  def close(): Unit = file.close()
}

object File {

  def open (path: Path, opts: OpenOption*) (implicit scheduler: Scheduler): File =
    new File (AsynchronousFileChannel.open (path, opts.toSet, scheduler.asExecutorService))
}
