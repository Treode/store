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

package com.treode.async.io.stubs

import java.io.EOFException
import java.util.{Arrays, ArrayDeque}
import scala.util.{Failure, Success}

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.implicits._
import com.treode.async.stubs.CallbackCaptor
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

import Async.async

/** A stub file that keeps its data in a byte array, eliminating the risk that tests fail to clean
  * up test files and leave turds all over your disk.  The stub file will grow as necessary to
  * accommodate `flush`; use the `size` parameter to initialize the underlying byte array to
  * avoid repeatedly growing it and save time on copies.
  *
  * In a multithread context, the `flush` and `fill` methods will do something much like a real
  * file would do: they will depend on the vagaries of the underling scheduler.  The mechanism
  * for capturing calls to `flush` and `fill` should be used only with the single-threaded
  * [[com.treode.async.stubs.StubScheduler StubScheduler]].
  */
class StubFile private (
    var data: Array [Byte],
    mask: Int
) (implicit
    scheduler: Scheduler
) extends File (null) {

  private val stack = new ArrayDeque [Callback [Unit]]

  /** If true, the next call to `flush` or `fill` will be captured and push on a stack. */
  var stop: Boolean = false

  var closed = false

  /** If true, a call to `flush` or `fill` was captured. */
  def hasLast: Boolean = !stack.isEmpty

  /** Pop the most recent call to `flush` or `fill` and return a callback which you can
    * `pass` or `fail`.
    */
  def last: Callback [Unit] = stack.pop()

  private def _stop (f: Callback [Unit] => Any): Async [Unit] = {
    require (!closed, "File has been closed")
    async { cb =>
      if (stop) {
        stack.push {
          case Success (v) =>
            f (cb)
          case Failure (t) =>
            cb.fail (t)
        }
      } else {
        f (cb)
      }}}

  override protected def _fill (input: PagedBuffer, pos: Long, len: Int): Async [Unit] = {
    val _pos = pos.toInt + input.readableBytes
    require (pos + len < Int.MaxValue)
    require (_pos >= 0, "Fill position must be non-negative")
    require ((_pos & mask) == 0, "Fill position must be aligned")
    _stop { cb =>
      try {
        if (len <= input.readableBytes) {
          scheduler.pass (cb, ())
        } else if (data.length < pos) {
          scheduler.fail (cb, new EOFException)
        } else  {
          input.capacity (input.readPos + len)
          val _len = math.min (data.length - _pos, input.writableBytes)
          require ((_len & mask) == 0, "Fill length must be aligned")
          input.writeBytes (data, _pos, _len)
          if (data.length < pos + len) {
            scheduler.fail (cb, new EOFException)
          } else {
            scheduler.pass (cb, ())
          }}
      } catch {
        case t: Throwable => scheduler.fail (cb, t)
      }}}

  override def flush (output: PagedBuffer, pos: Long): Async [Unit] = {
    require (pos >= 0, "Flush position must be non-negative")
    require (pos + output.readableBytes < Int.MaxValue)
    require ((pos & mask) == 0, "Flush position must be aligned.")
    require ((output.readableBytes & mask) == 0, "Flush length must be aligned.")
    _stop { cb =>
      try {
        if (data.length < pos + output.readableBytes)
          data = Arrays.copyOf (data, pos.toInt + output.readableBytes)
        output.readBytes (data, pos.toInt, output.readableBytes)
        scheduler.pass (cb, ())
      } catch {
        case t: Throwable => scheduler.fail (cb, t)
      }}}

  override def close(): Unit =
    closed = true

  override def toString = s"StubFile(size=${data.length})"
}

object StubFile {

  def apply (data: Array [Byte], align: Int) (implicit scheduler: Scheduler): StubFile = {
    require (align >= 0, "Alignment must be non-negative")
    new StubFile (data, (1 << align) - 1)
  }

  def apply (size: Int, align: Int) (implicit scheduler: Scheduler): StubFile =
    apply (new Array [Byte] (size), align)
}
