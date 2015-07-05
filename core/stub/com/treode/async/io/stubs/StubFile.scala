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

import com.treode.async.{Async, Callback, Scheduler}, Async.async
import com.treode.async.implicits._
import com.treode.async.stubs.CallbackCaptor
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

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
class StubFile private (data: StubFile.Data) (implicit scheduler: Scheduler)
extends File (null) {

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
    require (_pos >= 0, "Fill position must be non-negative.")
    require ((_pos & data.mask) == 0, "Fill position must be aligned.")
    _stop { cb =>
      try {
        if (len <= input.readableBytes) {
          scheduler.pass (cb, ())
        } else if (data.length < pos) {
          scheduler.fail (cb, new EOFException)
        } else  {
          input.capacity (input.readPos + len)
          val _len = math.min (data.length - _pos, input.writableBytes)
          require ((_len & data.mask) == 0, "Fill length must be aligned.")
          input.writeBytes (data.bytes, _pos, _len)
          if (data.length < pos + len) {
            scheduler.fail (cb, new EOFException)
          } else {
            scheduler.pass (cb, ())
          }}
      } catch {
        case t: Throwable => scheduler.fail (cb, t)
      }}}

  override def flush (output: PagedBuffer, pos: Long): Async [Unit] = {
    val _end = pos.toInt + output.readableBytes
    require (pos >= 0, "Flush position must be non-negative")
    require ((pos & data.mask) == 0, "Flush position must be aligned.")
    require ((output.readableBytes & data.mask) == 0, "Flush length must be aligned.")
    _stop { cb =>
      try {
        data.grow (_end)
        output.readBytes (data.bytes, pos.toInt, output.readableBytes)
        scheduler.pass (cb, ())
      } catch {
        case t: Throwable => scheduler.fail (cb, t)
      }}}

  override def close(): Unit =
    closed = true

  override def toString = s"StubFile(size=${data.length})"
}

object StubFile {

  class Data private (
    private var _bytes: Array [Byte],
    val mask: Int
  ) {

    def bytes: Array [Byte] =
      _bytes

    def length: Int =
      _bytes.length

    def resize (length: Int): Unit =
      _bytes = Arrays.copyOf (_bytes, length)

    def grow (length: Int): Unit =
      if (_bytes.length < length)
        resize (length)
  }

  object Data {

    def apply (size: Int, align: Int): Data = {
      require (0 <= align && align <= 24, "Alignment must be between 0 and 24 bits inclusive.")
      new Data (new Array [Byte] (size), align)
    }}

  def apply (data: Data) (implicit scheduler: Scheduler): StubFile =
    new StubFile (data)

  def apply (size: Int, align: Int) (implicit scheduler: Scheduler): StubFile =
    apply (Data (size, align))
}
