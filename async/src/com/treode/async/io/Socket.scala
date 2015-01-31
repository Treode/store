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
import java.nio.channels.{AsynchronousChannelGroup, AsynchronousSocketChannel}
import java.net.SocketAddress
import java.util.concurrent.TimeUnit
import scala.util.{Failure, Success}

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.implicits._
import com.treode.buffer.PagedBuffer

import Async.async
import TimeUnit.MILLISECONDS

/** A socket that has useful behavior (flush/fill) and that can be mocked. */
class Socket (socket: AsynchronousSocketChannel) (implicit scheduler: Scheduler) {
  import scheduler.whilst

  def localAddress: SocketAddress =
    socket.getLocalAddress

  def remoteAddress: SocketAddress =
    socket.getRemoteAddress

  def connect (addr: SocketAddress): Async [Unit] =
    async { cb =>
      try {
        socket.connect (addr, cb, Callback.UnitHandler)
      } catch {
        case t: Throwable => cb.fail (t)
      }}

  def close(): Unit =
    socket.close()

  private def read (dsts: Array [ByteBuffer]): Async [Long] =
    async (socket.read (dsts, 0, dsts.length, -1, MILLISECONDS, _, Callback.LongHandler))

  private def write (srcs: Array [ByteBuffer]): Async [Long] =
    async (socket.write (srcs, 0, srcs.length, -1, MILLISECONDS, _, Callback.LongHandler))

  /** Read from the socket until `input` has at least `len` readable bytes.  If `input` already has
    * that many readable bytes, this will immediately queue the callback on the scheduler.
    */
  def fill (input: PagedBuffer, len: Int): Async [Unit] = {
    input.capacity (input.writePos + len)
    val bufs = input.buffers (input.writePos, input.writeableBytes)
    whilst (input.readableBytes < len) {
      for (result <- read (bufs)) yield {
        require (result <= Int.MaxValue)
        if (result < 0)
          throw new Exception ("End of file reached.")
        input.writePos = input.writePos + result.toInt
      }}}

  /** Read a frame with its own length from the socket; return the length.
    *
    * Ensure `input` has at least four readable bytes, reading from the socket if necessary.
    * Interpret those as the length of bytes needed.  Read from the socket again if necessary,
    * until `input` has at least that many additional readable bytes.
    *
    * The mated method `frame` lives in [[com.treode.pickle.Pickler Pickler]].
    */
  def deframe (input: PagedBuffer): Async [Int] = {
    for {
      _ <- fill (input, 4)
      len = input.readInt()
      _ <- fill (input, len)
    } yield len
  }

  /** Write all readable bytes from `output` to the socket. */
  def flush (output: PagedBuffer): Async [Unit] = {
    val bufs = output.buffers (output.readPos, output.readableBytes)
    whilst (output.readableBytes > 0) {
      for (result <- write (bufs)) yield {
        require (result <= Int.MaxValue)
        if (result < 0)
          throw new Exception ("File write failed.")
        output.readPos = output.readPos + result.toInt
      }}}}

object Socket {

  def open (group: AsynchronousChannelGroup) (implicit scheduler: Scheduler): Socket =
    new Socket (AsynchronousSocketChannel.open (group)) (scheduler)
}
