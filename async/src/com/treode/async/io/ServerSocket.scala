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

import java.nio.channels._
import java.net.SocketAddress

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.implicits._

import Async.async

/** Something that can be mocked in tests. */
class ServerSocket (socket: AsynchronousServerSocketChannel) (implicit scheduler: Scheduler) {

  /** Adapts Callback to Java's NIO CompletionHandler. */
  object SocketHandler extends CompletionHandler [AsynchronousSocketChannel, Callback [Socket]] {
    def completed (v: AsynchronousSocketChannel, cb: Callback [Socket]) = cb.pass (new Socket (v))
    def failed (t: Throwable, cb: Callback [Socket]) = cb.fail (t)
  }

  def bind (addr: SocketAddress): Unit =
    socket.bind (addr)

  def accept(): Async [Socket] =
    async { cb =>
      try {
        socket.accept (cb, SocketHandler)
      } catch {
        case t: Throwable => cb.fail (t)
      }}

  def close(): Unit =
    socket.close()
}

object ServerSocket {

  def open (group: AsynchronousChannelGroup) (implicit scheduler: Scheduler): ServerSocket =
    new ServerSocket (AsynchronousServerSocketChannel.open (group)) (scheduler)
}
