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

package com.treode.cluster

import java.net.{InetSocketAddress, SocketAddress}
import java.nio.channels.AsynchronousChannelGroup
import scala.util.{Failure, Success}

import com.treode.async.{Callback, Scheduler}
import com.treode.async.io.{Socket, ServerSocket}
import com.treode.buffer.PagedBuffer

private class Listener (
  cellId: CellId,
  localId: HostId,
  localAddr: SocketAddress,
  group: AsynchronousChannelGroup,
  peers: PeerRegistry
) (implicit
    scheduler: Scheduler
) {

  private var server: ServerSocket = null

  private def sayHello (socket: Socket, input: PagedBuffer, remoteId: HostId) {
    val buffer = PagedBuffer (12)
    Hello.pickler.frame (Hello (localId, cellId), buffer)
    socket.flush (buffer) run {
      case Success (v) =>
        peers.get (remoteId) connect (socket, input, remoteId)
      case Failure (t) if isClosedException (t) =>
        ()
      case Failure (t) =>
        log.exceptionWhileGreeting (t)
        socket.close()
    }}

  private def hearHello (socket: Socket) {
    val buffer = PagedBuffer (12)
    socket.deframe (buffer) run {
      case Success (length) =>
        val Hello (remoteId, remoteCellId) = Hello.pickler.unpickle (buffer)
        if (remoteCellId == cellId) {
          sayHello (socket, buffer, remoteId)
        } else {
          log.rejectedForeignCell (remoteId, remoteCellId)
          socket.close()
        }
      case Failure (t) if isClosedException (t) =>
        ()
      case Failure (t) =>
        log.exceptionWhileGreeting (t)
        socket.close()
    }}

  private def loop() {
    server.accept() run {
      case Success (socket) =>
        scheduler.execute (hearHello (socket))
        loop()
      case Failure (t) if isClosedException (t) =>
        ()
      case Failure (t: Throwable) =>
        log.recyclingMessengerSocket (t)
        server.close()
        scheduler.delay (200) (startup())
        throw t
    }}

  def startup() {
    server = ServerSocket.open (group)
    server.bind (localAddr)
    log.acceptingConnections (cellId, localId, localAddr)
    loop()
  }

  def shutdown(): Unit =
    if (server != null) server.close()
}
