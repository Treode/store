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

import java.nio.channels.AsynchronousChannelGroup
import java.util
import scala.language.postfixOps
import scala.util.{Failure, Random, Success}

import com.treode.async.{Async, Backoff, Callback, Fiber, Scheduler}
import com.treode.async.implicits._
import com.treode.async.io.Socket
import com.treode.async.misc.RichInt
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{PickledValue, Pickler}

private class RemoteConnection (
  val id: HostId,
  localId: HostId,
  cellId: CellId,
  group: AsynchronousChannelGroup,
  ports: PortRegistry
) (implicit
    scheduler: Scheduler,
    config: ClusterConfig
) extends Peer {

  import config.connectingBackoff

  require (id != localId)

  type Queue = util.ArrayList [PickledValue]

  abstract class State {

    def disconnect (socket: Socket) = ()

    def unblock() = ()

    def sent() = ()

    def connect (socket: Socket, input: PagedBuffer, clientId: HostId) {
      loop (socket, input)
      state = Connected (socket, clientId, PagedBuffer (12))
    }

    def close() {
      state = Closed
    }

    def send (message: PickledValue) = ()
  }

  abstract class HaveSocket extends State {

    def socket: Socket
    def clientId: HostId
    def buffer: PagedBuffer
    def backoff: Iterator [Int]

    def enque (message: PickledValue) {
      PortRegistry.frame (message, buffer)
    }

    override def disconnect (socket: Socket) {
      if (socket == this.socket) {
        _close (socket)
        state = Disconnected (backoff)
      }}

    override def sent() {
      if (buffer.readableBytes == 0) {
        state = Connected (socket, clientId, buffer)
      } else {
        flush (socket, buffer)
        state = Sending (socket, clientId)
      }}

    override def connect (socket: Socket, input: PagedBuffer, clientId: HostId) {
      if (clientId < this.clientId) {
        _close (socket)
      } else {
        if (socket != this.socket)
          _close (this.socket)
        loop (socket, input)
        if (buffer.readableBytes == 0) {
          state = Connected (socket, clientId, buffer)
        } else {
          flush (socket, buffer)
          state = Sending (socket, clientId)
        }}}

    override def close() {
      state = Closed
      _close (socket)
    }

    override def send (message: PickledValue): Unit =
      enque (message)
  }

  case class Disconnected (backoff: Iterator [Int]) extends State {

    val time = System.currentTimeMillis

    override def send (message: PickledValue) {
      if (address != null) {
        val socket = Socket.open (group)
        greet (socket)
        state = new Connecting (socket, localId, time, backoff)
        state.send (message)
      }}

    override def toString = "Disconnected"
  }

  case class Connecting (socket: Socket,  clientId: HostId, time: Long, backoff: Iterator [Int])
  extends HaveSocket {

    val buffer = PagedBuffer (12)

    override def disconnect (socket: Socket) {
      if (socket == this.socket) {
        state = Block (time, backoff)
        _close (socket)
      }}

    override def toString = "Connecting"
  }

  case class Connected (socket: Socket, clientId: HostId, buffer: PagedBuffer) extends HaveSocket {

    def backoff = connectingBackoff.iterator (Random)

    override def sent() {
      if (buffer.readableBytes > 0) {
        flush (socket, buffer)
        state = Sending (socket, clientId)
      }}

    override def send (message: PickledValue) {
      enque (message)
      flush (socket, buffer)
      state = Sending (socket, clientId)
    }

    override def toString = "Connected"
  }

  case class Sending (socket: Socket, clientId: HostId) extends HaveSocket {

    val buffer = PagedBuffer (12)

    def backoff = connectingBackoff.iterator (Random)

    override def toString = "Sending"
  }

  case class Block (time: Long, backoff: Iterator [Int]) extends State {

    fiber.at (time + backoff.next) (RemoteConnection.this.unblock())

    override def unblock() {
      state = Disconnected (backoff)
    }

    override def toString = "Block"
  }

  case object Closed extends State {

    override def toString = "Closed"
  }

  private val fiber = new Fiber
  private var state: State = new Disconnected (connectingBackoff.iterator (Random))

  private def _close (socket: Socket): Unit =
    try {
      log.disconnected (id, socket.localAddress, socket.remoteAddress)
      socket.close()
    } catch {
      case t: Throwable if isClosedException (t) => ()
    }

  def loop (socket: Socket, input: PagedBuffer) {
    log.connected (id, socket.localAddress, socket.remoteAddress)
    val loop = Callback.fix [Unit] { loop => {
      case Success (v) =>
        ports.deliver (RemoteConnection.this, socket, input) run (loop)
      case Failure (t) if isClosedException (t) =>
        disconnect (socket)
      case Failure (t) =>
        log.exceptionReadingMessage (t)
        disconnect (socket)
    }}
    scheduler.execute (loop.pass (()))
  }

  def flush (socket: Socket, buffer: PagedBuffer): Unit = scheduler.execute {
    socket.flush (buffer) run {
      case Success (v) =>
        sent()
      case Failure (t) if isClosedException (t) =>
        disconnect (socket)
      case Failure (t) =>
        log.exceptionWritingMessage (t)
        disconnect (socket)
    }}

  private def hearHello (socket: Socket) {
    val input = PagedBuffer (12)
    socket.deframe (input) run {
      case Success (length) =>
        val Hello (clientId, clientCellId) = Hello.pickler.unpickle (input)
        if (clientId == id && clientCellId == cellId) {
          connect (socket, input, localId)
        } else if (clientCellId != cellId) {
          log.rejectedForeignCell (clientId, clientCellId)
          disconnect (socket)
        } else {
          log.errorWhileGreeting (id, clientId)
          disconnect (socket)
        }
      case Failure (t) if isClosedException (t) =>
        disconnect (socket)
      case Failure (t) =>
        log.exceptionWhileGreeting (t)
        disconnect (socket)
    }}

  private def sayHello (socket: Socket) {
    val buffer = PagedBuffer (12)
    Hello.pickler.frame (Hello (localId, cellId), buffer)
    socket.flush (buffer) run {
      case Success (v) =>
        hearHello (socket)
      case Failure (t) if isClosedException (t) =>
        disconnect (socket)
      case Failure (t) =>
        log.exceptionWhileGreeting (t)
        disconnect (socket)
    }}

  private def greet (socket: Socket) {
    socket.connect (address) run {
      case Success (v) =>
        sayHello (socket)
      case Failure (t) if isClosedException (t) =>
        disconnect (socket)
      case Failure (t) =>
        log.exceptionWhileGreeting (t)
        disconnect (socket)
    }}

  private def disconnect (socket: Socket): Unit = fiber.execute {
    state.disconnect (socket)
  }

  private def unblock(): Unit = fiber.execute {
    state.unblock()
  }

  private def sent(): Unit = fiber.execute {
    state.sent()
  }

  def connect (socket: Socket, input: PagedBuffer, clientId: HostId): Unit = fiber.execute {
    state.connect (socket, input, clientId)
  }

  def close(): Async [Unit] = fiber.supply {
    state.close()
  }

  def send [M] (p: Pickler [M], port: PortId, msg: M): Unit = fiber.execute {
    state.send (PickledValue (port.id, p, msg))
  }

  override def hashCode() = id.hashCode()

  override def equals (other: Any): Boolean =
    other match {
      case that: Peer => id == that.id
      case _ => false
    }

  override def toString =
    if (id.id < 256) f"Peer:${id.id}%02X" else f"Peer:${id.id}%016X"
}
