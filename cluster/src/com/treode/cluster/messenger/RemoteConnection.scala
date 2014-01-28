package com.treode.cluster.messenger

import java.nio.channels.AsynchronousChannelGroup
import java.util
import scala.collection.JavaConversions._
import scala.language.postfixOps
import scala.util.Random

import com.treode.async.{Scheduler, Callback, Fiber}
import com.treode.async.io.Socket
import com.treode.buffer.PagedBuffer
import com.treode.cluster.{ClusterEvents, HostId, MailboxId, Peer, messenger}
import com.treode.cluster.events.Events
import com.treode.cluster.misc.{BackoffTimer, RichInt}
import com.treode.pickle.Pickler

private class RemoteConnection (
  val id: HostId,
  localId: HostId,
  fiber: Fiber,
  group: AsynchronousChannelGroup,
  mailboxes: MailboxRegistry) (
    implicit scheduler: Scheduler,
    events: Events
) extends Peer {

  require (id != localId)

  type PickledMessage = PagedBuffer => Unit
  type Queue = util.ArrayList [PickledMessage]

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

    def send (message: PickledMessage) = ()
  }

  abstract class HaveSocket extends State {

    def socket: Socket
    def clientId: HostId
    def buffer: PagedBuffer
    def backoff: Iterator [Int]

    object Flushed extends Callback [Unit] {

      def pass (v: Unit) {
        //buffer.release()
        RemoteConnection.this.sent ()
      }

      def fail (t: Throwable) {
        //buffer.release()
        events.exceptionWritingMessage (t)
        RemoteConnection.this.disconnect (socket)
      }}

    def flush(): Unit = fiber.spawn {
      socket.flush (buffer, Flushed)
    }

    def enque (message: PickledMessage) {
      message (buffer)
    }

    override def disconnect (socket: Socket) {
      if (socket == this.socket) {
        socket.close()
        state = Disconnected (backoff)
      }}

    override def sent() {
      if (buffer.readableBytes == 0) {
        state = Connected (socket, clientId, buffer)
      } else {
        flush()
        state = Sending (socket, clientId)
      }}

    override def connect (socket: Socket, input: PagedBuffer, clientId: HostId) {
      if (clientId < this.clientId) {
        socket.close()
      } else {
        if (socket != this.socket)
          this.socket.close()
        loop (socket, input)
        if (buffer.readableBytes == 0) {
          state = Connected (socket, clientId, buffer)
        } else {
          flush()
          state = Sending (socket, clientId)
        }}}

    override def close() {
      socket.close()
      state = Closed
    }

    override def send (message: PickledMessage): Unit =
      enque (message)
  }

  case class Disconnected (backoff: Iterator [Int]) extends State {

    val time = System.currentTimeMillis

    override def send (message: PickledMessage) {
      val socket = Socket.open (group, scheduler)
      greet (socket)
      state = new Connecting (socket, localId, time, backoff)
      state.send (message)
    }}

  case class Connecting (socket: Socket,  clientId: HostId, time: Long, backoff: Iterator [Int])
  extends HaveSocket {

    val buffer = PagedBuffer (12)

    override def disconnect (socket: Socket) {
      if (socket == this.socket) {
        socket.close()
        state = Block (time, backoff)
      }}}

  case class Connected (socket: Socket, clientId: HostId, buffer: PagedBuffer) extends HaveSocket {

    def backoff = BlockedTimer.iterator

    override def send (message: PickledMessage) {
      enque (message)
      flush()
      state = Sending (socket, clientId)
    }}

  case class Sending (socket: Socket, clientId: HostId) extends HaveSocket {

    val buffer = PagedBuffer (12)

    def backoff = BlockedTimer.iterator
  }

  case class Block (time: Long, backoff: Iterator [Int]) extends State {

    fiber.at (time + backoff.next) (RemoteConnection.this.unblock())

    override def unblock() {
      state = Disconnected (backoff)
    }}

  case object Closed extends State

  private val BlockedTimer = BackoffTimer (500, 500, 1 minutes) (Random)

  private var state: State = new Disconnected (BlockedTimer.iterator)

  def loop (socket: Socket, input: PagedBuffer) {

    val loop = new Callback [Unit] {

      def pass (v: Unit) {
        mailboxes.deliver (RemoteConnection.this, socket, input, this)
      }

      def fail (t: Throwable) {
        events.exceptionReadingMessage (t)
        disconnect (socket)
      }}

    fiber.spawn (loop())
  }

  private def hearHello (socket: Socket) {
    val input = PagedBuffer (12)
    socket.fill (input, 9, new Callback [Unit] {
      def pass (v: Unit) {
        val Hello (clientId) = Hello.pickler.unpickle (input)
        if (clientId == id) {
          connect (socket, input, localId)
        } else {
          events.errorWhileGreeting (id, clientId)
          disconnect (socket)
        }}
      def fail (t: Throwable) {
        events.exceptionWhileGreeting (t)
        disconnect (socket)
      }})
  }

  private def sayHello (socket: Socket) {
    val buffer = PagedBuffer (12)
    Hello.pickler.pickle (Hello (localId), buffer)
    socket.flush (buffer, new Callback [Unit] {
      def pass (v: Unit) {
        hearHello (socket)
      }
      def fail (t: Throwable) {
        events.exceptionWhileGreeting (t)
        disconnect (socket)
      }})
  }

  private def greet (socket: Socket) {
    socket.connect (address, new Callback [Unit] {
      def pass (v: Unit) {
        sayHello (socket)
      }
      def fail (t: Throwable) {
        events.exceptionWhileGreeting (t)
        disconnect (socket)
      }})
  }

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

  def close(): Unit = fiber.execute {
    state.close()
  }

  def send [M] (p: Pickler [M], mbx: MailboxId, msg: M): Unit = fiber.execute {
    state.send (MailboxRegistry.frame (p, mbx, msg, _))
  }

  override def hashCode() = id.hashCode()

  override def equals (other: Any): Boolean =
    other match {
      case that: Peer => id == that.id
      case _ => false
    }

  override def toString = "Peer(%08X)" format id.id
}
