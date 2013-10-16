package com.treode.cluster.messenger

import java.nio.channels.AsynchronousChannelGroup
import java.util
import scala.collection.JavaConversions._
import scala.language.postfixOps
import scala.util.Random
import com.esotericsoftware.kryo.io.{Input, Output}
import com.treode.cluster.{ClusterEvents, HostId, MailboxId, Peer, messenger}
import com.treode.cluster.concurrent.{Callback, Fiber}
import com.treode.cluster.events.Events
import com.treode.cluster.misc.{BackoffTimer, KryoPool, RichInt}
import com.treode.pickle.{Pickler, pickle, unpickle}
import com.treode.cluster.Socket

private class RemoteConnection (
  val id: HostId,
  localId: HostId,
  fiber: Fiber,
  group: AsynchronousChannelGroup,
  mailboxes: MailboxRegistry) (
    implicit events: Events
) extends Peer {

  require (id != localId)

  type Queue = util.ArrayList [PickledMessage]

  abstract class State {

    def disconnect (socket: Socket) = ()

    def unblock() = ()

    def sent() = ()

    def connect (socket: Socket, input: Input, clientId: HostId) {
      Loop (socket, input)
      state = Connected (socket, clientId, KryoPool.output)
    }

    def close() {
      state = Closed
    }

    def send (message: PickledMessage) = ()
  }

  abstract class HaveSocket extends State {

    def socket: Socket
    def clientId: HostId
    def buffer: Output
    def backoff: Iterator [Int]

    object Flushed extends Callback [Unit] {

      def apply (v: Unit) {
        KryoPool.release (buffer)
        RemoteConnection.this.sent ()
      }

      def fail (t: Throwable) {
        KryoPool.release (buffer)
        events.exceptionWritingMessage (t)
        RemoteConnection.this.disconnect (socket)
      }}

    def flush(): Unit = fiber.spawn {
      messenger.flush (socket, buffer, Flushed)
    }

    def enque (message: PickledMessage) {
      buffer.writeLong (message.mbx.id)
      buffer.writeInt (0)
      val start = buffer.position
      message.write (buffer)
      val end = buffer.position
      buffer.setPosition (start - 4)
      buffer.writeInt (end - start)
      buffer.setPosition (end)
    }

    override def disconnect (socket: Socket) {
      if (socket == this.socket) {
        socket.close()
        state = Disconnected (backoff)
      }}

    override def sent() {
      if (buffer.position == 0) {
        state = Connected (socket, clientId, buffer)
      } else {
        flush()
        state = Sending (socket, clientId)
      }}

    override def connect (socket: Socket, input: Input, clientId: HostId) {
      if (clientId < this.clientId) {
        socket.close()
      } else {
        if (socket != this.socket)
          this.socket.close()
        Loop (socket, input)
        if (buffer.position == 0) {
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
      val socket = Socket.open (group)
      greet (socket)
      state = new Connecting (socket, localId, time, backoff)
      state.send (message)
    }}

  case class Connecting (socket: Socket,  clientId: HostId, time: Long, backoff: Iterator [Int])
  extends HaveSocket {

    val buffer = KryoPool.output

    override def disconnect (socket: Socket) {
      if (socket == this.socket) {
        socket.close()
        state = Block (time, backoff)
      }}}

  case class Connected (socket: Socket, clientId: HostId, buffer: Output) extends HaveSocket {

    def backoff = BlockedTimer.iterator

    override def send (message: PickledMessage) {
      enque (message)
      flush()
      state = Sending (socket, clientId)
    }}

  case class Sending (socket: Socket, clientId: HostId) extends HaveSocket {

    val buffer = KryoPool.output

    def backoff = BlockedTimer.iterator
  }

  case class Block (time: Long, backoff: Iterator [Int]) extends State {

    fiber.at (time + backoff.next) (RemoteConnection.this.unblock())

    override def unblock() {
      state = Disconnected (backoff)
    }}

  case object Closed extends State

  private val BlockedTimer = BackoffTimer (500, 500, 1 minutes) (Random)

  // Visible for testing.
  private [messenger] var state: State = new Disconnected (BlockedTimer.iterator)

  case class Loop (socket: Socket, input: Input) {

    case class MessageRead (mbx: MailboxId, length: Int) extends Callback [Unit] {

      def apply (v: Unit) {
        mailboxes.deliver (mbx, RemoteConnection.this, input, length)
        readHeader()
      }

      def fail (t: Throwable) {
        events.exceptionReadingMessage (t)
        disconnect (socket)
      }}

    def readMessage (mbx: MailboxId, length: Int): Unit =
      messenger.fill (socket, input, length, MessageRead (mbx, length))

    object HeaderRead extends Callback [Unit] {

      def apply (v: Unit) {
        val mbx = input.readLong()
        val length = input.readInt()
        readMessage (mbx, length)
      }

      def fail (t: Throwable) {
        events.exceptionReadingMessage (t)
        disconnect (socket)
      }}

    def readHeader(): Unit =
      messenger.fill (socket, input, 12, HeaderRead)

    fiber.spawn (readHeader())
  }

  private def hearHello (socket: Socket) {
    val buffer = new Input (256)
    messenger.fill (socket, buffer, 9, new Callback [Unit] {
      def apply (v: Unit) {
        val Hello (clientId) = unpickle (Hello.pickle, buffer)
        if (clientId == id) {
          Loop (socket, buffer)
          connect (socket, buffer, localId)
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
    val buffer = new Output (256)
    pickle (Hello.pickle, Hello (localId), buffer)
    messenger.flush (socket, buffer, new Callback [Unit] {
      def apply (v: Unit) {
        hearHello (socket)
      }
      def fail (t: Throwable) {
        events.exceptionWhileGreeting (t)
        disconnect (socket)
      }})
  }

  private def greet (socket: Socket) {
    socket.connect (address, new Callback [Unit] {
      def apply (v: Unit) {
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

  def connect (socket: Socket, input: Input, clientId: HostId): Unit = fiber.execute {
    state.connect (socket, input, clientId)
  }

  def close(): Unit = fiber.execute {
    state.close()
  }

  def send [A] (p: Pickler [A], mbx: MailboxId, msg: A): Unit = fiber.execute {
    state.send (PickledMessage (p, mbx, msg))
  }

  override def hashCode() = id.hashCode()

  override def equals (other: Any): Boolean =
    other match {
      case that: Peer => id == that.id
      case _ => false
    }

  override def toString = "Peer(%08X)" format id.id
}
