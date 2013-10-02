package com.treode.cluster.messenger

import java.nio.channels.{AsynchronousSocketChannel => Socket, AsynchronousChannelGroup, CompletionHandler}
import java.util
import scala.collection.JavaConversions._
import scala.language.postfixOps
import scala.util.Random

import com.treode.cluster.{ClusterEvents, HostId, MailboxId, Peer, messenger}
import com.treode.cluster.concurrent.Fiber
import com.treode.cluster.events.Events
import com.treode.cluster.misc.{BackoffTimer, RichInt}
import com.treode.pickle.{Pickler, pickle, unpickle}
import io.netty.buffer.ByteBuf

private [cluster] class RemoteConnection (
  val id: HostId,
  localId: HostId,
  fiber: Fiber,
  group: AsynchronousChannelGroup,
  mailboxes: MailboxRegistry) (
    implicit events: Events
) extends Peer {

  require (id != localId)

  abstract class State {

    def disconnect (socket: Socket) = ()

    def unblock() = ()

    def sent() = ()

    def connect (socket: Socket, input: ByteBuf, clientId: HostId) {
      Loop (socket, input)
      state = Connected (socket, clientId)
    }

    def close() {
      state = Closed
    }

    def send (message: PickledMessage) = ()
  }

  abstract class HaveSocket extends State {

    def socket: Socket
    def clientId: HostId
    def backoff: Iterator [Int]

    val queue = new util.ArrayList [PickledMessage] ()

    object Flushed extends CompletionHandler [Void, Void] {

      def completed (result: Void, attachment: Void) {
        RemoteConnection.this.sent ()
      }

      def failed (exc: Throwable, attachment: Void) {
        events.exceptionWritingMessage (exc)
        RemoteConnection.this.disconnect (socket)
      }}

    def flush (messages: util.ArrayList [PickledMessage]): Unit = fiber.spawn {
      val buffer = ByteBufPool.buffer()
      for (m <- messages) {
        buffer.writeLong (m.mbx.id)
        buffer.writerIndex (buffer.writerIndex + 4)
        val start = buffer.writerIndex
        m.write (buffer)
        val end = buffer.writerIndex
        buffer.setInt (start - 4, end - start)
      }
      messenger.flush (socket, buffer, Flushed)
    }

    override def disconnect (socket: Socket) {
      if (socket == this.socket) {
        socket.close()
        state = Disconnected (backoff)
      }}

    override def sent() {
      if (queue.isEmpty) {
        state = Connected (socket, clientId)
      } else {
        flush (queue)
        state = Sending (socket, clientId)
      }}

    override def connect (socket: Socket, input: ByteBuf, clientId: HostId) {
      if (clientId < this.clientId) {
        socket.close()
      } else {
        if (socket != this.socket)
          this.socket.close()
        Loop (socket, input)
        if (queue.isEmpty) {
          state = Connected (socket, clientId)
        } else {
          flush (queue)
          state = Sending (socket, clientId)
        }}}

    override def close() {
      socket.close()
      state = Closed
    }

    override def send (message: PickledMessage) {
      queue.add (message)
    }}

  case class Disconnected (backoff: Iterator [Int]) extends State {

    val time = System.currentTimeMillis

    override def send (message: PickledMessage) {
      val socket = Socket.open (group)
      greet (socket)
      state = new Connecting (socket, localId, time, backoff)
      state.send (message)
    }}

  case class Connecting (socket: Socket,  clientId: HostId, time: Long, backoff: Iterator [Int]) extends HaveSocket {

    override def disconnect (socket: Socket) {
      if (socket == this.socket) {
        socket.close()
        state = Block (time, backoff)
      }}}

  case class Connected (socket: Socket, clientId: HostId) extends HaveSocket {

    def backoff = BlockedTimer.iterator

    override def send (message: PickledMessage) {
      queue.add (message)
      flush (queue)
      state = Sending (socket, clientId)
    }}

  case class Sending (socket: Socket, clientId: HostId) extends HaveSocket {

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

  case class Loop (socket: Socket, input: ByteBuf) {

    case class MessageRead (mbx: MailboxId, length: Int) extends CompletionHandler [Void, Void] {

      def completed (result: Void, attachment: Void) {
        if (length > 0)
          mailboxes.deliver (mbx, RemoteConnection.this, input.readSlice (length))
        input.discardSomeReadBytes()
        readHeader()
      }

      def failed (exc: Throwable, attachment: Void) {
        events.exceptionReadingMessage (exc)
        input.release()
        disconnect (socket)
      }}

    def readMessage (mbx: MailboxId, length: Int): Unit =
      messenger.ensure (socket, input, length, MessageRead (mbx, length))

    object HeaderRead extends CompletionHandler [Void, Void] {

      def completed (result: Void, attachment: Void) {
        val mbx = input.readLong()
        val length = input.readInt()
        readMessage (mbx, length)
      }

      def failed (exc: Throwable, attachment: Void) {
        events.exceptionReadingMessage (exc)
        input.release()
        disconnect (socket)
      }}

    def readHeader(): Unit =
      messenger.ensure (socket, input, 12, HeaderRead)

    fiber.spawn (readHeader())
  }

  private def hearHello (socket: Socket) {
    val buffer = ByteBufPool.buffer()
    messenger.ensure (socket, buffer, 9, new CompletionHandler [Void, Void] {
      def completed (result: Void, attachment: Void) {
        val Hello (clientId) = unpickle (Hello.pickle, buffer)
        if (clientId == id) {
          Loop (socket, buffer)
          connect (socket, buffer, localId)
        } else {
          events.errorWhileGreeting (id, clientId)
          buffer.release()
          disconnect (socket)
        }}
      def failed (exc: Throwable, attachment: Void) {
        events.exceptionWhileGreeting (exc)
        buffer.release()
        disconnect (socket)
      }})
  }

  private def sayHello (socket: Socket) {
    val buffer = ByteBufPool.buffer (16)
    pickle (Hello.pickle, Hello (localId), buffer)
    messenger.flush (socket, buffer, new CompletionHandler [Void, Void] {
      def completed (result: Void, attachment: Void) {
        hearHello (socket)
      }
      def failed (exc: Throwable, attachment: Void) {
        events.exceptionWhileGreeting (exc)
        disconnect (socket)
      }})
  }

  private def greet (socket: Socket) {
    socket.connect (address, null, new CompletionHandler [Void, Void] {
      def completed (result: Void, attachment: Void) {
        sayHello (socket)
      }
      def failed (exc: Throwable, attachment: Void) {
        events.exceptionWhileGreeting (exc)
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

  def connect (socket: Socket, input: ByteBuf, clientId: HostId): Unit = fiber.execute {
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
