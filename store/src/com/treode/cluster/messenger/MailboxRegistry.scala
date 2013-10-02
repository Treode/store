package com.treode.cluster.messenger

import java.util.concurrent.ConcurrentHashMap
import scala.util.Random

import com.treode.pickle._
import com.treode.cluster.{EphemeralMailbox, ClusterEvents, MailboxId, Peer}
import com.treode.cluster.concurrent.{Mailbox, Scheduler}
import com.treode.cluster.events.Events
import io.netty.buffer.ByteBuf

class MailboxRegistry (implicit scheduler: Scheduler, events: Events) {

  // Visible for testing.
  private [messenger] val mbxs = new ConcurrentHashMap [Long, PickledFunction]

  private [messenger] def deliver (id: MailboxId, from: Peer, buf: ByteBuf) {
    val f = mbxs.get (id.id)
    if (f != null) {
      try {
        f (buf, from)
      } catch {
        case e: Throwable => events.exceptionFromMessageHandler (e)
      }
    } else if (id.isFixed) {
      events.mailboxNotRecognized (id, buf.readableBytes)
    }}

  private [messenger] def close (id: MailboxId, pf: PickledFunction) {
    require (!id.isFixed, "The id to unregister a mailbox must be ephemeral: " + id)
    mbxs.remove (id.id, pf)
  }

  def register [M] (p: Pickler [M], id: MailboxId) (f: (M, Peer) => Any) {
    val pf = PickledFunction (p, f)
    require (id.isFixed, "The id for a registered mailbox must be fixed: " + id)
    require (mbxs.putIfAbsent (id.id, pf) == null, "Mailbox already registered: " + id)
  }

  private class EphemeralImpl [M] (val id: MailboxId, p: Pickler [M])
      extends EphemeralMailbox [M] with PickledFunction {

    private val mbx = new Mailbox [(M, Peer)] (scheduler)

    def close(): Unit =
      MailboxRegistry.this.close (id, this)

    def receive (receiver: (M, Peer) => Any): Unit =
      mbx.receive {case (msg, from) => receiver (msg, from)}

    def apply (buffer: ByteBuf, from: Peer) = {
      val message = unpickle (p, buffer)
      require (buffer.readableBytes() == 0, "Bytes remain after unpickling message")
      mbx.send (message, from)
    }}

  def open [M] (p: Pickler [M]): EphemeralMailbox [M] = {
    var mbx = new EphemeralImpl (Random.nextLong, p)
    while (mbx.id.isFixed || mbxs.putIfAbsent (mbx.id.id, mbx) != null)
      mbx = new EphemeralImpl (Random.nextLong, p)
    mbx
  }}
