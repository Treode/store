package com.treode.cluster.messenger

import java.util.concurrent.ConcurrentHashMap
import scala.util.Random

import com.treode.async.{Mailbox, Scheduler}
import com.treode.buffer.PagedBuffer
import com.treode.cluster.{EphemeralMailbox, ClusterEvents, MailboxId, Peer}
import com.treode.cluster.events.Events
import com.treode.pickle.{Pickler, unpickle}

class MailboxRegistry (implicit events: Events) {

  // Visible for testing.
  private [messenger] val mbxs = new ConcurrentHashMap [Long, PickledFunction]

  private [cluster] def deliver (id: MailboxId, from: Peer, buffer: PagedBuffer, length: Int) {
    if (length == 0)
      return

    val end = buffer.readPos + length
    val f = mbxs.get (id.id)
    if (f == null) {
      if (id.isFixed)
        events.mailboxNotRecognized (id, length)
      buffer.readPos = end
      buffer.discard (end)
      return
    }

    try {
      f (from, buffer)
      if (buffer.readPos != end) {
        events.unpicklingMessageConsumedWrongNumberOfBytes (id)
        buffer.readPos = end
        buffer.discard (end)
      }
    } catch {
      case e: Throwable =>
        events.exceptionFromMessageHandler (e)
        buffer.readPos = end
        buffer.discard (end)
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

  private class EphemeralImpl [M] (val id: MailboxId, p: Pickler [M], scheduler: Scheduler)
      extends EphemeralMailbox [M] with PickledFunction {

    private val mbx = new Mailbox [(M, Peer)] (scheduler)

    def close(): Unit =
      MailboxRegistry.this.close (id, this)

    def receive (receiver: (M, Peer) => Any): Unit =
      mbx.receive {case (msg, from) => receiver (msg, from)}

    def whilst (condition: => Boolean) (receiver: (M, Peer) => Any) {
      if (condition) {
        mbx.receive { case (msg, from) =>
          receiver (msg, from)
          whilst (condition) (receiver)
        }
      } else {
        close()
      }}

    def apply (from: Peer, buffer: PagedBuffer): Unit =
      mbx.send (unpickle (p, buffer), from)
  }

  def open [M] (p: Pickler [M], scheduler: Scheduler): EphemeralMailbox [M] = {
    var mbx = new EphemeralImpl (Random.nextLong, p, scheduler)
    while (mbx.id.isFixed || mbxs.putIfAbsent (mbx.id.id, mbx) != null)
      mbx = new EphemeralImpl (Random.nextLong, p, scheduler)
    mbx
  }}
