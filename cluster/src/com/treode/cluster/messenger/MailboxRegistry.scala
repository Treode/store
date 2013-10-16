package com.treode.cluster.messenger

import java.util.concurrent.ConcurrentHashMap
import scala.util.Random

import com.esotericsoftware.kryo.io.Input
import com.treode.pickle._
import com.treode.cluster.{EphemeralMailbox, ClusterEvents, MailboxId, Peer}
import com.treode.cluster.concurrent.{Mailbox, Scheduler}
import com.treode.cluster.events.Events

class MailboxRegistry (implicit events: Events) {

  // Visible for testing.
  private [messenger] val mbxs = new ConcurrentHashMap [Long, PickledFunction]

  private [messenger] def deliver (id: MailboxId, from: Peer, input: Input, length: Int) {
    if (length == 0)
      return

    val end = input.position + length
    val f = mbxs.get (id.id)
    if (f == null) {
      if (id.isFixed)
        events.mailboxNotRecognized (id, length)
      input.setPosition (end)
      return
    }

    try {
      f (from, input)
      if (input.position != end) {
        events.unpicklingMessageConsumedWrongNumberOfBytes (id)
        input.setPosition (end)
      }
    } catch {
      case e: Throwable =>
        events.exceptionFromMessageHandler (e)
        input.setPosition (end)
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

    def apply (from: Peer, input: Input): Unit =
      mbx.send (unpickle (p, input), from)
  }

  def open [M] (p: Pickler [M], scheduler: Scheduler): EphemeralMailbox [M] = {
    var mbx = new EphemeralImpl (Random.nextLong, p, scheduler)
    while (mbx.id.isFixed || mbxs.putIfAbsent (mbx.id.id, mbx) != null)
      mbx = new EphemeralImpl (Random.nextLong, p, scheduler)
    mbx
  }}
