package com.treode.cluster

import com.treode.async.{Async, Mailbox, Scheduler}
import com.treode.async.io.Socket
import com.treode.buffer.{Input, PagedBuffer, Output}
import com.treode.pickle.{InvalidTagException, Pickler, Picklers, PicklerRegistry}

import PicklerRegistry.FunctionTag

class MailboxRegistry {

  private type Handler = FunctionTag [Peer, Any]

  private val mailboxes =
    PicklerRegistry [Handler] { id: Long =>
      if (MailboxId (id) .isFixed)
        throw new InvalidTagException ("mailbox", id)
      else
        PicklerRegistry.const [Peer, Any] (id, ())
    }

  private [cluster] def deliver [M] (p: Pickler [M], from: Peer, mbx: MailboxId, msg: M) {
    val handler = mailboxes.loopback (p, mbx.id, msg)
    handler (from)
  }

  private [cluster] def deliver (from: Peer, socket: Socket, buffer: PagedBuffer): Async [Unit] = {
    for (len <- socket.deframe (buffer))
      yield mailboxes.unpickle (buffer, len) (from)
  }

  def listen [M] (p: Pickler [M], id: MailboxId) (f: (M, Peer) => Any): Unit =
    PicklerRegistry.tupled (mailboxes, p, id.id) (f)

  private class EphemeralMailboxImpl [M] (val id: MailboxId, mbx: Mailbox [(M, Peer)])
  extends EphemeralMailbox [M] {

    def close() =
      mailboxes.unregister (id.id)

    def receive (receiver: (M, Peer) => Any) =
      mbx.receive {case (msg, from) => receiver (msg, from)}

    def whilst (condition: => Boolean) (receiver: (M, Peer) => Any) {
      if (condition) {
        mbx.receive { case (msg, from) =>
          receiver (msg, from)
          whilst (condition) (receiver)
        }
      } else {
        close()
      }}}

  def open [M] (p: Pickler [M], scheduler: Scheduler): EphemeralMailbox [M] = {
    val mbx = new Mailbox [(M, Peer)] (scheduler)
    val cmbx = Mailbox.curried2 (mbx)
    val id = mailboxes.open (p) {
      val id = MailboxId.newEphemeral
      (id.id, PicklerRegistry.curried (p, id.id) (cmbx))
    }
    new EphemeralMailboxImpl (id, mbx)
  }}

private object MailboxRegistry {

  def frame [M] (p: Pickler [M], id: MailboxId, msg: M, buf: PagedBuffer): Unit =
    Picklers.tuple (MailboxId.pickler, p) .frame ((id, msg), buf)
}
