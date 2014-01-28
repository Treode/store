package com.treode.cluster.messenger

import com.treode.async.{Callback, Mailbox, Scheduler, callback}
import com.treode.async.io.Socket
import com.treode.buffer.{Input, PagedBuffer, Output}
import com.treode.cluster.{EphemeralMailbox, MailboxId, Peer}
import com.treode.pickle.{InvalidTagException, Pickler, Picklers, PicklerRegistry}

import PicklerRegistry.TaggedFunction

class MailboxRegistry {

  private type Handler = TaggedFunction [Peer, Any]

  private val mailboxes =
    PicklerRegistry [Handler] { id =>
      if (MailboxId (id) .isFixed)
        PicklerRegistry.const [Peer, Any] (())
      else
        throw new InvalidTagException ("mailbox", id)
    }

  private [cluster] def deliver [M] (p: Pickler [M], from: Peer, mbx: MailboxId, msg: M) {
    val handler = mailboxes.loopback (p, mbx.id, msg)
    handler (from)
  }

  private [cluster] def deliver (from: Peer, socket: Socket, buffer: PagedBuffer, cb: Callback [Unit]) {
    socket.deframe (buffer, callback (cb) { len =>
      val handler = mailboxes.unpickle (buffer, len)
      handler (from)
    })
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
