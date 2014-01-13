package com.treode.cluster.messenger

import scala.util.Random

import com.treode.async.{Callback, Mailbox, Scheduler, callback}
import com.treode.async.io.{Framer, Socket}
import com.treode.buffer.PagedBuffer
import com.treode.cluster.{EphemeralMailbox, ClusterEvents, MailboxId, Peer}
import com.treode.cluster.events.Events
import com.treode.pickle.{Pickler, pickle, unpickle}

class MailboxRegistry (implicit events: Events) {

  private val mailboxes = new Framer [MailboxId, Peer => Any] (MailboxId.framer)

  private [cluster] def deliver [M] (p: Pickler [M], from: Peer, mbx: MailboxId, msg: M): Unit =
    mailboxes.send (p, mbx, msg, _ (from))

  private [cluster] def deliver (from: Peer, socket: Socket, buffer: PagedBuffer, cb: Callback [Unit]) {
    mailboxes.read (socket, buffer, _ (from), callback (cb) { _ =>
      buffer.discard (buffer.readPos)
    })
  }

  def register [M] (p: Pickler [M], id: MailboxId) (f: (M, Peer) => Any): Unit =
    mailboxes.register (p, id, f.curried)

  private class EphemeralMailboxImpl [M] (val id: MailboxId, mbx: Mailbox [(M, Peer)])
  extends EphemeralMailbox [M] {

    def close() =
      mailboxes.unregister (id)

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
    val id = mailboxes.register (p, Mailbox.curried2 (mbx))
    new EphemeralMailboxImpl (id, mbx)
  }}
