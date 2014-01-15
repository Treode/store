package com.treode.cluster.messenger

import com.treode.async.{Callback, Mailbox, Scheduler, callback}
import com.treode.async.io.{Framer, Socket}
import com.treode.buffer.PagedBuffer
import com.treode.cluster.{EphemeralMailbox, MailboxId, Peer}
import com.treode.pickle.{Pickler, pickle, unpickle}

class MailboxRegistry {

  private type Handler = (Peer => Any)
  private val mailboxes = new Framer [MailboxId, MailboxId, Handler] (MailboxRegistry.framer)

  private [cluster] def deliver [M] (p: Pickler [M], from: Peer, mbx: MailboxId, msg: M) {
    val (_, action) = mailboxes.read (p, mbx, msg)
    action foreach (_ (from))
  }

  private [cluster] def deliver (from: Peer, socket: Socket, buffer: PagedBuffer, cb: Callback [Unit]) {
    mailboxes.read (socket, buffer, callback [Unit, (MailboxId, Option [Handler])] (cb) {
      case (_, Some (action)) => action (from)
      case (_, None) => ()
    })
  }

  def register [M] (p: Pickler [M], id: MailboxId) (f: (M, Peer) => Any): Unit =
    mailboxes.register (p, id) (f.curried)

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
    val id = mailboxes.register (p) (Mailbox.curried2 (mbx))
    new EphemeralMailboxImpl (id, mbx)
  }}

object MailboxRegistry {

  val framer: Framer.Strategy [MailboxId, MailboxId] =
    new Framer.Strategy [MailboxId, MailboxId] {

      def newEphemeralId = MailboxId.newEphemeral()

      def isEphemeralId (id: MailboxId) = MailboxId.isEphemeral (id)

      def readHeader (buf: PagedBuffer) = {
        val id = MailboxId (buf.readLong())
        (Some (id), id)
      }

      def writeHeader (hdr: MailboxId, buf: PagedBuffer) {
        buf.writeLong (hdr.id)
      }}}
