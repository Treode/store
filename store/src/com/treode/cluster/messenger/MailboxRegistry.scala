package com.treode.cluster.messenger

import java.util.concurrent.ConcurrentHashMap

import com.treode.pickle.Pickler
import com.treode.cluster.{ClusterEvents, MailboxId, Peer}
import com.treode.cluster.events.Events
import io.netty.buffer.ByteBuf

class MailboxRegistry (implicit events: Events) {

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

  def register [M] (p: Pickler [M], id: MailboxId) (f: (M, Peer) => Any) {
    val pf = PickledFunction (p, f)
    require (id.isFixed, "The id for a registered mailbox must be fixed: " + id)
    require (mbxs.putIfAbsent (id.id, pf) == null, "Mailbox already registered: " + id)
  }}
