package com.treode.cluster.messenger

import com.treode.async.io.Socket
import com.treode.buffer.PagedBuffer
import com.treode.cluster.{HostId, MailboxId, Peer}
import com.treode.pickle.{Pickler, pickle}

private class LocalConnection (val id: HostId, mbxs: MailboxRegistry) extends Peer {

  def connect (socket: Socket, input: PagedBuffer, clientId: HostId) =
    throw new IllegalArgumentException

  def close() = ()

  def send [A] (p: Pickler [A], mbx: MailboxId, msg: A) {
    val buffer = PagedBuffer (12)
    pickle (p, msg, buffer)
    mbxs.deliver (mbx, this, buffer, buffer.writePos)
  }

  override def hashCode = id.id.hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: Peer => id == that.id
      case _ => false
    }

  override def toString = "Peer(local)"
}
