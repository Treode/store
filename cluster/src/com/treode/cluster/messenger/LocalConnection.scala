package com.treode.cluster.messenger

import com.treode.cluster.{HostId, MailboxId, Peer}
import com.treode.cluster.io.Socket
import com.treode.pickle.{Buffer, Pickler, pickle}

private class LocalConnection (val id: HostId, mbxs: MailboxRegistry) extends Peer {

  def connect (socket: Socket, input: Buffer, clientId: HostId) =
    throw new IllegalArgumentException

  def close() = ()

  def send [A] (p: Pickler [A], mbx: MailboxId, msg: A) {
    val buffer = Buffer (12)
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
