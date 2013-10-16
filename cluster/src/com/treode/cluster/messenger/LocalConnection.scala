package com.treode.cluster.messenger

import com.esotericsoftware.kryo.io.{Input, Output}
import com.treode.cluster.{HostId, MailboxId, Peer}
import com.treode.pickle.{Pickler, pickle}
import com.treode.cluster.Socket

private class LocalConnection (val id: HostId, mbxs: MailboxRegistry) extends Peer {

  def connect (socket: Socket, input: Input, clientId: HostId) =
    throw new IllegalArgumentException

  def close() = ()

  def send [A] (p: Pickler [A], mbx: MailboxId, msg: A) {
    val output = new Output (256)
    pickle (p, msg, output)
    val input = new Input (output.getBuffer)
    mbxs.deliver (mbx, this, input, output.position)
  }

  override def hashCode = id.id.hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: Peer => id == that.id
      case _ => false
    }

  override def toString = "Peer(local)"
}
