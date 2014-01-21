package com.treode.cluster

import java.net.SocketAddress

import com.treode.async.io.Socket
import com.treode.buffer.PagedBuffer
import com.treode.pickle.Pickler

private class StubConnection (val id: HostId, localId: HostId, network: StubNetwork)
extends Peer {

  address = new SocketAddress {}

  def send [M] (p: Pickler [M], mbx: MailboxId, msg: M): Unit =
    network.deliver (p, localId, id, mbx, msg)

  // Stubs do not require this.
  def connect (socket: Socket, input: PagedBuffer, clientId: HostId) =
    throw new UnsupportedOperationException

  def close(): Unit = ()

  override def hashCode() = id.hashCode()

  override def equals (other: Any): Boolean =
    other match {
      case that: Peer => id == that.id
      case _ => false
    }

  override def toString = s"StubConnection($localId->$id)"
}
