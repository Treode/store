package com.treode.cluster.stubs

import java.net.SocketAddress

import com.treode.async.io.Socket
import com.treode.buffer.PagedBuffer
import com.treode.cluster.{HostId, Peer, PortId}
import com.treode.pickle.Pickler

private class StubConnection (val id: HostId, localId: HostId, network: StubNetwork)
extends Peer {

  address = new SocketAddress {}

  def send [M] (p: Pickler [M], port: PortId, msg: M): Unit =
    network.deliver (p, localId, id, port, msg)

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

  override def toString =
    f"StubConnection(${localId.id}%02X->${id.id}%02X)"
}
