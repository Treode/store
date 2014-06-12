package com.treode.cluster

import com.treode.async.Async
import com.treode.async.io.Socket
import com.treode.buffer.PagedBuffer
import com.treode.pickle.Pickler

import Async.supply

private class LocalConnection (val id: HostId, ports: PortRegistry) extends Peer {

  def connect (socket: Socket, input: PagedBuffer, clientId: HostId) =
    throw new IllegalArgumentException

  def close(): Async [Unit] = supply()

  def send [M] (p: Pickler [M], port: PortId, msg: M): Unit =
    ports.deliver (p, this, port, msg)

  override def hashCode = id.id.hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: Peer => id == that.id
      case _ => false
    }

  override def toString = "Peer(local)"
}
