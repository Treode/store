package com.treode.cluster

import java.net.SocketAddress
import scala.collection.JavaConversions._

import com.treode.async.io.Socket
import com.treode.buffer.PagedBuffer
import com.treode.pickle.Pickler

class StubConnection (cluster: BaseStubCluster, val id: HostId, localId: HostId) extends Peer {

  import cluster._

  def send [M] (p: Pickler [M], mbx: MailboxId, msg: M): Unit =
    cluster.deliver (p, localId, id, mbx, msg)

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

  override def toString = s"ConnectionStub($localId->$id)"
}
