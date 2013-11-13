package com.treode.cluster

import java.net.SocketAddress
import scala.collection.JavaConversions._

import com.treode.cluster.io.Socket
import com.treode.pickle.{Buffer, Pickler}

class ConnectionStub (cluster: ClusterStubBase, val id: HostId, localId: HostId) extends Peer {

  import cluster._

  def send [M] (p: Pickler [M], mbx: MailboxId, msg: M): Unit =
    cluster.deliver (p, localId, id, mbx, msg)

  // Stubs do not require this.
  def connect (socket: Socket, input: Buffer, clientId: HostId) =
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
