package com.treode.cluster.messenger

import java.nio.channels.{AsynchronousSocketChannel => Socket}

import com.treode.cluster.{HostId, MailboxId, Peer}
import com.treode.pickle.{Pickler, pickle}
import io.netty.buffer.{UnpooledByteBufAllocator, ByteBuf}

private [cluster] class LocalConnection (val id: HostId, mbxs: MailboxRegistry) extends Peer {

  def connect (socket: Socket, input: ByteBuf, clientId: HostId) =
    throw new IllegalArgumentException

  def close() = ()

  def send [A] (p: Pickler [A], mbx: MailboxId, msg: A) {
    val buf = ByteBufPool.heapBuffer()
    pickle (p, msg, buf)
    mbxs.deliver (mbx, this, buf)
    buf.release()
  }

  override def hashCode = id.id.hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: Peer => id == that.id
      case _ => false
    }

  override def toString = "Peer(local)"
}
