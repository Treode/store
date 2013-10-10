package com.treode.cluster

import java.nio.channels.{AsynchronousSocketChannel => Socket}

import com.treode.pickle.Pickler
import java.net.SocketAddress
import io.netty.buffer.ByteBuf

trait Peer {

  var address: SocketAddress = null

  private [cluster] def connect (socket: Socket, input: ByteBuf, clientId: HostId)
  private [cluster] def close()

  def id: HostId
  def send [A] (p: Pickler [A], mbx: MailboxId, msg: A)
}
