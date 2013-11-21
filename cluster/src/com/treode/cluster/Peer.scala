package com.treode.cluster

import java.net.SocketAddress

import com.treode.async.io.Socket
import com.treode.pickle.{Buffer, Pickler}

trait Peer {

  var address: SocketAddress = null

  private [cluster] def connect (socket: Socket, input: Buffer, clientId: HostId)
  private [cluster] def close()

  def id: HostId
  def send [A] (p: Pickler [A], mbx: MailboxId, msg: A)
}
