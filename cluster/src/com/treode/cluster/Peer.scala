package com.treode.cluster

import java.net.SocketAddress

import com.esotericsoftware.kryo.io.Input
import com.treode.pickle.Pickler

trait Peer {

  var address: SocketAddress = null

  private [cluster] def connect (socket: Socket, input: Input, clientId: HostId)
  private [cluster] def close()

  def id: HostId
  def send [A] (p: Pickler [A], mbx: MailboxId, msg: A)
}
