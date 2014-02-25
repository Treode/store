package com.treode.cluster

import java.net.SocketAddress

import com.treode.async.io.Socket
import com.treode.buffer.PagedBuffer
import com.treode.pickle.Pickler

trait Peer {

  var address: SocketAddress = null

  private [cluster] def connect (socket: Socket, input: PagedBuffer, clientId: HostId)
  private [cluster] def close()

  def id: HostId
  def send [A] (p: Pickler [A], mbx: MailboxId, msg: A)
}

object Peer {

  val void: Peer =
    new Peer {
      def connect (socket: Socket, input: PagedBuffer, clientId: HostId) = ()
      def close() = ()
      def id: HostId = ???
      def send [A] (p: Pickler [A], mbx: MailboxId, msg: A) = ()
    }}
