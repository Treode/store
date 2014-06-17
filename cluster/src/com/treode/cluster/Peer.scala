package com.treode.cluster

import java.net.SocketAddress

import com.treode.async.Async
import com.treode.async.io.Socket
import com.treode.buffer.PagedBuffer
import com.treode.pickle.Pickler

trait Peer {

  private [cluster] var address: SocketAddress = null

  private [cluster] def connect (socket: Socket, input: PagedBuffer, clientId: HostId)

  private [cluster] def close(): Async [Unit]

  def id: HostId

  def send [A] (p: Pickler [A], port: PortId, msg: A)
}

object Peer {

  private [cluster] val address = {
    import ClusterPicklers._
    RumorDescriptor (0x4E39A29FA7477F53L, socketAddress)
  }}
