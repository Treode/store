package com.treode.cluster

import java.net.SocketAddress

import com.treode.async.io.Socket
import com.treode.buffer.PagedBuffer
import com.treode.pickle.Pickler

trait Peer {

  private var _address: SocketAddress = null

  private [cluster] def address_= (v: SocketAddress): Unit =
    _address = v

  def address: SocketAddress =
    _address

  private var _load: Double = 0.0

  private [cluster] def load_= (v: Double): Unit =
    _load = v

  def load: Double =
    _load

  private var _time: Long = 0

  private [cluster] def time_= (v: Long): Unit =
    _time = v

  def time: Long =
    _time

  private [cluster] def connect (socket: Socket, input: PagedBuffer, clientId: HostId)

  private [cluster] def close()

  def id: HostId

  def send [A] (p: Pickler [A], port: PortId, msg: A)
}

object Peer {

  private [cluster] val address = {
    import ClusterPicklers._
    RumorDescriptor (0x4E39A29FA7477F53L, socketAddress)
  }

  private [cluster] val load = {
    import ClusterPicklers._
    RumorDescriptor (0x13DB5A22A5B05595L, double)
  }

  private [cluster] val time = {
    import ClusterPicklers._
    RumorDescriptor (0xC566EFF9BBF991AFL, ulong)
  }}
