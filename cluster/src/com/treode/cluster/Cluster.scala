package com.treode.cluster

import java.net.SocketAddress
import com.treode.async.Scheduler
import com.treode.pickle.Pickler

trait Cluster {

  def listen [M] (desc: MessageDescriptor [M]) (f: (M, Peer) => Any)

  def listen [M] (desc: RumorDescriptor [M]) (f: (M, Peer) => Any)

  def hail (remoteId: HostId, remoteAddr: SocketAddress)

  def peer (id: HostId): Peer

  def rpeer: Option [Peer]

  def open [M] (p: Pickler [M], s: Scheduler): EphemeralMailbox [M]

  def spread [M] (desc: RumorDescriptor [M]) (msg: M)

  def locate (id: Int): Acknowledgements

  def locate [K] (p: Pickler [K], seed: Long, key: K): Acknowledgements =
    locate (0)
}
