package com.treode.cluster

import com.treode.async.Scheduler
import com.treode.cluster.messenger.{MailboxRegistry, PeerRegistry}
import com.treode.pickle.Pickler

trait Cluster {

  def listen [M] (desc: MessageDescriptor [M]) (f: (M, Peer) => Any)

  def open [M] (p: Pickler [M], s: Scheduler): EphemeralMailbox [M]

  def peer (id: HostId): Peer

  def locate (id: Int): Acknowledgements

  def locate [K] (p: Pickler [K], seed: Long, key: K): Acknowledgements =
    locate (0)
}
