package com.treode.cluster

import scala.util.Random

import com.treode.async.Scheduler
import com.treode.cluster.messenger.{PeerRegistry, MailboxRegistry}
import com.treode.pickle.Pickler

trait Host {

  val localId: HostId
  val random: Random
  val scheduler: Scheduler
  val mailboxes: MailboxRegistry
  val peers: PeerRegistry

  def locate (id: Int): Acknowledgements

  def locate [K] (p: Pickler [K], seed: Long, key: K): Acknowledgements =
    locate (0)
}
