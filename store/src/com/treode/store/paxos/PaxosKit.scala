package com.treode.store.paxos

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.{Acknowledgements, Cluster}
import com.treode.disk.Disks
import com.treode.store.{Bytes, StoreConfig}
import com.treode.store.tier.TierTable

import Async.async

private class PaxosKit (
    val archive: TierTable
) (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val disks: Disks,
    val config: StoreConfig) extends Paxos {

  val acceptors = new Acceptors (this)

  val proposers = new Proposers (this)

  def locate (key: Bytes): Acknowledgements =
    cluster.locate (Bytes.pickler, 0, key)

  def lead (key: Bytes, value: Bytes): Async [Bytes] =
    proposers.propose (0, key, value)

  def propose (key: Bytes, value: Bytes): Async [Bytes] =
    proposers.propose (random.nextInt (17) + 1, key, value)
}
