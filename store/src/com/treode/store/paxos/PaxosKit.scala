package com.treode.store.paxos

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.{Cluster, ReplyTracker}
import com.treode.disk.Disk
import com.treode.store.{Atlas, Bytes, Cohort, Library, StoreConfig, TxClock}
import com.treode.store.tier.TierTable

import Async.{async, when}
import PaxosKit.locator
import PaxosMover.Targets

private class PaxosKit (
    val archive: TierTable
) (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val disks: Disk,
    val library: Library,
    val config: StoreConfig
) extends Paxos {

  val acceptors = new Acceptors (this)
  val proposers = new Proposers (this)
  val mover = new PaxosMover (this)

  def lead (key: Bytes, time: TxClock, value: Bytes): Async [Bytes] =
    proposers.propose (0, key, time, value)

  def propose (key: Bytes, time: TxClock, value: Bytes): Async [Bytes] =
    proposers.propose (random.nextInt (17) + 1, key, time, value)

  def rebalance (atlas: Atlas): Async [Unit] = {
    val targets = Targets (atlas)
    for {
      _ <- mover.rebalance (targets)
    } yield {
      if (targets.isEmpty)
        archive.compact()
    }}}

private object PaxosKit {

  val locator = {
    import PaxosPicklers._
    tuple (bytes, txClock)
  }}
