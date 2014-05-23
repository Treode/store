package com.treode.store.paxos

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.misc.EpochReleaser
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

  import library.atlas

  val acceptors = new Acceptors (this)
  val proposers = new Proposers (this)
  val releaser = new EpochReleaser
  val mover = new PaxosMover (this)

  def place (key: Bytes, time: TxClock): Int =
    atlas.place (locator, (key, time))

  def locate (key: Bytes, time: TxClock): Cohort =
    atlas.locate (locator, (key, time))

  def track (key: Bytes, time: TxClock): ReplyTracker =
    locate (key, time) .track

  def lead (key: Bytes, time: TxClock, value: Bytes): Async [Bytes] =
    proposers.propose (0, key, time, value)

  def propose (key: Bytes, time: TxClock, value: Bytes): Async [Bytes] =
    proposers.propose (random.nextInt (17) + 1, key, time, value)

  def rebalance (atlas: Atlas): Async [Unit] = {
    val targets = Targets (atlas)
    for {
      _ <- when (!targets.isEmpty) (releaser.release())
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
