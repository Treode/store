package com.treode.store.atomic

import scala.util.Random

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.cluster.{Cluster, Peer, ReplyTracker}
import com.treode.disk.Disk
import com.treode.store._
import com.treode.store.paxos.Paxos
import com.treode.store.tier.TierTable

import Async.{async, supply}
import AtomicMover.Targets
import WriteDirector.deliberate

private class AtomicKit (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val disk: Disk,
    val library: Library,
    val paxos: Paxos,
    val config: StoreConfig
) extends Atomic {

  import library.{atlas, releaser}

  val tables = new TimedStore (this)
  val reader = new ReadDeputy (this)
  val writers = new WriteDeputies (this)
  val scanner = new ScanDeputy (this)
  val mover = new AtomicMover (this)

  def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]] =
    releaser.join (ReadDirector.read (rt, ops, this))

  def write (xid: TxId, ct: TxClock, ops: WriteOp*): Async [TxClock] =
    releaser.join (WriteDirector.write (xid, ct, ops, this))

  def status (xid: TxId): Async [TxStatus] =
    deliberate.propose (xid.id, xid.time, TxStatus.Aborted)

  def scan (table: TableId, start: Bound [Key], window: Window, slice: Slice): CellIterator =
    ScanDirector.scan (table, start, window, slice, this)

  def hosts (slice: Slice): Seq [(Peer, Int)] =
    atlas.hosts (slice) .map {case (host, count) => (cluster.peer (host), count)}

  def rebalance (atlas: Atlas): Async [Unit] = {
    val targets = Targets (atlas)
    for {
      _ <- mover.rebalance (targets)
    } yield {
      if (targets.isEmpty)
        tables.compact()
    }}}
