package com.treode.store.paxos

import scala.util.Random

import com.treode.async.{Async, AsyncIterator, Scheduler}
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.implicits._
import com.treode.cluster.{Cluster, HostId}
import com.treode.cluster.stubs.{StubPeer, StubNetwork}
import com.treode.disk.Disk
import com.treode.disk.stubs.{StubDisk, StubDiskDrive}
import com.treode.store._
import com.treode.store.catalog.Catalogs
import com.treode.store.tier.TierTable
import org.scalatest.Assertions

import Assertions._
import Async.{guard, supply}
import PaxosTestTools._

private class StubPaxosHost (
    val localId: HostId
) (implicit
    val random: Random,
    val scheduler: ChildScheduler,
    val cluster: StubPeer,
    val disks: Disk,
    val library: Library,
    val catalogs: Catalogs,
    val paxos: PaxosKit
) extends StoreClusterChecks.Host {

  val librarian = Librarian (paxos.rebalance _)

  cluster.startup()

  def shutdown() {
    cluster.shutdown()
    scheduler.shutdown()
  }

  def setAtlas (cohorts: Cohort*) {
    val atlas = Atlas (cohorts.toArray, 1)
    library.atlas = atlas
    library.residents = atlas.residents (localId)
  }

  def issueAtlas (cohorts: Cohort*): Async [Unit] = {
    var issued = false
    var tries = 0
    scheduler.whilst (!issued) {
      val save = (library.atlas, library.residents)
      val version = library.atlas.version + 1
      val atlas = Atlas (cohorts.toArray, version)
      library.atlas = atlas
      library.residents = atlas.residents (localId)
      catalogs
          .issue (Atlas.catalog) (version, atlas)
          .map (_ => issued = true)
          .recover {
            case _: Throwable if !(library.atlas eq atlas) && library.atlas == atlas =>
              issued = true
            case t: StaleException if tries < 16 =>
              if (library.atlas eq atlas) {
                library.atlas = save._1
                library.residents = save._2
              }
              tries += 1
            case t: TimeoutException if tries < 16 =>
              if (library.atlas eq atlas) {
                library.atlas = save._1
                library.residents = save._2
              }
              tries += 1
          }}}

  def atlas: Atlas =
    library.atlas

  def unsettled: Boolean =
    !library.atlas.settled

  def acceptorsOpen: Boolean =
    !paxos.acceptors.acceptors.isEmpty

  def proposersOpen: Boolean =
    !paxos.proposers.proposers.isEmpty

  def audit: AsyncIterator [Cell] =
    paxos.archive.iterator (Residents.all)

  def propose (key: Bytes, time: TxClock, value: Bytes): Async [Bytes] =
    paxos.propose (key, time, value)
}

private object StubPaxosHost extends StoreClusterChecks.Package [StubPaxosHost] {

  def boot (
      id: HostId,
      drive: StubDiskDrive,
      init: Boolean
  ) (implicit
      random: Random,
      parent: Scheduler,
      network: StubNetwork,
      config: StoreTestConfig
  ): Async [StubPaxosHost] = {

    import config.{checkpointProbability, compactionProbability}

    implicit val scheduler = new ChildScheduler (parent)
    implicit val cluster = new StubPeer (id)
    implicit val library = new Library
    implicit val recovery = StubDisk.recover (config.stubDiskConfig)
    implicit val _catalogs = Catalogs.recover()
    val _paxos = Paxos.recover()

    for {
      launch <- if (init) recovery.attach (drive) else recovery.reattach (drive)
      catalogs <- _catalogs.launch (launch)
      paxos <- _paxos.launch (launch) map (_.asInstanceOf [PaxosKit])
    } yield {
      launch.launch()
      new StubPaxosHost (id) (random, scheduler, cluster, launch.disks, library, catalogs, paxos)
    }}

  def install () (implicit r: Random, s: Scheduler, n: StubNetwork): Async [StubPaxosHost] = {
    implicit val config = StoreTestConfig()
    boot (r.nextLong, new StubDiskDrive, true)
  }}
