package com.treode.store.paxos

import scala.util.Random

import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.cluster.stubs.StubNetwork
import com.treode.disk.stubs.{CrashChecks, StubDiskDrive}
import com.treode.store.{StoreClusterChecks, StoreTestConfig}
import org.scalatest.{Informing, Suite}

import PaxosTestTools._

trait PaxosBehaviors extends CrashChecks with StoreClusterChecks {
  this: Suite with Informing =>

  private [paxos] def crashAndRecover (
      nbatch: Int,
      nputs: Int
  ) (implicit
      random: Random,
      config: StoreTestConfig
  ) = {

    val tracker = new PaxosTracker
    val disk = new StubDiskDrive

    setup { implicit scheduler =>
      implicit val network = StubNetwork (random)
      for {
        host <- StubPaxosHost.boot (H1, disk, true)
        _ = host.setAtlas (settled (host))
        _ <- tracker.batches (nbatch, nputs, host)
      } yield ()
    }

    .recover { implicit scheduler =>
      implicit val network = StubNetwork (random)
      val host = StubPaxosHost .boot (H1, disk, false) .pass
      host.setAtlas (settled (host))
      tracker.check (host) .pass
    }}


  private [paxos] def achieveConsensus (nbatches: Int, nputs: Int) (implicit random: Random) = {

    val tracker = new PaxosTracker

    cluster.host (StubPaxosHost)

    .setup { implicit scheduler => (h1, h2) =>
      tracker.batches (nbatches, nputs, h1, h2)
    }

    .recover { implicit scheduler => h1 =>
      tracker.check (h1)
    }}}
