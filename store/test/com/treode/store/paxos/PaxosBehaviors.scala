package com.treode.store.paxos

import scala.util.Random

import com.treode.async.{Async, AsyncIterator, Scheduler}
import com.treode.async.implicits._
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.cluster.stubs.StubNetwork
import com.treode.disk.stubs.{CrashChecks, StubDiskDrive}
import com.treode.store._
import org.scalatest.FreeSpec

import Async.supply
import PaxosTestTools._

trait PaxosBehaviors extends CrashChecks with StoreClusterChecks {
  this: FreeSpec =>

  private def scan (hosts: Seq [StubPaxosHost]) (implicit scheduler: Scheduler): Async [Seq [Cell]] = {
    val iter = AsyncIterator.merge (hosts map (_.scan))
    val cells = Seq.newBuilder [Cell]
    for {
      _ <- iter.dedupe.foreach (c => supply (cells += c))
    } yield {
      cells.result
    }}

  private [paxos] def crashAndRecover (
      nbatch: Int,
      nputs: Int
  ) (implicit
      random: Random,
      config: StoreTestConfig
  ) = {

    val tracker = new PaxosTracker
    val disk = new StubDiskDrive

    crash
    .info (s"$config")
    .info (s"crashAndRecover ($nbatch, $nputs)")

    .setup { implicit scheduler =>
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

    cluster.info (s"achieveConsensus ($nbatches, $nputs)")

    .host (StubPaxosHost)

    .setup { implicit scheduler => (h1, h2) =>
      tracker.batches (nbatches, nputs, h1, h2)
    }

    .whilst { hosts =>
      def unsettled = hosts exists (_.unsettled)
      def acceptorsOpen = hosts exists (_.acceptorsOpen)
      def proposersOpen = hosts exists (_.proposersOpen)
      unsettled || acceptorsOpen || proposersOpen
    }

    .verify { implicit scheduler => host =>
      tracker.check (host)
    }

    .whilst { hosts =>
      def unsettled = hosts exists (_.unsettled)
      def acceptorsOpen = hosts exists (_.acceptorsOpen)
      def proposersOpen = hosts exists (_.proposersOpen)
      unsettled || acceptorsOpen || proposersOpen
    }

    .audit { implicit scheduler => hosts =>
      for {
        cells <- scan (hosts)
      } yield {
        tracker.check (cells)
      }}}}
