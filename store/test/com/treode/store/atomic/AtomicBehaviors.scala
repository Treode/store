package com.treode.store.atomic

import scala.collection.{JavaConversions, SortedMap}
import scala.util.Random

import com.treode.async.{Async, AsyncIterator, Scheduler}
import com.treode.async.stubs.implicits._
import com.treode.cluster.stubs.StubNetwork
import com.treode.disk.stubs.{CrashChecks, StubDiskDrive}
import com.treode.store._
import org.scalatest.{Informing, Suite}

import Async.supply
import AtomicTestTools._
import AtomicTracker._
import JavaConversions._

trait AtomicBehaviors extends CrashChecks with StoreClusterChecks {
  this: Suite with Informing =>

  private def scan (hosts: Seq [StubAtomicHost]) (implicit scheduler: Scheduler) = {
    var cells = newTrackedCells
    for {
      _ <- for ((t, c) <- AsyncIterator.merge (hosts map (_.scan))) supply {
        val tk = (t.id, c.key.long)
        cells += tk -> (cells (tk) + ((c.time, c.value.get.int)))
      }
    } yield {
      cells
    }}

  private [atomic] def crashAndRecover (
      nbatches: Int,
      ntables: Int,
      nkeys: Int,
      nwrites: Int,
      nops: Int
  ) (implicit
      random: Random,
      config: StoreTestConfig
  ) = {

    val tracker = new AtomicTracker
    val disk = new StubDiskDrive

    crash
    .info (s"$config")
    .info (s"crashAndRecover ($nbatches, $ntables, $nkeys, $nwrites, $nops)")

    .setup { implicit scheduler =>
      implicit val network = StubNetwork (random)
      for {
        host <- StubAtomicHost.boot (H1, disk, true)
        _ = host.setAtlas (settled (host))
        _ <- tracker.batches (nbatches, ntables, nkeys, nwrites, nops, host)
      } yield ()
    }

    .recover { implicit scheduler =>
      implicit val network = StubNetwork (random)
      val host = StubAtomicHost.boot (H1, disk, false) .pass
      host.setAtlas (settled (host))
      scheduler.run (timers = !host.atomic.writers.deputies.isEmpty)
      tracker.check (host) .pass
    }}

  private [atomic] def issueAtomicWrites (
      nbatches: Int,
      ntables: Int,
      nkeys: Int,
      nwrites: Int,
      nops: Int
  ) (implicit
      random: Random
  ) = {

    val tracker = new AtomicTracker

    cluster.info (s"achieveConsensus ($nbatches, $ntables, $nkeys, $nwrites, $nops)")

    .host (StubAtomicHost)

    .setup { implicit scheduler => (h1, h2) =>
      tracker.batches (nbatches, ntables, nkeys, nwrites, nops, h1)
    }

    .whilst { hosts =>
      def unsettled = hosts exists (_.unsettled)
      def deputiesOpen = hosts exists (_.deputiesOpen)
      unsettled || deputiesOpen
    }

    .verify { implicit scheduler => host =>
      tracker.check (host)
    }

    .whilst { hosts =>
      def unsettled = hosts exists (_.unsettled)
      def deputiesOpen = hosts exists (_.deputiesOpen)
      unsettled || deputiesOpen
    }

    .audit { implicit scheduler => hosts =>
      for {
        cells <- scan (hosts)
      } yield {
        tracker.check (cells)
      }}}}