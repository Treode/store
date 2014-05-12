package com.treode.store.paxos

import java.util.concurrent.TimeoutException
import scala.util.Random

import com.treode.async.{Async, Callback}
import com.treode.async.implicits._
import com.treode.async.stubs.AsyncChecks
import com.treode.async.stubs.implicits._
import com.treode.cluster.stubs.StubNetwork
import com.treode.disk.stubs.{CrashChecks, StubDiskDrive}
import com.treode.store.{Bytes, Cell, StoreClusterChecks, StoreTestKit}
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec

import Async.{guard, latch, supply}
import Callback.{ignore => disregard}
import PaxosTestTools._

class PaxosSpec extends FreeSpec with CrashChecks with StoreClusterChecks {

  "The paxos implementation should" - {

    "recover from a crash when" - {

      for { (name, checkpoint) <- Seq (
          "not checkpointed at all"   -> 0.0,
          "checkpointed occasionally" -> 0.01,
          "checkpointed frequently"   -> 0.1)
      } s"$name and" - {

        for { (name, compaction) <- Seq (
            "not compacted at all"   -> 0.0,
            "compacted occasionally" -> 0.01,
            "compacted frequently"   -> 0.1)
      } s"$name with" - {

        for { (name, (nbatch, nput)) <- Seq (
            "some batches"     -> (10, 10),
            "lots of batches"  -> (100, 10),
            "some big batches" -> (10, 100))
        } name taggedAs (Intensive, Periodic) in {

          forAllCrashes { implicit random =>

            val tracker = new PaxosTracker
            val disk = new StubDiskDrive

            setup { implicit scheduler =>
              implicit val network = StubNetwork (random)
              for {
                host <- StubPaxosHost.boot (H1, checkpoint, compaction, disk, true)
                _ = host.setAtlas (settled (host))
                _ <- tracker.batches (nbatch, nput, host)
              } yield ()
            }

            .recover { implicit scheduler =>
              implicit val network = StubNetwork (random)
              val host = StubPaxosHost .boot (H1, checkpoint, compaction, disk, false) .pass
              host.setAtlas (settled (host))
              tracker.check (host) .pass
            }}}}}}

    "achieve consensus with" - {

      def test (implicit random: Random) = {
        val tracker = new PaxosTracker
        cluster.host (StubPaxosHost)
        .setup { implicit scheduler => (h1, h2) =>
          tracker.batches (10, 10, h1, h2)
        }
        .recover { implicit scheduler => h1 =>
          tracker.check (h1)
        }}

      for { (name, flakiness) <- Seq (
          "a reliable network" -> 0.0,
          "a flakey network"   -> 0.1)
      } s"$name and" - {

        "stable hosts" taggedAs (Intensive, Periodic) in {
          forThreeStableHosts (0.1, 0.1, flakiness) { implicit random =>
            test
          }}

        "a host crashes before closing" taggedAs (Intensive, Periodic) in {
          forOneHostCrashing (0.1, 0.1, flakiness) { implicit random =>
            test
          }}

        "a host reboots after opening" taggedAs (Intensive, Periodic) in {
          forOneHostRebooting (0.1, 0.1, flakiness) { implicit random =>
            test
          }}}}

    "rebalance" in {
      implicit val kit = StoreTestKit.random()
      import kit.{network, random, scheduler}

      val hs = Seq.fill (4) (StubPaxosHost .install() .pass)
      val Seq (h1, h2, h3, h4) = hs
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1, h2, h3)) .pass
      h1.issueAtlas (moving (h1, h2, h3) (h1, h2, h4)) .pass

      val k = Bytes (0xB3334572873016E4L)
      h1.paxos.propose (k, 0, 1) .expect (1)

      for (h <- hs)
        h.paxos.archive.get (k, 0) .expect (k##0::1)
      kit.run (count = 1000, timers = true)
      expectAtlas (3, settled (h1, h2, h4)) (hs)
      for (h <- Seq (h1, h2, h4))
        h.paxos.archive.get (k, 0) .expect (k##0::1)
      // TierTable.get does not account for residents.
      // h3.paxos.archive.get (k, 0) .expect (k##0)
    }}}
