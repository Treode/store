package com.treode.store.paxos

import com.treode.async.stubs.implicits._
import com.treode.cluster.stubs.StubNetwork
import com.treode.disk.stubs.StubDiskDrive
import com.treode.store.{Bytes, StoreTestConfig, StoreTestKit}
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec

import PaxosTestTools._

class PaxosSequentialSpec extends FreeSpec with PaxosBehaviors {

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

        implicit val config = StoreTestConfig (
            checkpointProbability = checkpoint,
            compactionProbability = compaction)

        for { (name, (nbatch, nputs)) <- Seq (
            "some batches"     -> (10, 10),
            "lots of batches"  -> (100, 10),
            "some big batches" -> (10, 100))
        } name taggedAs (Intensive, Periodic) in {

          forAllCrashes { implicit random =>
            crashAndRecover (nbatch, nputs)
          }}}}}

    "achieve consensus with" - {

      for { (name, flakiness) <- Seq (
          "a reliable network" -> 0.0,
          "a flakey network"   -> 0.1)
      } s"$name and" - {

        implicit val config = StoreTestConfig (messageFlakiness = flakiness)

        "stable hosts" taggedAs (Intensive, Periodic) in {
          forThreeStableHosts { implicit random =>
            achieveConsensus (10, 10)
          }}

        "a host is offline" taggedAs (Intensive, Periodic) in {
          forOneHostOffline { implicit random =>
            achieveConsensus (10, 10)
          }}

        "a host crashes" taggedAs (Intensive, Periodic) in {
          forOneHostCrashing { implicit random =>
            achieveConsensus (10, 10)
          }}

        "a host reboots" taggedAs (Intensive, Periodic) in {
          forOneHostRebooting { implicit random =>
            achieveConsensus (10, 10)
          }}

        "a host bounces" taggedAs (Intensive, Periodic) in {
          forOneHostBouncing { implicit random =>
            achieveConsensus (10, 10)
          }}}}

    "rebalance" in { pending
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
