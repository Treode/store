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
            if checkpoint >= compaction
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

        "three stable hosts" taggedAs (Intensive, Periodic) in {
          forThreeStableHosts { implicit random =>
            achieveConsensus (10, 10)
          }}

        "one of three hosts is offline" taggedAs (Intensive, Periodic) in {
          forOneHostOffline { implicit random =>
            achieveConsensus (10, 10)
          }}

        "one of three hosts crashes" taggedAs (Intensive, Periodic) in {
          forOneHostCrashing { implicit random =>
            achieveConsensus (10, 10)
          }}

        "one of three hosts reboots" taggedAs (Intensive, Periodic) in {
          forOneHostRebooting { implicit random =>
            achieveConsensus (10, 10)
          }}

        "one of three hosts bounces" taggedAs (Intensive, Periodic) in {
          forOneHostBouncing { implicit random =>
            achieveConsensus (10, 10)
          }}

        "one host moving to another" taggedAs (Intensive, Periodic) in {
          forOneHostMoving { implicit random =>
            achieveConsensus (10, 10)
          }}

        "three hosts moving to others" taggedAs (Intensive, Periodic) in {
          forThreeHostsMoving { implicit random =>
            achieveConsensus (10, 10)
          }}

        "for three hosts growing to eight" taggedAs (Intensive, Periodic) in {
          for3to8 { implicit random =>
            achieveConsensus (10, 10)
          }}
      }}}}
