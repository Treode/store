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

        "for one host" taggedAs (Intensive, Periodic) in {
          for1host { implicit random =>
            achieveConsensus (10, 10)
          }}

        "three stable hosts" taggedAs (Intensive, Periodic) in {
          for3hosts { implicit random =>
            achieveConsensus (10, 10)
          }}

        "eight stable hosts" taggedAs (Intensive, Periodic) in {
          for8hosts { implicit random =>
            achieveConsensus (10, 10)
          }}

        "one of three hosts is offline" taggedAs (Intensive, Periodic) in {
          for3with1offline { implicit random =>
            achieveConsensus (10, 10)
          }}

        "one of three hosts crashes" taggedAs (Intensive, Periodic) in {
          for3with1crashing { implicit random =>
            achieveConsensus (10, 10)
          }}

        "one of three hosts reboots" taggedAs (Intensive, Periodic) in {
          for3with1rebooting { implicit random =>
            achieveConsensus (10, 10)
          }}

        "one of three hosts bounces" taggedAs (Intensive, Periodic) in {
          for3with1bouncing { implicit random =>
            achieveConsensus (10, 10)
          }}

        "one host moving to another" taggedAs (Intensive, Periodic) in {
          for1to1 { implicit random =>
            achieveConsensus (10, 10)
          }}

        "one host growing to three" taggedAs (Intensive, Periodic) in {
          for1to3 { implicit random =>
            achieveConsensus (10, 10)
          }}

        "one host growing to three, one bounces" taggedAs (Intensive, Periodic) in {
          for1to3with1bouncing { implicit random =>
            achieveConsensus (10, 10)
          }}

        "three hosts shrinking to one" taggedAs (Intensive, Periodic) in {
          for3to1 { implicit random =>
            achieveConsensus (10, 10)
          }}

        "three hosts shrinking to one, one bounces" taggedAs (Intensive, Periodic) in {
          for3to1with1bouncing { implicit random =>
            achieveConsensus (10, 10)
          }}

        "three hosts replacing one" taggedAs (Intensive, Periodic) in {
          for3replacing1 { implicit random =>
            achieveConsensus (10, 10)
          }}

        "three hosts replacing one, source bounces" taggedAs (Intensive, Periodic) in {
          for3replacing1withSourceBouncing { implicit random =>
            achieveConsensus (10, 10)
          }}

        "three hosts replacing one, target bounces" taggedAs (Intensive, Periodic) in {
          for3replacing1withTargetBouncing { implicit random =>
            achieveConsensus (10, 10)
          }}

        "three hosts replacing one, common bounces" taggedAs (Intensive, Periodic) in {
          for3replacing1withCommonBouncing { implicit random =>
            achieveConsensus (10, 10)
          }}

        "three hosts replacing two" taggedAs (Intensive, Periodic) in {
          for3replacing2 { implicit random =>
            achieveConsensus (10, 10)
          }}

        "three hosts replacing two, source bounces" taggedAs (Intensive, Periodic) in {
          for3replacing2withSourceBouncing { implicit random =>
            achieveConsensus (10, 10)
          }}

        "three hosts replacing two, target bounces" taggedAs (Intensive, Periodic) in {
          for3replacing2withTargetBouncing { implicit random =>
            achieveConsensus (10, 10)
          }}

        "three hosts replacing two, common bounces" taggedAs (Intensive, Periodic) in {
          for3replacing2withCommonBouncing { implicit random =>
            achieveConsensus (10, 10)
          }}

        "three hosts moving to three others" taggedAs (Intensive, Periodic) in {
          for3to3 { implicit random =>
            achieveConsensus (10, 10)
          }}

        "three hosts moving to three others, source bounces" taggedAs (Intensive, Periodic) in {
          for3to3withSourceBouncing { implicit random =>
            achieveConsensus (10, 10)
          }}

        "three hosts moving to three others, target bounces" taggedAs (Intensive, Periodic) in {
          for3to3withTargetBouncing { implicit random =>
            achieveConsensus (10, 10)
          }}

        "for three hosts growing to eight" taggedAs (Intensive, Periodic) in {
          for3to8 { implicit random =>
            achieveConsensus (10, 10)
          }}

        "for eight hosts shrinking to three" taggedAs (Intensive, Periodic) in {
          for8to3 { implicit random =>
            achieveConsensus (10, 10)
          }}}}}}
