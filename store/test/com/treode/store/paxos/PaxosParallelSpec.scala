package com.treode.store.paxos

import com.treode.store.StoreTestConfig
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.{FreeSpec, ParallelTestExecution}

class PaxosParallelSpec extends FreeSpec with ParallelTestExecution with PaxosBehaviors {

  "The paxos implementation should" - {

    "achieve consensus with" - {

      for { (name, flakiness) <- Seq (
          "a reliable network" -> 0.0,
          "a flakey network"   -> 0.1)
      } s"$name and" - {

        implicit val config = StoreTestConfig (messageFlakiness = flakiness)

        "stable hosts (multithreaded)" taggedAs (Intensive, Periodic) in {
          forThreeStableHostsMultithreaded { implicit random =>
            achieveConsensus (100, 3)
          }}

        "a host is offline (multithreaded)" taggedAs (Intensive, Periodic) in {
          forOneHostOfflineMultithreaded { implicit random =>
            achieveConsensus (100, 3)
          }}

        "a host crashes (multithreaded)" taggedAs (Intensive, Periodic) in {
          forOneHostCrashingMultithreaded { implicit random =>
            achieveConsensus (100, 3)
          }}

        "a host reboots (multithreaded)" taggedAs (Intensive, Periodic) in {
          forOneHostRebootingMultithreaded { implicit random =>
            achieveConsensus (100, 3)
          }}

        "a host bounces (multithreaded)" taggedAs (Intensive, Periodic) in {
          forOneHostBouncingMultithreaded { implicit random =>
            achieveConsensus (100, 3)
          }}}}}}
