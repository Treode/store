package com.treode.store.paxos

import com.treode.tags.{Intensive, Periodic}
import org.scalatest.{FreeSpec, ParallelTestExecution}

class PaxosParallelSpec extends FreeSpec with ParallelTestExecution with PaxosBehaviors {

  "The paxos implementation should" - {

    "achieve consensus with" - {

      for { (name, flakiness) <- Seq (
          //"a reliable network" -> 0.0,
          "a flakey network"   -> 0.1)
      } s"$name and" - {

        "stable hosts (multithreaded)" taggedAs (Intensive, Periodic) in {
          forThreeStableHostsMultithreaded (0.1, 0.1, flakiness) { implicit random =>
            achieveConsensus (100, 3)
          }}

        "a host is offline (multithreaded)" taggedAs (Intensive, Periodic) in {
          forOneHostOfflineMultithreaded (0.1, 0.1, flakiness) { implicit random =>
            achieveConsensus (100, 3)
          }}

        "a host crashes (multithreaded)" taggedAs (Intensive, Periodic) in {
          forOneHostCrashingMultithreaded (0.1, 0.1, flakiness) { implicit random =>
            achieveConsensus (100, 3)
          }}

        "a host reboots (multithreaded)" taggedAs (Intensive, Periodic) in {
          forOneHostRebootingMultithreaded (0.1, 0.1, flakiness) { implicit random =>
            achieveConsensus (100, 3)
          }}

        "a host bounces (multithreaded)" taggedAs (Intensive, Periodic) in {
          forOneHostBouncingMultithreaded (0.1, 0.1, flakiness) { implicit random =>
            achieveConsensus (100, 3)
          }}}}}}
