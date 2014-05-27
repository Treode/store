package com.treode.store.paxos

import com.treode.store.StoreTestConfig
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.{FreeSpec, ParallelTestExecution}

class PaxosParallelSpec extends FreeSpec with ParallelTestExecution with PaxosBehaviors {

  "The paxos implementation should" - {

    "achieve consensus with" - {

      "a flakey network and" - {

        implicit val config = StoreTestConfig()

        "three stable hosts (multithreaded)" taggedAs (Intensive, Periodic) in {
          for3hostsMT { implicit random =>
            achieveConsensus (10, 10)
          }}

        "a host bounces (multithreaded)" taggedAs (Intensive, Periodic) in {
          for3with1bouncingMT { implicit random =>
            achieveConsensus (10, 10)
          }}

        "for three hosts growing to eight (multithreaded)" taggedAs (Intensive, Periodic) in {
          for3to8MT { implicit random =>
            achieveConsensus (10, 10)
          }}}}}}
