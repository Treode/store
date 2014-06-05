package com.treode.store.paxos

import scala.util.Random

import com.treode.store.StoreTestConfig
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.{FreeSpec, ParallelTestExecution}

class PaxosParallelSpec extends FreeSpec with ParallelTestExecution with PaxosBehaviors {

  "The paxos implementation should" - {

    "achieve consensus with" - {

      implicit val config = StoreTestConfig()

      val init = { implicit random: Random =>
        achieveConsensus (10, 10)
      }

      for3hostsMT (init)
      for3with1bouncingMT  (init)
      for3to8MT (init)
    }}}
