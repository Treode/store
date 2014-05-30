package com.treode.store.atomic

import com.treode.store.StoreTestConfig
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec

import AtomicTestTools._

class AtomicSequentialSpec extends FreeSpec with AtomicBehaviors {

  "The atomic implementation should" - {

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

        for { (name, (ntables, nkeys)) <- Seq (
            "few tables"   -> (3, 100),
            "many tables"  -> (30, 100))
        } s"$name with" - {

          for { (name, (nbatches, nwrites, nops)) <- Seq (
              "small batches with small writes" -> (7, 5, 3),
              "small batches with large writes" -> (7, 5, 20),
              "big batches with small writes"   -> (7, 35, 3))
              if !(ntables == 30 && nwrites == 35)
          } name taggedAs (Intensive, Periodic) in {

            forAllCrashes { implicit random =>
              crashAndRecover (nbatches, ntables, nkeys, nwrites, nops)
            }}}}}}

    "issueAtomicWrites with" - {

      for { (name, flakiness) <- Seq (
          //"a reliable network" -> 0.0,
          "a flakey network"   -> 0.1)
      } s"$name and" - {

        implicit val config = StoreTestConfig (messageFlakiness = flakiness)

        forVariousClusters { implicit random =>
            issueAtomicWrites (7, 3, 100, 5, 3)
        }}}}}
