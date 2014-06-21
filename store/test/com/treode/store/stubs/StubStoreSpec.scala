package com.treode.store.stubs

import java.util.concurrent.Executors
import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.stubs.{AsyncChecks, StubScheduler}
import com.treode.tags.{Intensive, Periodic}
import com.treode.store._
import org.scalatest.FreeSpec

import StoreTestTools._

class StubStoreSpec extends FreeSpec with AsyncChecks with StoreBehaviors {

  val newStore = { kit: StoreTestKit =>
    new StubStore () (kit.scheduler)
  }

  "The StubStore should" - {

    behave like aStore (newStore)

    "conserve money during account transfers (multithreaded)" taggedAs (Intensive, Periodic) in {
      multithreaded { scheduler =>
        implicit val kit = StoreTestKit.multithreaded (scheduler)
        testAccountTransfers (100) (newStore)
      }}}}
