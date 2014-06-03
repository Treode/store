package com.treode.store.stubs

import java.util.concurrent.Executors
import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.stubs.{AsyncChecks, StubScheduler}
import com.treode.store._
import org.scalatest.FreeSpec

import StoreTestTools._

class StubStoreSpec extends FreeSpec with AsyncChecks with StoreBehaviors {

  def newStubStore (kit: StoreTestKit): StubStore =
    new StubStore () (kit.scheduler)

  "The StubStore should" - {

    behave like aStore (newStubStore _)

    behave like aMultithreadableStore (10000) (newStubStore _)
  }}
