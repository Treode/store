package com.treode.store.atomic

import java.util.concurrent.Executors
import scala.util.Random

import com.treode.async.stubs.{AsyncChecks, CallbackCaptor, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.cluster.stubs.StubNetwork
import com.treode.store._
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.{BeforeAndAfterAll, FreeSpec, PropSpec, Suites}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.SpanSugar

import AtomicTestTools._
import SpanSugar._
import WriteOp._

class AtomicSpec extends FreeSpec with StoreBehaviors with AsyncChecks {

  override val timeLimit = 15 minutes

  private val newStore = { implicit kit: StoreTestKit =>
    import kit.{random, scheduler, network}
    val hs = Seq.fill (3) (StubAtomicHost.install() .pass)
    val Seq (h1, h2, h3) = hs
    for (h <- hs)
      h.setAtlas (settled (h1, h2, h3))
    new TestableCluster (hs)
  }

  "The atomic implementation should" - {

    behave like aStore (newStore)

    "conserve money during account transfers" taggedAs (Intensive, Periodic) in {
      forAllSeeds { random =>
        implicit val kit = StoreTestKit.random (random)
        testAccountTransfers (100) (newStore)
      }}

    "conserve money during account transfers (multithreaded)" taggedAs (Intensive, Periodic) in {
      multithreaded { scheduler =>
        implicit val kit = StoreTestKit.multithreaded (scheduler)
        testAccountTransfers (100) (newStore)
      }}}}
