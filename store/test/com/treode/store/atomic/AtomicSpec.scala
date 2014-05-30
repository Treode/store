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

  "The atomic implementation should" - {

    behave like aStore { implicit kit =>
      import kit.{random, scheduler, network}
      val hs = Seq.fill (3) (StubAtomicHost .install() .pass)
      val Seq (h1, h2, h3) = hs
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))
      new TestableCluster (hs)
    }

    behave like aMultithreadableStore (100) { implicit kit =>
      import kit.{random, scheduler, network}
      val hs = Seq.fill (3) (StubAtomicHost .install() .await)
      val Seq (h1, h2, h3) = hs
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))
      new TestableCluster (hs)
    }}}
