package com.treode.store.atomic

import scala.util.Random

import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.cluster.stubs.{MessageCaptor, StubNetwork}
import com.treode.store.StoreTestTools
import org.scalatest.FreeSpec

import StoreTestTools._

class WriteDeputySpec extends FreeSpec {

  "The WriteDeputy should" - {

    "work" in {
      implicit val (random, scheduler, network) = newKit()
      val host = StubAtomicHost.install() .expectPass()
    }
  }

}
