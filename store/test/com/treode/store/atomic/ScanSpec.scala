package com.treode.store.atomic

import com.treode.async.AsyncIterator
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.cluster.stubs.StubNetwork
import com.treode.store.{Bytes, Fruits, TableId, TxClock}
import org.scalatest.FlatSpec

import AtomicTestTools._
import Fruits._

class ScanSpec extends FlatSpec {

  val ID: TableId = 0x67FDFBBE91E7CE8EL

  def assertSeq [A] (expected: A*) (actual: AsyncIterator [A]) (implicit s: StubScheduler): Unit =
    assertResult (expected) (actual.toSeq)

  "Scan" should "handle an empty table" in {
    val kit = StubNetwork()
    import kit.scheduler

    val hs = kit.install (3, new StubAtomicHost (_, kit))
    val Seq (h1, h2, h3) = hs
    for (h <- hs)
      h.setAtlas (settled (h1, h2, h3))

    assertSeq () (h1.scan (ID, Bytes.empty, TxClock.max))
  }

  it should "handle a filled table" in {
    val kit = StubNetwork()
    import kit.scheduler

    val hs = kit.install (3, new StubAtomicHost (_, kit))
    val Seq (h1, h2, h3) = hs
    for (h <- hs)
      h.setAtlas (settled (h1, h2, h3))

    h1.putCells (ID, Apple##1::1, Banana##1::1)
    h1.putCells (ID, Banana##1::1, Grape##1::1)
    h1.putCells (ID, Apple##1::1, Grape##1::1)

    assertSeq (Apple##1::1, Banana##1::1, Grape##1::1) {
      h1.scan (ID, Bytes.empty, TxClock.max)
    }}}
