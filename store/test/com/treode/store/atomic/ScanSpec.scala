package com.treode.store.atomic

import com.treode.async.AsyncIterator
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.cluster.stubs.StubNetwork
import com.treode.store._
import org.scalatest.FlatSpec

import AtomicTestTools._
import Fruits._

class ScanSpec extends FlatSpec {

  val ID: TableId = 0x67FDFBBE91E7CE8EL

  def assertSeq [A] (expected: A*) (actual: AsyncIterator [A]) (implicit s: StubScheduler): Unit =
    assertResult (expected) (actual.toSeq)

  private def setup (populate: Boolean) = {
    implicit val kit = StoreTestKit.random()
    import kit.{random, scheduler}

    val hs = Seq.fill (3) (StubAtomicHost .install() .pass)
    val Seq (h1, h2, h3) = hs
    for (h <- hs)
      h.setAtlas (settled (h1, h2, h3))

    if (populate) {
      h1.putCells (ID, Apple##1::1, Banana##1::1)
      h2.putCells (ID, Banana##1::1, Grape##1::1)
      h3.putCells (ID, Apple##1::1, Grape##1::1)
    }

    (kit, h1)
  }

  "Scan" should "handle an empty table" in {

    val (kit, host) = setup (false)
    import kit.scheduler

    assertSeq () (host.scan (ID, Bound.Inclusive (Key.MinValue)))
  }

  it should "handle a filled table (through all)" in {

    val (kit, host) = setup (true)
    import kit.scheduler

    assertSeq (Apple##1::1, Banana##1::1, Grape##1::1) {
      host.scan (ID, Bound.Inclusive (Key.MinValue))
    }}

  it should "handle a filled table (inclusive)" in {

    val (kit, host) = setup (true)
    import kit.scheduler

    assertSeq (Banana##1::1, Grape##1::1) {
      host.scan (ID, Bound.Inclusive (Key (Banana, 1)))
    }}

  it should "handle a filled table (exclusive)" in {

    val (kit, host) = setup (true)
    import kit.scheduler

    assertSeq (Banana##1::1, Grape##1::1) {
      host.scan (ID, Bound.Exclusive (Key (Apple, 1)))
    }}}
