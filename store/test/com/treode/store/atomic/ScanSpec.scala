package com.treode.store.atomic

import com.treode.async.AsyncIterator
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.cluster.stubs.StubNetwork
import com.treode.store._
import org.scalatest.FlatSpec

import AtomicTestTools._
import Bound.{Exclusive, Inclusive}
import Fruits._
import Window.{Between, Recent, Through}

class ScanSpec extends FlatSpec {

  val EMPTY =TableId (0x67)
  val SHORT = TableId (0xFD)
  val LONG = TableId (0xBE)

  private def setup (populate: Boolean) = {
    implicit val kit = StoreTestKit.random()
    import kit.{random, scheduler}

    val hs = Seq.fill (3) (StubAtomicHost .install() .pass)
    val Seq (h1, h2, h3) = hs
    for (h <- hs)
      h.setAtlas (settled (h1, h2, h3))

    h1.putCells (SHORT, Apple##1::1, Banana##1::1)
    h2.putCells (SHORT, Banana##1::1, Grape##1::1)
    h3.putCells (SHORT, Apple##1::1, Grape##1::1)

    h1.putCells (LONG, Apple##2::2, Apple##1::1, Grape##2::2, Grape##1::1)
    h2.putCells (LONG, Apple##3::3, Apple##2::2, Grape##3::3, Grape##1::1)
    h3.putCells (LONG, Apple##3::3, Apple##1::1, Grape##3::3, Grape##2::2)

    (kit, h1)
  }

  "Scan" should "handle an empty table" in {

    val (kit, host) = setup (false)
    import kit.scheduler

    assertCells () {
      host.scan (EMPTY, MinStart, AllTimes, AllSlices)
    }}

  it should "handle a non-empty table" in {

    val (kit, host) = setup (true)
    import kit.scheduler

    assertCells (Apple##1::1, Banana##1::1, Grape##1::1) {
      host.scan (SHORT, MinStart, AllTimes, AllSlices)
    }}

  it should "handle an inclusive start position" in {

    val (kit, host) = setup (true)
    import kit.scheduler

    assertCells (Banana##1::1, Grape##1::1) {
      host.scan (SHORT, Inclusive (Key (Banana, 1)), AllTimes, AllSlices)
    }}

  it should "handle an exclusive start position" in {

    val (kit, host) = setup (true)
    import kit.scheduler

    assertCells (Banana##1::1, Grape##1::1) {
      host.scan (SHORT, Exclusive (Key (Apple, 1)), AllTimes, AllSlices)
    }}

  it should "handle a filter" in {

    val (kit, host) = setup (true)
    import kit.scheduler

    assertCells (Apple##1::1, Grape##1::1) {
      host.scan (LONG, MinStart, Recent (1, true), AllSlices)
    }
    assertCells (Apple##2::2, Grape##2::2) {
      host.scan (LONG, MinStart, Recent (2, true), AllSlices)
    }
    assertCells (Apple##3::3, Grape##3::3) {
      host.scan (LONG, MinStart, Recent (3, true), AllSlices)
    }}

  it should "return only a slice" in {

    val (kit, host) = setup (true)
    import kit.scheduler

    assertCells () {
      host.scan (LONG, MinStart, AllTimes, Slice (0, 4))
    }
    assertCells (Grape##3::3, Grape##2::2, Grape##1::1) {
      host.scan (LONG, MinStart, AllTimes, Slice (1, 4))
    }
    assertCells () {
      host.scan (LONG, MinStart, AllTimes, Slice (2, 4))
    }
    assertCells (Apple##3::3, Apple##2::2, Apple##1::1) {
      host.scan (LONG, MinStart, AllTimes, Slice (3, 4))
    }}}
