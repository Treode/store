package com.treode.disk

import org.scalatest.FlatSpec

import DiskTestTools._

class AbstractReleaserSpec extends FlatSpec {

  implicit val config = DiskTestConfig()

  val geometry = DiskGeometry.test()

  private class TestReleaser () {

    val releaser  = new AbstractReleaser [Int] {}

    def join(): Int =
      releaser._join()

    def leaveAndExpect (epoch: Int) (fs: Int*): Unit =
      assertResult (fs) (releaser._leave (epoch))

    def releaseAndExpect (ns: Int*) (fs: Int*): Unit =
      assertResult (fs) (releaser._release (ns))
  }

  "The SegmentReleaser" should "free immediately when there are no parties" in {
    val releaser = new TestReleaser
    releaser.releaseAndExpect (0) (0)
  }

  it should "not free until the party leaves" in {
    val releaser = new TestReleaser
    val e1 = releaser.join()
    releaser.releaseAndExpect (0) ()
    releaser.leaveAndExpect (e1) (0)
  }

  it should "not free until previous epoch is freed" in {
    val releaser = new TestReleaser
    val e1 = releaser.join()
    releaser.releaseAndExpect (0) ()
    releaser.releaseAndExpect (1) ()
    releaser.leaveAndExpect (e1) (0, 1)
  }

  it should "not free until the previous epoch is free and the party leaves" in {
    val releaser = new TestReleaser
    val e1 = releaser.join()
    releaser.releaseAndExpect (0) ()
    val e2 = releaser.join()
    releaser.releaseAndExpect (1) ()
    releaser.leaveAndExpect (e1) (0)
    releaser.leaveAndExpect (e2) (1)
  }

  it should "not free until the parties leave the previous epoch is free" in {
    val releaser = new TestReleaser
    val e1 = releaser.join()
    releaser.releaseAndExpect (0) ()
    val e2 = releaser.join()
    releaser.releaseAndExpect (1) ()
    releaser.leaveAndExpect (e2) ()
    releaser.leaveAndExpect (e1) (0, 1)
  }}
