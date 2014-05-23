package com.treode.async.misc

import org.scalatest.FlatSpec

class EpochReleaserSpec extends FlatSpec {

  private class TestReleaser () {

    val releaser = new EpochReleaser
    var released = Seq.empty [Int]

    def join(): Int =
      releaser. join()

    def leaveAndExpect (epoch: Int) (fs: Int*) {
      released = Seq.empty
      releaser.leave (epoch)
      assertResult (fs) (released)
    }

    def releaseAndExpect (ns: Int*) (fs: Int*) {
      released = Seq.empty
      releaser.release (released ++= ns)
      assertResult (fs) (released)
    }}

  "The EpochReleaser" should "free immediately when there are no parties" in {
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
