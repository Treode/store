package com.treode.disk

import org.scalatest.FlatSpec

class SegmentReleaserSpec extends FlatSpec {

  private implicit class RichSegmentReleaser (releaser: SegmentReleaser) {

    def ptrs (ns: Seq [Int]): Seq [SegmentPointer] =
        ns map (SegmentPointer (0, _))

    def leaveAndExpect (epoch: Int) (fs: Int*): Unit =
      expectResult (ptrs (fs)) (releaser._leave (epoch))

    def releaseAndExpect (ns: Int*) (fs: Int*): Unit =
      expectResult (ptrs (fs)) (releaser._release (ptrs (ns)))
  }

  "The SegmentReleaser" should "free immediately when there are no parties" in {
    val releaser = new SegmentReleaser (null)
    releaser.releaseAndExpect (0) (0)
  }

  it should "not free until the party leaves" in {
    val releaser = new SegmentReleaser (null)
    val e1 = releaser.join()
    releaser.releaseAndExpect (0) ()
    releaser.leaveAndExpect (e1) (0)
  }

  it should "not free until previous epoch is freed" in {
    val releaser = new SegmentReleaser (null)
    val e1 = releaser.join()
    releaser.releaseAndExpect (0) ()
    releaser.releaseAndExpect (1) ()
    releaser.leaveAndExpect (e1) (0, 1)
  }

  it should "not free until the previous epoch is free and the party leaves" in {
    val releaser = new SegmentReleaser (null)
    val e1 = releaser.join()
    releaser.releaseAndExpect (0) ()
    val e2 = releaser.join()
    releaser.releaseAndExpect (1) ()
    releaser.leaveAndExpect (e1) (0)
    releaser.leaveAndExpect (e2) (1)
  }

  it should "not free until the parties leave the previous epoch is free" in {
    val releaser = new SegmentReleaser (null)
    val e1 = releaser.join()
    releaser.releaseAndExpect (0) ()
    val e2 = releaser.join()
    releaser.releaseAndExpect (1) ()
    releaser.leaveAndExpect (e2) ()
    releaser.leaveAndExpect (e1) (0, 1)
  }}
