package com.treode.disk

import org.scalatest.FlatSpec

class ReleaserSpec extends FlatSpec {

  implicit val config = DisksConfig (8, 1<<24, 1<<16, 10, 1)

  val geometry = DiskGeometry (10, 6, 1<<20)

  private implicit class RichSegmentReleaser (releaser: Releaser) {

    def ptrs (ns: Seq [Int]): Seq [SegmentPointer] =
        ns map (n => SegmentPointer (null, geometry.segmentBounds (n)))

    def leaveAndExpect (epoch: Int) (fs: Int*): Unit =
      assertResult (fs) (releaser._leave (epoch) .map (_.num))

    def releaseAndExpect (ns: Int*) (fs: Int*): Unit =
      assertResult (fs) (releaser._release (ptrs (ns)) .map (_.num))
  }

  "The SegmentReleaser" should "free immediately when there are no parties" in {
    val releaser = new Releaser
    releaser.releaseAndExpect (0) (0)
  }

  it should "not free until the party leaves" in {
    val releaser = new Releaser
    val e1 = releaser.join()
    releaser.releaseAndExpect (0) ()
    releaser.leaveAndExpect (e1) (0)
  }

  it should "not free until previous epoch is freed" in {
    val releaser = new Releaser
    val e1 = releaser.join()
    releaser.releaseAndExpect (0) ()
    releaser.releaseAndExpect (1) ()
    releaser.leaveAndExpect (e1) (0, 1)
  }

  it should "not free until the previous epoch is free and the party leaves" in {
    val releaser = new Releaser
    val e1 = releaser.join()
    releaser.releaseAndExpect (0) ()
    val e2 = releaser.join()
    releaser.releaseAndExpect (1) ()
    releaser.leaveAndExpect (e1) (0)
    releaser.leaveAndExpect (e2) (1)
  }

  it should "not free until the parties leave the previous epoch is free" in {
    val releaser = new Releaser
    val e1 = releaser.join()
    releaser.releaseAndExpect (0) ()
    val e2 = releaser.join()
    releaser.releaseAndExpect (1) ()
    releaser.leaveAndExpect (e2) ()
    releaser.leaveAndExpect (e1) (0, 1)
  }}
