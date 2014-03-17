package com.treode.disk

import org.scalatest.FlatSpec

class DiskGeometrySpec extends FlatSpec {

  implicit val config = DisksConfig (14, 1<<24, 1<<16, 10, 1)

  val block = 1<<12
  val seg = 1<<16

  def assertBounds (id: Int, pos: Long, limit: Long) (actual: SegmentBounds): Unit =
    assertResult (SegmentBounds (id, pos, limit)) (actual)

  "DiskGeometry" should "compute the segment count" in {
    val disk1 = 1<<20
    val disk2 = 1<<21
    def c (diskBytes: Long) = DiskGeometry (16, 12, diskBytes).segmentCount
    assertResult (16) (c (disk1))
    assertResult (17) (c (disk1 + 4*block))
    assertResult (32) (c (disk2))
    assertResult (32) (c (disk2 - seg + 4*block))
    assertResult (31) (c (disk2 - seg + 4*block - 1))
    assertResult (32) (c (disk2 + 4*block - 1))
    assertResult (33) (c (disk2 + 4*block))
  }

  it should "align block length" in {
    val c = DiskGeometry (16, 12, 1<<20)
    assertResult (0) (c.blockAlignLength (0))
    assertResult (block) (c.blockAlignLength (1))
    assertResult (block) (c.blockAlignLength (4095))
    assertResult (block) (c.blockAlignLength (4096))
    assertResult (2*block) (c.blockAlignLength (4097))
  }

  it should "compute the segment bounds" in {
    val c = DiskGeometry (16, 12, (1<<20) + 6*block)
    assertBounds (0, config.diskLeadBytes, seg) (c.segmentBounds (0))
    assertBounds (1, seg, 2*seg) (c.segmentBounds (1))
    assertBounds (2, 2*seg, 3*seg) (c.segmentBounds (2))
    assertBounds (2, 2*seg, 3*seg) (c.segmentBounds (2))
    assertBounds (16, 16*seg, 16*seg + 6*block) (c.segmentBounds (16))
  }}
