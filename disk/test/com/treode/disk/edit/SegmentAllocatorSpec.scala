package com.treode.disk.edit

import scala.collection.JavaConversions._

import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.disk.{DiskTestConfig, DriveGeometry}
import org.scalatest.FlatSpec

import SegmentAllocator.{bitmapOf, bitmapRange}

class SegmentAllocatorSpec extends FlatSpec {

  // The SegmentAllocator is mostly a veneer over EWAHCompressedBitmap. We do a few basic tests
  // to make sure the facade is correct, but the complex logic is truly in the bitmap.

  implicit val config = DiskTestConfig()
  implicit val geom = DriveGeometry (8, 6, 1 << 14)

  "bitmapOf (Int)" should "make a bitmap with one bit set" in {
    assertResult (Seq (3)) (bitmapOf (3) .toSeq)
  }

  "bitmapOf (Int*)" should "make a bitmap with some bits set" in {
    assertResult (Seq.empty) (bitmapOf (Set.empty [Int]) .toSeq)
    assertResult (Seq (1, 2, 3)) (bitmapOf (Set (1, 2, 3)) .toSeq)
  }

  "bitmapRange" should "make a bitmap with some bits set" in {
    assertResult (Seq.empty) (bitmapRange (4, 4) .toSeq)
    assertResult (Seq (4, 5, 6)) (bitmapRange (4, 7) .toSeq)
  }

  "SegmentAllocator" should "allocate a new segment and free it" in {
    implicit val scheduler = StubScheduler.random()
    val alloc = new SegmentAllocator (geom)
    assert (alloc.allocated == 0)
    val seg = alloc.alloc()
    assert (alloc.allocated == 1)
    alloc.free (seg.num)
    assert (alloc.allocated == 0)
  }

  it should "allocate a given segment and free it" in {
    implicit val scheduler = StubScheduler.random()
    val alloc = new SegmentAllocator (geom)
    assert (alloc.allocated == 0)
    val seg = alloc.alloc (1)
    assert (seg.num == 1)
    assert (alloc.allocated == 1)
    alloc.free (1)
    assert (alloc.allocated == 0)
  }

  it should "allocate given segments and free some" in {
    implicit val scheduler = StubScheduler.random()
    val alloc = new SegmentAllocator (geom)
    assert (alloc.allocated == 0)
    alloc.alloc (Set (1, 2, 3))
    assert (alloc.allocated == 3)
    alloc.free (Set (2, 3, 4))
    assert (alloc.allocated == 1)
  }

  it should "invoke the drainer immediately when every segment is already free" in {
    implicit val scheduler = StubScheduler.random()
    val alloc = new SegmentAllocator (geom)
    alloc.awaitDrained().expectPass()
  }

  it should "invoke the drainer later when the final segment becomes free" in {
    implicit val scheduler = StubScheduler.random()
    val alloc = new SegmentAllocator (geom)
    alloc.alloc (Set (1, 2))
    val cb = alloc.awaitDrained().capture()
    scheduler.run()
    cb.assertNotInvoked()
    alloc.free (1)
    scheduler.run()
    cb.assertNotInvoked()
    alloc.free (2)
    scheduler.run()
    cb.assertInvoked()
  }

  it should "invoke the drainer later when the final set of segments becomes free" in {
    implicit val scheduler = StubScheduler.random()
    val alloc = new SegmentAllocator (geom)
    alloc.alloc (Set (1, 2, 3, 4))
    val cb = alloc.awaitDrained().capture()
    scheduler.run()
    cb.assertNotInvoked()
    alloc.free (Set (1, 2))
    scheduler.run()
    cb.assertNotInvoked()
    alloc.free (Set (3, 4))
    scheduler.run()
    cb.assertInvoked()
  }}
