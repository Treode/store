package com.treode.store.disk2

import java.lang.{Long => JLong}
import java.util.Objects
import com.treode.pickle.Picklers

class DiskConfig private [disk2] (
    val segmentBits: Int,
    val pageBits: Int,
    val blockBits: Int,
    val diskBytes: Long) {

  val segmentBytes = 1 << segmentBits
  val segmentMask = segmentBytes - 1
  val segmentCount = ((diskBytes + segmentMask.toLong) >> segmentBits).toInt

  private [disk2] def segment (num: Int): Segment = {
    require (0 <= num && num < segmentCount)
    val pos = if (num == 0) DiskLeadBytes else num << segmentBits
    val end = (num + 1) << segmentBits
    val limit = if (end > diskBytes) diskBytes else end
    Segment (num, pos, limit)
  }

  override def hashCode: Int =
    Objects.hash (segmentBits: JLong, pageBits: JLong, blockBits: JLong, diskBytes: JLong)

  override def equals (other: Any): Boolean =
    other match {
      case that: DiskConfig =>
        segmentBits == that.segmentBits &&
        pageBits == that.pageBits &&
        blockBits == that.blockBits &&
        diskBytes == that.diskBytes
      case _ =>
        false
    }

  override def toString = s"DiskConfig($segmentBits, $pageBits, $blockBits, $diskBytes)"
}

object DiskConfig {

  def apply (
      segmentBits: Int,
      pageBits: Int,
      blockBits: Int,
      diskBytes: Long): DiskConfig = {

    require (segmentBits > SuperBlockBits, s"segmentBits must be greater than $SuperBlockBits")
    require (pageBits > 0, "pageBits must be greater than 0")
    require (pageBits < segmentBits, "pageBits must be less than segmentBits")
    require (blockBits > 0, "blockBits must be greater than 0")
    require (blockBits < pageBits, "blockBits must be less than pageBits")
    require (diskBytes > 0, "diskBytes must be greater than zero")

    val maxSegmentBits = diskBytes >> (segmentBits + 2)
    require (segmentBits < maxSegmentBits,
        s"segmentBits must allow at least four segments on the disk (max $maxSegmentBits for $diskBytes bytes)")

    new DiskConfig (
        segmentBits,
        pageBits,
        blockBits,
        diskBytes)
  }

  val pickle = {
    import Picklers._
    wrap4 (int, int, int, long) {
      (a, b, c, d) => new DiskConfig (a, b, c, d)
    } {
      v => (v.segmentBits, v.pageBits, v.blockBits, v.diskBytes)
    }}}
