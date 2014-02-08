package com.treode.disk

class DiskDriveConfig private (
    val segmentBits: Int,
    val blockBits: Int,
    val diskBytes: Long) {

  val segmentBytes = 1 << segmentBits
  val segmentMask = ~(segmentBytes - 1)

  val blockBytes = 1 << blockBits
  val blockMask = ~(blockBytes - 1)

  val segmentCount = ((diskBytes + segmentBytes - (blockBytes<<2)) >> segmentBits).toInt

  def blockAlignLength (length: Int): Int =
    (length + blockBytes - 1) & blockMask

  private [disk] def segmentBounds (num: Int): SegmentBounds = {
    require (0 <= num && num < segmentCount)
    val pos = if (num == 0) DiskLeadBytes else num << segmentBits
    val end = (num + 1) << segmentBits
    val limit = if (end > diskBytes) diskBytes else end
    SegmentBounds (num, pos, limit)
  }

  override def hashCode: Int =
    (segmentBits, blockBits, diskBytes).hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: DiskDriveConfig =>
        segmentBits == that.segmentBits &&
        blockBits == that.blockBits &&
        diskBytes == that.diskBytes
      case _ =>
        false
    }

  override def toString = s"DiskDriveConfig($segmentBits, $blockBits, $diskBytes)"
}

object DiskDriveConfig {

  def apply (
      segmentBits: Int,
      blockBits: Int,
      diskBytes: Long): DiskDriveConfig = {

    require (segmentBits > 0, "A segment must have more than 0 bytes")
    require (blockBits > 0, "A block must have more than 0 bytes")
    require (diskBytes > 0, "A disk must have more than 0 bytes")
    require (segmentBits >= blockBits+2, "A segment must have at least 4 blocks")
    require (diskBytes >= (1<<(segmentBits+4)), "A disk must have at least 16 segments")

    new DiskDriveConfig (
        segmentBits,
        blockBits,
        diskBytes)
  }

  val pickler = {
    import DiskPicklers._
    wrap (uint, uint, ulong)
    .build ((apply _).tupled)
    .inspect (v => (v.segmentBits, v.blockBits, v.diskBytes))
  }}
