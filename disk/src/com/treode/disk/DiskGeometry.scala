package com.treode.disk

class DiskGeometry private (
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

  private [disk] def segmentNum (pos: Long): Int =
    (pos >> segmentBits) .toInt

  private [disk] def segmentBounds (num: Int) (implicit config: DisksConfig): SegmentBounds = {
    require (0 <= num && num < segmentCount)
    val pos = if (num == 0) config.diskLeadBytes else num << segmentBits
    val end = (num + 1) << segmentBits
    val limit = if (end > diskBytes) diskBytes else end
    SegmentBounds (num, pos, limit)
  }

  override def hashCode: Int =
    (segmentBits, blockBits, diskBytes).hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: DiskGeometry =>
        segmentBits == that.segmentBits &&
        blockBits == that.blockBits &&
        diskBytes == that.diskBytes
      case _ =>
        false
    }

  override def toString: String =
    s"DiskGeometry($segmentBits, $blockBits, $diskBytes)"
}

object DiskGeometry {

  def apply (
      segmentBits: Int,
      blockBits: Int,
      diskBytes: Long
  ) (implicit
      config: DisksConfig
  ): DiskGeometry = {

    require (
        segmentBits > 0,
        "A segment must have more than 0 bytes.")
    require (
        blockBits > 0,
        "A block must have more than 0 bytes.")
    require (
        diskBytes > 0,
        "A disk must have more than 0 bytes.")
    require (
        segmentBits >= blockBits,
        "A segment must be at least one block.")
    require (
        blockBits <= config.superBlockBits,
        "A superblock must be at least one disk block.")
    require (
        segmentBits >= config.minimumSegmentBits,
        "A segment must be larger than the largest record or page.")

    val free = (diskBytes >> segmentBits) - (config.diskLeadBytes >> segmentBits)
    require (free >= 16, "A disk must have at least 16 segments")

    new DiskGeometry (
        segmentBits,
        blockBits,
        diskBytes)
  }

  def standard (
      segmentBits: Int = 30,
      blockBits: Int = 13,
      diskBytes: Long = -1
  ) (implicit
      config: DisksConfig
   ): DiskGeometry =
     DiskGeometry (
         segmentBits,
         blockBits,
         diskBytes)

  val pickler = {
    import DiskPicklers._
    wrap (uint, uint, ulong)
    .build { v =>
      val (s, b, d) = v
      require (s > 0 && b > 0 && d > 0)
      new DiskGeometry (s, b, d)
    }
    .inspect (v => (v.segmentBits, v.blockBits, v.diskBytes))
  }}
