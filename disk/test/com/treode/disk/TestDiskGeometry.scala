package com.treode.disk

object TestDiskGeometry {

  def apply (
      segmentBits: Int = 12,
      blockBits: Int = 6,
      diskBytes: Long = 1<<20
  ) (implicit
      config: DiskConfig
   ): DiskGeometry =
     DiskGeometry (
         segmentBits,
         blockBits,
         diskBytes)
}
