package com.treode.disk

object TestDiskGeometry {

  def apply (
      segmentBits: Int = 10,
      blockBits: Int = 6,
      diskBytes: Long = 1<<20
  ) (implicit
      config: DisksConfig
   ): DiskGeometry =
     DiskGeometry (
         segmentBits,
         blockBits,
         diskBytes)
}
