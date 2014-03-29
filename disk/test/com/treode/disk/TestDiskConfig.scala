package com.treode.disk

object TestDisksConfig {

  def apply (
      cell: CellId = 0,
      superBlockBits: Int = 8,
      maximumRecordBytes: Int = 1<<8,
      maximumPageBytes: Int = 1<<8,
      checkpointBytes: Int = Int.MaxValue,
      checkpointEntries: Int = Int.MaxValue,
      cleaningFrequency: Int = Int.MaxValue,
      cleaningLoad: Int = 1
  ): DisksConfig =
    DisksConfig (
        cell: CellId,
        superBlockBits,
        maximumRecordBytes,
        maximumPageBytes,
        checkpointBytes,
        checkpointEntries,
        cleaningFrequency,
        cleaningLoad)
}
