package com.treode.disk

class DiskTestConfig (
    superBlockBits: Int,
    maximumRecordBytes: Int,
    maximumPageBytes: Int,
    checkpointBytes: Int,
    checkpointEntries: Int,
    cleaningFrequency: Int,
    cleaningLoad: Int
) extends DiskConfig (
    superBlockBits,
    maximumRecordBytes,
    maximumPageBytes,
    checkpointBytes,
    checkpointEntries,
    cleaningFrequency,
    cleaningLoad)

object DiskTestConfig {

  def apply (
      superBlockBits: Int = 8,
      maximumRecordBytes: Int = 1<<10,
      maximumPageBytes: Int = 1<<10,
      checkpointBytes: Int = Int.MaxValue,
      checkpointEntries: Int = Int.MaxValue,
      cleaningFrequency: Int = Int.MaxValue,
      cleaningLoad: Int = 1
  ): DiskConfig =
    DiskConfig (
        superBlockBits,
        maximumRecordBytes,
        maximumPageBytes,
        checkpointBytes,
        checkpointEntries,
        cleaningFrequency,
        cleaningLoad)
}
