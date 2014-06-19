package com.treode.disk

object DiskTestConfig {

  def apply (
      checkpointBytes: Int = Int.MaxValue,
      checkpointEntries: Int = Int.MaxValue,
      cleaningFrequency: Int = Int.MaxValue,
      cleaningLoad: Int = 1,
      maximumRecordBytes: Int = 1<<10,
      maximumPageBytes: Int = 1<<10,
      pageCacheEntries: Int = 100,
      superBlockBits: Int = 8
  ): Disk.Config =
    Disk.Config (
        checkpointBytes = checkpointBytes,
        checkpointEntries = checkpointEntries,
        cleaningFrequency = cleaningFrequency,
        cleaningLoad = cleaningLoad,
        maximumRecordBytes = maximumRecordBytes,
        maximumPageBytes = maximumPageBytes,
        pageCacheEntries = pageCacheEntries,
        superBlockBits = superBlockBits)
}
