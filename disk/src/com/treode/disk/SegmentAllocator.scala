package com.treode.disk

private class SegmentAllocator private (config: DiskDriveConfig, private var _free: IntSet) {

  def allocSeg (num: Int): SegmentBounds = {
    _free = _free.remove (num)
    config.segmentBounds (num)
  }

  def allocPos (pos: Long): SegmentBounds = {
    allocSeg ((pos >> config.segmentBits).toInt)
  }

  def allocate(): SegmentBounds = {
    _free.min match {
      case Some (num) =>
        _free = _free.remove (num)
        allocSeg (num)
      case None =>
        throw new DiskFullException
    }}

  def allocated: IntSet = {
    _free.complement
  }

  def free: IntSet = {
    _free.clone()
  }

  def free (nums: Seq [Int]) {
    val _nums = IntSet (nums: _*)
    _free = _free.add (_nums)
  }}

private object SegmentAllocator {

  def init (config: DiskDriveConfig): SegmentAllocator = {
    val all = IntSet.fill (config.segmentCount)
    val superbs = IntSet.fill (DiskLeadBytes >> config.segmentBits)
    val free = all.remove (superbs)
    new SegmentAllocator (config, free)
  }

  def recover (config: DiskDriveConfig, free: IntSet): SegmentAllocator =
    new SegmentAllocator (config, free)
}
