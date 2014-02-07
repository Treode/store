package com.treode.disk

private class SegmentAllocator private (config: DiskDriveConfig, private var _free: IntSet) {

  def alloc (num: Int): SegmentBounds = {
    _free = _free.remove (num)
    config.segmentBounds (num)
  }

  def alloc(): SegmentBounds = {
    val iter = _free.iterator
    if (!iter.hasNext)
      throw new DiskFullException
    alloc (iter.next())
  }

  def allocated: IntSet =
    _free.complement

  def free (nums: Seq [Int]) {
    val _nums = IntSet (nums: _*)
    _free = _free.add (_nums)
  }

  def free: IntSet =
    _free.clone()
}

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
