package com.treode.disk

private class SegmentAllocator private (geometry: DiskGeometry, private var _free: IntSet) (
        implicit config: DisksConfig) {

  def alloc (num: Int): SegmentBounds = {
    _free = _free.remove (num)
    geometry.segmentBounds (num)
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

  def init (geometry: DiskGeometry) (implicit config: DisksConfig): SegmentAllocator = {
    val all = IntSet.fill (geometry.segmentCount)
    val superbs = IntSet.fill (config.diskLeadBytes >> geometry.segmentBits)
    val free = all.remove (superbs)
    new SegmentAllocator (geometry, free)
  }

  def recover (geometry: DiskGeometry, free: IntSet) (implicit config: DisksConfig) : SegmentAllocator =
    new SegmentAllocator (geometry, free)
}
