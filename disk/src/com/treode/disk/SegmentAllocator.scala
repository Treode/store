package com.treode.disk

private class SegmentAllocator private (config: DiskDriveConfig, private var _free: IntSet) {

  def allocSeg (num: Int): SegmentBounds = synchronized {
    _free = _free.remove (num)
    config.segmentBounds (num)
  }

  def allocPos (pos: Long): SegmentBounds = synchronized {
    allocSeg ((pos >> config.segmentBits).toInt)
  }

  def allocate(): SegmentBounds = synchronized {
    _free.min match {
      case Some (num) =>
        _free = _free.remove (num)
        allocSeg (num)
      case None =>
        throw new DiskFullException
    }}

  def allocated: IntSet = synchronized {
    _free.complement
  }

  def free: IntSet =
    _free

  def free (nums: Seq [Int]) {
    val _nums = IntSet (nums: _*)
    synchronized {
      _free = _free.add (_nums)
    }}

  def checkpoint (gen: Int): SegmentAllocator.Meta =
    SegmentAllocator.Meta (_free)
}

private object SegmentAllocator {

  case class Meta (free: IntSet)

  object Meta {

    val pickler = {
      import DiskPicklers._
      val intset = IntSet.pickle
      wrap (intset) build (Meta.apply _) inspect (_.free)
    }}

  def init (config: DiskDriveConfig): SegmentAllocator = {
    val all = IntSet.fill (config.segmentCount)
    val superbs = IntSet.fill (DiskLeadBytes >> config.segmentBits)
    val free = all.remove (superbs)
    new SegmentAllocator (config, free)
  }

  def recover (config: DiskDriveConfig, meta: Meta): SegmentAllocator =
    new SegmentAllocator (config, meta.free)
}
