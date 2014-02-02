package com.treode.disk

private class SegmentAllocator (config: DiskDriveConfig) {

  private var _free = IntSet.fill (0)

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

  def init() {
    _free = IntSet.fill (config.segmentCount)
    val superblocks = IntSet.fill (DiskLeadBytes >> config.segmentBits)
    _free = _free.remove (superblocks)
  }

  def checkpoint (gen: Int): Allocator.Meta =
    Allocator.Meta (_free)

  def recover (gen: Int, meta: Allocator.Meta): Unit =
    _free = meta.free
}

private object Allocator {

  case class Meta (free: IntSet)

  object Meta {

    val pickler = {
      import DiskPicklers._
      val intset = IntSet.pickle
      wrap (intset) build (Meta.apply _) inspect (_.free)
    }}
}
