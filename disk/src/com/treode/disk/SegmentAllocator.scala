package com.treode.disk

private class SegmentAllocator (config: DiskDriveConfig) {

  var free = IntSet.fill (0)

  def allocSeg (num: Int): Segment = {
    free = free.remove (num)
    config.segment (num)
  }

  def allocPos (pos: Long): Segment =
    allocSeg ((pos >> config.segmentBits).toInt)

  def allocate(): Segment = {
    free.min match {
      case Some (num) =>
        free = free.remove (num)
        allocSeg (num)
      case None =>
        throw new DiskFullException
    }}

  def allocated: IntSet =
    free.complement

  def init() {
    free = IntSet.fill (config.segmentCount)
    val superblocks = IntSet.fill (DiskLeadBytes >> config.segmentBits)
    free = free.remove (superblocks)
  }

  def checkpoint (gen: Int): Allocator.Meta = {
    Allocator.Meta (free)
  }

  def recover (gen: Int, meta: Allocator.Meta) {
    free = meta.free
  }}

private object Allocator {

  case class Meta (free: IntSet)

  object Meta {

    val pickler = {
      import DiskPicklers._
      val intset = IntSet.pickle
      wrap (intset) build (Meta.apply _) inspect (_.free)
    }}
}
