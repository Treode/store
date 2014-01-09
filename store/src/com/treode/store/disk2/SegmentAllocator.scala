package com.treode.store.disk2

import scala.collection.JavaConversions._

import com.treode.pickle._

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

    val pickle = {
      import Picklers._
      val intset = IntSet.pickle
      wrap1 (intset) (Meta.apply _) (_.free)
    }}
}
