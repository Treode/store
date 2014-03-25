package com.treode.disk

import scala.collection.mutable.ArrayBuffer

import Allocator.segmentBounds

private class Allocator private (private var _free: IntSet) {

  def alloc (geometry: DiskGeometry, config: DisksConfig): SegmentBounds = {
    val iter = _free.iterator
    if (!iter.hasNext)
      throw new DiskFullException
    val num = iter.next
    _free = _free.remove (num)
    segmentBounds (num, geometry, config)
  }

  def alloc (num: Int, geometry: DiskGeometry, config: DisksConfig): SegmentBounds = {
    _free = _free.remove (num)
    segmentBounds (num, geometry, config)
  }

  def free (nums: IntSet): Unit =
    free.add (nums)

  def free: IntSet = _free

  def cleanable (protect: IntSet): Iterator [Int] =
    free.complement.remove (protect) .iterator

  def drained (ignore: Seq [Int]): Boolean = {
    val alloc = free.complement.remove (IntSet (ignore.sorted: _*))
    alloc.size == 0
  }}

private object Allocator {

  def segmentBounds (num: Int, geometry: DiskGeometry, config: DisksConfig): SegmentBounds = {
    require (0 <= num && num < geometry.segmentCount)
    val pos = if (num == 0) config.diskLeadBytes else num << geometry.segmentBits
    val end = (num + 1) << geometry.segmentBits
    val limit = if (end > geometry.diskBytes) geometry.diskBytes else end
    SegmentBounds (num, pos, limit)
  }

  def apply (free: IntSet): Allocator =
    new Allocator (free)

  def apply (geometry: DiskGeometry, config: DisksConfig): Allocator = {
    val all = IntSet.fill (geometry.segmentCount)
    val superbs = IntSet.fill (config.diskLeadBytes >> geometry.segmentBits)
    new Allocator (all.remove (superbs))
  }}
