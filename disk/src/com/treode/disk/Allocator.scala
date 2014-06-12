package com.treode.disk

import scala.collection.mutable.ArrayBuffer

import Allocator.segmentBounds

private class Allocator private (private var _free: IntSet) {

  def alloc (geometry: DiskGeometry, config: DiskConfig): SegmentBounds = {
    val iter = _free.iterator
    if (!iter.hasNext)
      throw new DiskFullException
    val num = iter.next
    _free = _free.remove (num)
    segmentBounds (num, geometry, config)
  }

  def alloc (num: Int, geometry: DiskGeometry, config: DiskConfig): SegmentBounds = {
    _free = _free.remove (num)
    segmentBounds (num, geometry, config)
  }

  def free (nums: IntSet): Unit =
    _free = _free.add (nums)

  def free: IntSet = _free

  def cleanable (ignore: IntSet): IntSet =
    free.complement.remove (ignore)

  def drained (ignore: IntSet): Boolean = {
    val alloc = free.complement.remove (ignore)
    alloc.size == 0
  }}

private object Allocator {

  def segmentBounds (num: Int, geometry: DiskGeometry, config: DiskConfig): SegmentBounds = {
    require (0 <= num && num < geometry.segmentCount)
    val pos = if (num == 0) config.diskLeadBytes else num.toLong << geometry.segmentBits
    val end = (num.toLong + 1) << geometry.segmentBits
    val limit = if (end > geometry.diskBytes) geometry.diskBytes else end
    SegmentBounds (num, pos, limit)
  }

  def apply (free: IntSet): Allocator =
    new Allocator (free)

  def apply (geometry: DiskGeometry, config: DiskConfig): Allocator = {
    val all = IntSet.fill (geometry.segmentCount)
    val superbs = IntSet.fill (config.diskLeadBytes >> geometry.segmentBits)
    new Allocator (all.remove (superbs))
  }}
