package com.treode.disk

private case class SegmentBounds (num: Int, base: Long, limit: Long) {

  def contains (pos: Long): Boolean =
    base <= pos && pos < limit
}
