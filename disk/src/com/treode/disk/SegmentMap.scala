package com.treode.disk

private class SegmentMap (
    private val m: Map [PickledPageHandler, Long],
    val byteSize: Int) {

  def add (t: PickledPageHandler, byteSize: Long): SegmentMap = {
    m.get (t) match {
      case Some (s) =>
        new SegmentMap (
            m + (t -> (s + byteSize)),
            this.byteSize)
      case None =>
        new SegmentMap (
            m + (t -> byteSize),
            this.byteSize + t.tag.byteSize + SegmentMap.longByteSize)
    }}}

private object SegmentMap {

  val emptyByteSize = 5
  val longByteSize = 9

  val empty = new SegmentMap (Map.empty, emptyByteSize)

  def pickler (pages: PageRegistry) = {
    import DiskPicklers._
    wrap (map (pages.pickler, long), int)
    .build (v => new SegmentMap (v._1, v._2))
    .inspect (v => (v.m, v.byteSize))
  }}
