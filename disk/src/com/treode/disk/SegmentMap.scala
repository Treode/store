package com.treode.disk

import com.treode.pickle.Pickler

private class SegmentMap (
    private val m: Map [TagRegistry.Tagger, Long],
    val byteSize: Int) {

  def add (t: TagRegistry.Tagger, byteSize: Long): SegmentMap = {
    m.get (t) match {
      case Some (s) =>
        new SegmentMap (
            m + (t -> (s + byteSize)),
            this.byteSize)
      case None =>
        new SegmentMap (
            m + (t -> byteSize),
            this.byteSize + t.byteSize + SegmentMap.longByteSize)
    }}}

private object SegmentMap {

  val emptyByteSize = 5
  val longByteSize = 9

  val empty = new SegmentMap (Map.empty, emptyByteSize)

  def pickler = {
    import DiskPicklers._
    wrap (map (TagRegistry.pickler, long), int)
    .build [SegmentMap] (_ => throw new UnsupportedOperationException)
    .inspect (v => (v.m, v.byteSize))
  }

  def unpickler (registry: TagRegistry [PickledPageHandler]) = {
    import DiskPicklers._

    val retag = wrap (registry.unpickler)
    .build (_.retag)
    .inspect (_ => throw new UnsupportedOperationException)

    wrap (map (retag, long), int)
    .build (v => new SegmentMap (v._1, v._2))
    .inspect (_ => throw new UnsupportedOperationException)
  }}
