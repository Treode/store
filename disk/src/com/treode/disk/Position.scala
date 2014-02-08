package com.treode.disk

case class Position (disk: Int, offset: Long, length: Int)

object Position {

  val pickler = {
    import DiskPicklers._
    wrap (uint, ulong, uint)
    .build ((Position.apply _).tupled)
    .inspect (v => (v.disk, v.offset, v.length))
  }}
