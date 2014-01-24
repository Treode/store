package com.treode.disk

case class Position (disk: Int, offset: Long, length: Int)

object Position {

  val pickle = {
    import DiskPicklers._
    wrap (int, long, int)
    .build ((Position.apply _).tupled)
    .inspect (v => (v.disk, v.offset, v.length))
  }}
