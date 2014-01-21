package com.treode.disk

import com.treode.pickle.Picklers

case class Position (disk: Int, offset: Long, length: Int)

object Position {

  val pickle = {
    import Picklers._
    wrap (int, long, int)
    .build ((Position.apply _).tupled)
    .inspect (v => (v.disk, v.offset, v.length))
  }}
