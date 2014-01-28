package com.treode.store.simple

import com.treode.disk.Position
import com.treode.store.StorePicklers

private case class Tier (gen: Long, pos: Position)

private object Tier {

  val pickler = {
    import StorePicklers._
    wrap (long, pos)
    .build ((Tier.apply _).tupled)
    .inspect (v => (v.gen, v.pos))
  }}
