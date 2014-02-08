package com.treode.store.atomic

import com.treode.store.TxClock

private sealed abstract class WriteResponse

private object WriteResponse {

  case class Prepared (ft: TxClock) extends WriteResponse
  case class Collisions (ks: Set [Int]) extends WriteResponse
  case object Advance extends WriteResponse
  case object Committed extends WriteResponse
  case object Aborted extends WriteResponse
  case object Failed extends WriteResponse

  val pickler = {
    import AtomicPicklers._
    tagged [WriteResponse] (
        0x1 -> wrap (txClock) .build (Prepared.apply _) .inspect (_.ft),
        0x2 -> wrap (set (uint)) .build (Collisions.apply _) .inspect (_.ks),
        0x3 -> const (Advance),
        0x4 -> const (Committed),
        0x5 -> const (Aborted),
        0x6 -> const (Failed))
  }}
