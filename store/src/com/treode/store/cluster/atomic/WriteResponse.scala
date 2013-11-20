package com.treode.store.cluster.atomic

import com.treode.store.TxClock

private sealed abstract class WriteResponse

private object WriteResponse {

  case class Prepared (ft: TxClock) extends WriteResponse
  case class Collisions (ks: Set [Int]) extends WriteResponse
  case object Advance extends WriteResponse
  case object Committed extends WriteResponse
  case object Aborted extends WriteResponse
  case object Failed extends WriteResponse

  val pickle = {
    import AtomicPicklers._
    tagged [WriteResponse] (
        0x1 -> wrap1 (txClock) (Prepared.apply _) (_.ft),
        0x2 -> wrap1 (set (int)) (Collisions.apply _) (_.ks),
        0x3 -> const (Advance),
        0x4 -> const (Committed),
        0x5 -> const (Aborted),
        0x6 -> const (Failed))
  }}
