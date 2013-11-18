package com.treode.store.cluster.atomic

import com.treode.store.TxClock

private sealed abstract class AtomicResponse

private object AtomicResponse {

  case class Prepared (ft: TxClock) extends AtomicResponse
  case class Collisions (ks: Set [Int]) extends AtomicResponse
  case object Advance extends AtomicResponse
  case object Committed extends AtomicResponse
  case object Aborted extends AtomicResponse
  case object Failed extends AtomicResponse

  val pickle = {
    import AtomicPicklers._
    tagged [AtomicResponse] (
        0x1 -> wrap1 (txClock) (Prepared.apply _) (_.ft),
        0x2 -> wrap1 (set (int)) (Collisions.apply _) (_.ks),
        0x3 -> const (Advance),
        0x4 -> const (Committed),
        0x5 -> const (Aborted),
        0x6 -> const (Failed))
  }}
