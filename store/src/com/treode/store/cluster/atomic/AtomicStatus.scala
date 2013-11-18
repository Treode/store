package com.treode.store.cluster.atomic

import com.treode.store.{PaxosAccessor, TxClock}

sealed abstract class AtomicStatus

object AtomicStatus {

  case object Aborted extends AtomicStatus

  case class Committed (wt: TxClock) extends AtomicStatus

  val pickle = {
    import AtomicPicklers._
    tagged [AtomicStatus] (
        0x1 -> const (Aborted),
        0x2 -> wrap1 (txClock) (Committed.apply _) (_.wt))
  }}
