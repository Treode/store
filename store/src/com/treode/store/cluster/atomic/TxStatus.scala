package com.treode.store.cluster.atomic

import com.treode.store.{PaxosAccessor, TxClock}

sealed abstract class TxStatus

object TxStatus {

  case object Aborted extends TxStatus

  case class Committed (wt: TxClock) extends TxStatus

  val pickle = {
    import AtomicPicklers._
    tagged [TxStatus] (
        0x1 -> const (Aborted),
        0x2 -> wrap (txClock) .build (Committed.apply _) .inspect (_.wt))
  }}
