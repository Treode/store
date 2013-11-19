package com.treode.store.cluster.atomic

import com.treode.store.{TxClock, WriteOp}

private sealed abstract class DeputyStatus

private object DeputyStatus {

  case class Prepared (ft: TxClock, ops: Seq [WriteOp]) extends DeputyStatus

  case object Committed extends DeputyStatus

  case object Aborted extends DeputyStatus

  val pickle = {
    import AtomicPicklers._
    tagged [DeputyStatus] (
        0x1 -> wrap2 (txClock, seq (writeOp)) (Prepared.apply _) (v => (v.ft, v.ops)),
        0x2 -> const (Committed),
        0x3 -> const (Aborted))
  }}
