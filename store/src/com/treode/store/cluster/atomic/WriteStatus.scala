package com.treode.store.cluster.atomic

import com.treode.store.{TxClock, WriteOp}

private sealed abstract class WriteStatus

private object WriteStatus {

  case class Prepared (ft: TxClock, ops: Seq [WriteOp]) extends WriteStatus

  case object Committed extends WriteStatus

  case object Aborted extends WriteStatus

  val pickle = {
    import AtomicPicklers._
    tagged [WriteStatus] (
        0x1 -> wrap2 (txClock, seq (writeOp)) (Prepared.apply _) (v => (v.ft, v.ops)),
        0x2 -> const (Committed),
        0x3 -> const (Aborted))
  }}
