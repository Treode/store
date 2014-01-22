package com.treode.store.atomic

import com.treode.store.{TxClock, WriteOp}

private sealed abstract class WriteStatus

private object WriteStatus {

  case class Prepared (ft: TxClock, ops: Seq [WriteOp]) extends WriteStatus

  case object Committed extends WriteStatus

  case object Aborted extends WriteStatus

  val pickle = {
    import AtomicPicklers._
    tagged [WriteStatus] (
        0x1 -> wrap (txClock, seq (writeOp))
            .build ((Prepared.apply _).tupled)
            .inspect (v => (v.ft, v.ops)),
        0x2 -> const (Committed),
        0x3 -> const (Aborted))
  }}
