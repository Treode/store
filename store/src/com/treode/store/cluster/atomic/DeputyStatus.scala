package com.treode.store.cluster.atomic

import com.treode.store.{WriteBatch, WriteOp}

private sealed abstract class DeputyStatus

private object DeputyStatus {

  case class Prepared (batch: WriteBatch) extends DeputyStatus

  case object Committed extends DeputyStatus

  case object Aborted extends DeputyStatus

  val pickle = {
    import AtomicPicklers._
    tagged [DeputyStatus] (
        0x1 -> wrap1 (writeBatch) (Prepared.apply _) (_.batch),
        0x2 -> const (Committed),
        0x3 -> const (Aborted))
  }}
