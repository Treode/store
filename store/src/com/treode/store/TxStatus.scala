package com.treode.store

sealed abstract class TxStatus

object TxStatus {

  case object Aborted extends TxStatus

  case class Committed (wt: TxClock) extends TxStatus

  val pickler = {
    import StorePicklers._
    tagged [TxStatus] (
        0x1 -> const (Aborted),
        0x2 -> wrap (txClock) .build (Committed.apply _) .inspect (_.wt))
  }}
