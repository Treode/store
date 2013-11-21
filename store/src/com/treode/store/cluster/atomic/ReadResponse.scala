package com.treode.store.cluster.atomic

import com.treode.store.{Bytes, TxClock, Value}

private sealed abstract class ReadResponse

private object ReadResponse {

  case class Got (vs: Seq [Value]) extends ReadResponse
  case object Failed extends ReadResponse

  val pickle = {
    import AtomicPicklers._
    tagged [ReadResponse] (
        0x1 -> wrap1 (seq (value)) (Got.apply _) (_.vs),
        0x2 -> const (Failed))
  }}
