package com.treode.store.atomic

import com.treode.store.{Bytes, TxClock, Value}

private sealed abstract class ReadResponse

private object ReadResponse {

  case class Got (vs: Seq [Value]) extends ReadResponse
  case object Failed extends ReadResponse

  val pickler = {
    import AtomicPicklers._
    tagged [ReadResponse] (
        0x1 -> wrap (seq (value)) .build (Got.apply _) .inspect (_.vs),
        0x2 -> const (Failed))
  }}
