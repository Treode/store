package com.treode.store

import org.joda.time.Instant

case class TxId (id: Bytes, time: Instant) {

  override def toString = f"TxId:$id:0x${time.getMillis}%X"
}

object TxId {

  def apply (id: Long, time: Long): TxId =
    new TxId (Bytes (id), new Instant (time))

  val pickler = {
    import StorePicklers._
    wrap (bytes, instant)
    .build (v => new TxId (v._1, v._2))
    .inspect (v => (v.id, v.time))
  }}
