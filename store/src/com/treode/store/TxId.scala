package com.treode.store

import org.joda.time.Instant

case class TxId (id: Bytes, time: Instant) extends Ordered [TxId] {

  def compare (that: TxId): Int = {
    val r = id compare that.id
    if (r != 0) return r
    time.getMillis compare that.time.getMillis
  }

  override def toString = f"TxId:$id:0x${time.getMillis}%X"
}

object TxId extends Ordering [TxId] {

  val MinValue = TxId (Bytes.MinValue, 0)

  def apply (id: Bytes, time: Long): TxId =
    TxId (id, new Instant (time))

  def compare (x: TxId, y: TxId): Int =
    x compare y

  val pickler = {
    import StorePicklers._
    wrap (bytes, instant)
    .build (v => new TxId (v._1, v._2))
    .inspect (v => (v.id, v.time))
  }}
