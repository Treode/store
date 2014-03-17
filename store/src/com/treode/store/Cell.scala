package com.treode.store

case class Cell (val key: Bytes, val time: TxClock, val value: Option [Bytes])
extends Ordered [Cell] {

  def byteSize = Cell.pickler.byteSize (this)

  def compare (that: Cell): Int = {
    var r = key compare that.key
    if (r != 0) return r
    // Reverse chronological order
    r = that.time compare time
    if (r != 0) return r
    value compare that.value
  }

  override def toString = "Cell" + (key, time, value)
}

object Cell extends Ordering [Cell] {

  def compare (x: Cell, y: Cell): Int =
    x compare y

  val pickler = {
    import StorePicklers._
    wrap (bytes, txClock, option (bytes))
    .build ((apply _).tupled)
    .inspect (v => (v.key, v.time, v.value))
  }}
