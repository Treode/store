package com.treode.store

private case class TimedCell (val key: Bytes, val time: TxClock, val value: Option [Bytes])
extends Ordered [TimedCell] {

  def byteSize = TimedCell.pickler.byteSize (this)

  def compare (that: TimedCell): Int = {
    var r = key compare that.key
    if (r != 0) return r
    // Reverse chronological order
    r = that.time compare time
    if (r != 0) return r
    value compare that.value
  }

  override def toString = "TimedCell" + (key, time, value)
}

private object TimedCell extends Ordering [TimedCell] {

  def compare (x: TimedCell, y: TimedCell): Int =
    x compare y

  val pickler = {
    import StorePicklers._
    wrap (bytes, txClock, option (bytes))
    .build ((apply _).tupled)
    .inspect (v => (v.key, v.time, v.value))
  }}
