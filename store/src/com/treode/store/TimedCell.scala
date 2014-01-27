package com.treode.store

private class TimedCell (val key: Bytes, val time: TxClock, val value: Option [Bytes])
extends Ordered [TimedCell] {

  def byteSize = TimedCell.pickler.byteSize (this)

  def compare (that: TimedCell): Int = {
    val rk = key compare that.key
    if (rk != 0)
      return rk
    // Reverse chronological order
    that.time compare time
  }

  override def hashCode: Int =
    41 * (key.hashCode + 41) + time.hashCode

  override def equals (other: Any) =
    other match {
      case that: TimedCell =>
        this.key == that.key && this.time == that.time
      case _ => false
    }

  override def toString = "Cell" + (key, time, value)
}

private object TimedCell extends Ordering [TimedCell] {

  def apply (key: Bytes, vt: TxClock, value: Option [Bytes]): TimedCell =
    new TimedCell (key, vt, value)

  def compare (x: TimedCell, y: TimedCell): Int =
    x compare y

  val pickler = {
    import StorePicklers._
    wrap (bytes, txClock, option (bytes))
    .build ((apply _).tupled)
    .inspect (v => (v.key, v.time, v.value))
  }}
