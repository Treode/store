package com.treode.store.tier

import com.treode.store.{Bytes, TxClock}

private [store] class Cell (val key: Bytes, val time: TxClock, val value: Option [Bytes])
extends Ordered [Cell] {

  def byteSize: Int = {
    val n = key.byteSize + time.byteSize
    value match {
      case Some (v) => n + v.byteSize
      case None     => n
    }}

  def compare (that: Cell): Int = {
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
      case that: Cell =>
        this.key == that.key && this.time == that.time
      case _ => false
    }

  override def toString = "Cell" + (key, time, value)
}

private [store] object Cell extends Ordering [Cell] {

  def apply (key: Bytes, vt: TxClock, value: Option [Bytes]): Cell =
    new Cell (key, vt, value)

  def compare (x: Cell, y: Cell): Int =
    x compare y
}
