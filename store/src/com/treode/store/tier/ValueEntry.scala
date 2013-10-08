package com.treode.store.tier

import com.treode.store.{Bytes, TxClock}

private class ValueEntry (val key: Bytes, val time: TxClock, val value: Option [Bytes])
extends Entry with Ordered [ValueEntry] {

  def byteSize: Int = {
    val n = key.byteSize + time.byteSize
    value match {
      case Some (v) => n + v.byteSize
      case None     => n
    }}

  def compare (that: ValueEntry): Int = {
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
      case that: ValueEntry =>
        this.key == that.key && this.time == that.time
      case _ => false
    }

  override def toString = "ValueEntry" + (key, time, value)
}

private object ValueEntry extends Ordering [ValueEntry] {

  def apply (key: Bytes, vt: TxClock, value: Option [Bytes]): ValueEntry =
    new ValueEntry (key, vt, value)

  def compare (x: ValueEntry, y: ValueEntry): Int =
    x compare y
}
