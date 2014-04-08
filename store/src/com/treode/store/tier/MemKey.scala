package com.treode.store.tier

import com.treode.store.{Bytes, Cell, TxClock}

private case class MemKey (key: Bytes, time: TxClock) extends Ordered [MemKey] {

  def compare (that: MemKey): Int = {
    var r = key compare that.key
    if (r != 0) return r
    // Reverse chronological order
    that.time compare time
  }}

private object MemKey extends Ordering [MemKey] {

  def apply (cell: Cell): MemKey =
    new MemKey (cell.key, cell.time)

  def compare (x: MemKey, y: MemKey): Int =
    x compare y
}
