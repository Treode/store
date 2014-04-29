package com.treode.store.stubs

import com.treode.store.{Bytes, TableId, TxClock}

private case class StubKey (table: TableId, key: Bytes, time: TxClock)
extends Ordered [StubKey] {

  def compare (that: StubKey): Int = {
    var r = table compare that.table
    if (r != 0) return r
    r = key compare that.key
    if (r != 0) return r
    // Reverse chronological order.
    that.time compare time
  }}

private object StubKey extends Ordering [StubKey] {

  def compare (x: StubKey, y: StubKey): Int =
    x compare y
}
