package com.treode.store

private case class Key (table: TableId, key: Bytes, time: TxClock) extends Ordered [Key] {

  def compare (that: Key): Int = {
    var r = table compare that.table
    if (r != 0) return r
    r = key compare that.key
    if (r != 0) return r
    // Reverse chronological order.
    that.time compare time
  }}

private object Key extends Ordering [Key] {

  def compare (x: Key, y: Key): Int =
    x compare y
}
