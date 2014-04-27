package com.treode.store

case class Cell (key: Bytes, time: TxClock, value: Option [Bytes]) extends Ordered [Cell] {

  def byteSize = Cell.pickler.byteSize (this)

  def timedKey: Key = Key (key, time)

  def timedValue: Value = Value (time, value)

  def compare (that: Cell): Int = {
    val r = key compare that.key
    if (r != 0) return r
    // Reverse chronological order
    that.time compare time
  }}

object Cell extends Ordering [Cell] {

  val sentinel = Cell (Bytes.empty, TxClock.sentinel, None)

  def compare (x: Cell, y: Cell): Int =
    x compare y

  val pickler = {
    import StorePicklers._
    wrap (bytes, txClock, option (bytes))
    .build ((apply _).tupled)
    .inspect (v => (v.key, v.time, v.value))
  }}
