package com.treode.store

import com.treode.pickle.Pickler

case class Cell (key: Bytes, time: TxClock, value: Option [Bytes]) extends Ordered [Cell] {

  def byteSize = Cell.pickler.byteSize (this)

  def key [K] (p: Pickler [K]): K =
    key.unpickle (p)

  def value [V] (p: Pickler [V]): Option [V] =
    value map (_.unpickle (p))

  def timedKey: Key = Key (key, time)

  def timedValue: Value = Value (time, value)

  def compare (that: Cell): Int = {
    val r = key compare that.key
    if (r != 0) return r
    // Reverse chronological order
    that.time compare time
  }}

object Cell extends Ordering [Cell] {

  def compare (x: Cell, y: Cell): Int =
    x compare y

  val locator = {
    import StorePicklers._
    tuple (tableId, bytes)
  }

  val pickler = {
    import StorePicklers._
    wrap (bytes, txClock, option (bytes))
    .build ((apply _).tupled)
    .inspect (v => (v.key, v.time, v.value))
  }}
