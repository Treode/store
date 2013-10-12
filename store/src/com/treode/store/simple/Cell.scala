package com.treode.store.simple

import com.treode.store.{Bytes, TxClock}

private [store] class Cell (val key: Bytes, val value: Option [Bytes]) extends Ordered [Cell] {

  def byteSize: Int = {
    val n = key.byteSize
    value match {
      case Some (v) => n + v.byteSize
      case None     => n
    }}

  def compare (that: Cell): Int = key compare that.key

  override def hashCode: Int = key.hashCode

  override def equals (other: Any) =
    other match {
      case that: Cell => this.key == that.key
      case _          => false
    }

  override def toString = "Cell" + (key, value)
}

private [store] object Cell extends Ordering [Cell] {

  def apply (key: Bytes, value: Option [Bytes]): Cell =
    new Cell (key, value)

  def compare (x: Cell, y: Cell): Int =
    x compare y
}
