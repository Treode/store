package com.treode.store.simple

import com.treode.pickle.size
import com.treode.store.{Bytes, StorePicklers, TxClock}

private [store] class Cell (val key: Bytes, val value: Option [Bytes]) extends Ordered [Cell] {

  def byteSize = size (Cell.pickle, this)

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

  val pickle = {
    import StorePicklers._
    wrap2 (bytes, option (bytes)) (Cell.apply _) (v => (v.key, v.value))
  }}
