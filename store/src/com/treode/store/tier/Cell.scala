package com.treode.store.tier

import java.util.{Map => JMap}
import com.treode.store.{Bytes, StorePicklers}

private class Cell (val key: Bytes, val value: Option [Bytes])
extends Ordered [Cell] {

  def byteSize = Cell.pickler.byteSize (this)

  def compare (that: Cell): Int = key compare that.key

  override def hashCode: Int = key.hashCode

  override def equals (other: Any) =
    other match {
      case that: Cell => this.key == that.key
      case _          => false
    }

  override def toString = "Cell" + (key, value)
}

private object Cell extends Ordering [Cell] {

  def apply (key: Bytes, value: Option [Bytes]): Cell =
    new Cell (key, value)

  def apply (entry: JMap.Entry [Bytes, Option [Bytes]]): Cell =
    new Cell (entry.getKey, entry.getValue)

  def compare (x: Cell, y: Cell): Int =
    x compare y

  val pickler = {
    import StorePicklers._
    wrap (bytes, option (bytes))
    .build (v => new Cell (v._1, v._2))
    .inspect (v => (v.key, v.value))
  }}
