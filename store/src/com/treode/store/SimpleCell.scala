package com.treode.store

import com.treode.pickle.size

private class SimpleCell (val key: Bytes, val value: Option [Bytes])
extends Ordered [SimpleCell] {

  def byteSize = size (SimpleCell.pickler, this)

  def compare (that: SimpleCell): Int = key compare that.key

  override def hashCode: Int = key.hashCode

  override def equals (other: Any) =
    other match {
      case that: SimpleCell => this.key == that.key
      case _                => false
    }

  override def toString = "Cell" + (key, value)
}

private object SimpleCell extends Ordering [SimpleCell] {

  def apply (key: Bytes, value: Option [Bytes]): SimpleCell =
    new SimpleCell (key, value)

  def compare (x: SimpleCell, y: SimpleCell): Int =
    x compare y

  val pickler = {
    import StorePicklers._
    wrap (bytes, option (bytes)) build ((apply _).tupled) inspect (v => (v.key, v.value))
  }}
