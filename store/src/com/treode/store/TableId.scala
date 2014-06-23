package com.treode.store

import scala.language.implicitConversions

import com.google.common.primitives.UnsignedLongs
import com.treode.pickle.Picklers

/** The identifier for a table.
  *
  * We recommend using a random long, which is highly likely to be unique.  This removes the need
  * to keep a registry and is very easy to search.  To generate a random long, try
  *
  * '''
  * head -c 8 /dev/urandom | hexdump -e "\"0x\" 8/1 \"%02X\" \"L\n\""
  * '''
  */
class TableId private (val id: Long) extends AnyVal with Ordered [TableId] {

  def compare (that: TableId): Int =
    UnsignedLongs.compare (this.id, that.id)

  override def toString =
    if (id < 256) f"Table:$id%02X" else f"Table:$id%016X"
}

object TableId extends Ordering [TableId] {

  val MinValue = TableId (0)

  val MaxValue = TableId (-1)

  implicit def apply (id: Long): TableId =
    new TableId (id)

  def compare (x: TableId, y: TableId): Int =
    x compare y

  val pickler = {
    import Picklers._
    wrap (fixedLong) build (apply _) inspect (_.id)
  }}
