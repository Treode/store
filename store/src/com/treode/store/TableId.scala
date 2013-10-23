package com.treode.store

import scala.language.implicitConversions

import com.treode.pickle.Picklers

class TableId private (val id: Long) extends AnyVal {

  override def toString = f"TableId:$id%08X"
}

object TableId {

  implicit def apply (id: Long): TableId =
    new TableId (id)

  val pickle = {
    import Picklers._
    wrap1 (fixedLong) (apply _) (_.id)
  }}
