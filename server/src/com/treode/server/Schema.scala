 package com.treode.server

import scala.collection.mutable.HashMap

import com.treode.store._

case class Schema (map: HashMap [String, Long]) {

  def getTableId (s: String): Option [TableId] =
    map.get (s) .map (TableId (_))
}
