 package com.treode.server

import com.treode.store._
import com.treode.twitter.finagle.http.BadRequestException
import scala.collection.mutable.HashMap

case class Schema (map: HashMap [String, Long]) {

  def getTableId (s: String): TableId = {
    val id = map.get (s)
    id match {
      case Some (id) => id.toString.getTableId
      case None => throw new BadRequestException (s"Bad table ID: $s")
    }}}
