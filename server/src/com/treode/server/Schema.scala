 package com.treode.server

import com.treode.store._
import com.treode.twitter.finagle.http.BadRequestException
import scala.collection.mutable.HashMap

class Schema (mapping: HashMap [String, Long]) {

  def getTableId (s: String): TableId = {
    val id = mapping.get (s)
    id match {
      case Some (id)
        => id.toString.getTableId
        case None =>
        throw new BadRequestException (s"Bad table ID: $s")
    }}}
