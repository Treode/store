 package com.treode.server

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, HashSet}

import com.fasterxml.jackson.databind.JsonNode
import com.treode.store.{Bytes, TableId, WriteOp}
import com.treode.twitter.finagle.http.BadRequestException

case class Schema (map: HashMap [String, Long]) {

  def getTableId (s: String): Option [TableId] =
    map.get (s) .map (TableId (_))

  private def requireTableId (name: String): TableId =
    getTableId (name) match {
      case Some (id) => id
      case None => throw new BadRequestException (s"Bad table ID: $name")
    }

  def parseBatchWrite (json: JsonNode): Seq [WriteOp] = {
    var writes = false
    var ops = Seq.empty [WriteOp]
    val pairs = new HashSet [(String, String)]
    for (row <- json.iterator) {
      val table = row.getAttribute ("table") .textValue
      val key = row.getAttribute ("key") .textValue
      val op = row.getAttribute ("op") .textValue
      if (pairs.contains ((table, key))) {
        throw new BadRequestException (s"""Multiple rows found for "$table:$key".""")
      } else {
        pairs += ((table, key))
        op.toLowerCase match {
          case "create" =>
            val obj = row.getAttribute ("obj")
            ops = ops :+ WriteOp.Create (requireTableId (table), Bytes (key), obj.toBytes)
            writes = true
          case "hold" =>
            ops = ops :+ WriteOp.Hold (requireTableId (table), Bytes (key))
          case "update" =>
            val obj = row.getAttribute ("obj")
            ops = ops :+ WriteOp.Update (requireTableId (table), Bytes (key), obj.toBytes)
            writes = true
          case "delete" =>
            ops = ops :+ WriteOp.Delete (requireTableId (table), Bytes (key))
            writes = true
          case _ =>
            throw new BadRequestException (s"""Unsupported operation: "$op".""")
        }}}
      if (!writes)
        throw new BadRequestException ("Batch must have some writes.")
      ops
  }
}
