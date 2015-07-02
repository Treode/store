 package com.treode.server

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashSet, StringBuilder}

import com.fasterxml.jackson.databind.JsonNode
import com.treode.pickle.Picklers
import com.treode.store.{Bytes, CatalogDescriptor, TableId, WriteOp}
import com.treode.twitter.finagle.http.BadRequestException

class Schema private (
  val tables: Map [String, TableId],
  val version: Int,
  val source: String
) {

  def getTableId (s: String): Option [TableId] =
    tables.get (s)

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

  def copy (version: Int = version): Schema =
    new Schema (tables, version, source)

  override def hashCode: Int =
    tables.hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: Schema => tables == that.tables
      case _ => false
    }

  override def toString: String =
    source
}

object Schema {

  def apply (tables: Map [String, TableId]): Schema =
    new Schema (tables, 0, "<no source>")

  def apply (tables: Map [String, TableId], source: String): Schema =
    new Schema (tables, 0, source)

  val empty: Schema =
    new Schema (Map.empty, 0, "")

  val pickler = {
    import Picklers._
    wrap (map (string, TableId.pickler), uint, string)
    .build (v => new Schema (v._1, v._2, v._3))
    .inspect (v => (v.tables, v.version, v.source))
  }

  val catalog = CatalogDescriptor (0x48FA40B90BA03D25L, pickler)
}
