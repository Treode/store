package com.treode.server

import scala.collection.JavaConversions._
import scala.collection.mutable.HashSet

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.treode.twitter.finagle.http.BadRequestException
import com.treode.store._
import com.treode.async.Async
import com.treode.async.BatchIterator
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.util.Future

class SchematicStore (store: Store, schema: Schema) {

  def read (name: String, key: String, rt: TxClock): Async [Seq [Value]] = {
    val ops = Seq (ReadOp (schema.getTableId  (name), Bytes (key)))
      store.read (rt, ops:_*)
  }

  def update (name: String, key: String, value: JsonNode, tx: TxId, ct: TxClock): Async [TxClock] = {
    val ops = Seq (WriteOp.Update (schema.getTableId (name), Bytes (key), value.toBytes))
    store.write (tx, ct, ops:_*)
  }

  def delete (name: String, key: String, tx: TxId, ct: TxClock): Async [TxClock] = {
    val ops = Seq (WriteOp.Delete (schema.getTableId (name), Bytes (key)))
    store.write (tx, ct, ops:_*)
  }

  def batch (tx: TxId, ct: TxClock, node: JsonNode): Async [TxClock] = {
    var writes = false
    var ops = Seq.empty [WriteOp]
    val pairs = new HashSet [(String, String)]
    for (row <- node.iterator) {
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
            ops = ops :+ WriteOp.Create (schema.getTableId (table), Bytes (key), obj.toBytes)
            writes = true
          case "hold" =>
            ops = ops :+ WriteOp.Hold (schema.getTableId (table), Bytes (key))
          case "update" =>
            val obj = row.getAttribute ("obj")
            ops = ops :+ WriteOp.Update (schema.getTableId (table), Bytes (key), obj.toBytes)
            writes = true
          case "delete" =>
            ops = ops :+ WriteOp.Delete (schema.getTableId (table), Bytes (key))
            writes = true
          case _ =>
            throw new BadRequestException (s"""Unsupported operation: "$op".""")
        }}}
      if (!writes)
        throw new BadRequestException ("Batch must have some writes.")
    store.write (tx, ct, ops:_*)
  }

  def scan (
      name: String,
      key: Bound [Key] = Bound.firstKey,
      window: Window = Window.all,
      slice: Slice = Slice.all,
      batch: Batch = Batch.suggested
  ): BatchIterator [Cell] = {
    store.scan (schema.getTableId (name), key, window, slice, batch)
  }}
