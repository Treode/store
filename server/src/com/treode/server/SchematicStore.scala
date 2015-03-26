package com.treode.server

import com.twitter.util.Future
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.treode.store._
import com.treode.async.Async
import com.treode.async.BatchIterator
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import scala.collection.JavaConversions._
import scala.collection.mutable.HashSet

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

  def batch_write(tx: TxId, ct: TxClock, req: Request): Async [TxClock] = {
    val mapper = new ObjectMapper
    val node = mapper.readTree(req.getContentString)
    val it = node.iterator
    var ops = Seq [WriteOp] ()
    val pairs = new HashSet [(String, String)]
    
    for (request <- it) {
      val table = request.get("table") .textValue
      val key = request.get("key") .textValue
      val op = request.get("op") .textValue
      if (pairs.contains((table, key))) {
        throw new DuplicatePairsException
      } else {
        pairs += ((table, key))
        op match {
          case "UPDATE" => {
            val obj = request .get ("obj")
            ops = ops :+ WriteOp.Update (schema.getTableId (table), Bytes (key), obj.toBytes)
          }
          case "DELETE" => {
            ops = ops :+ WriteOp.Delete (schema.getTableId (table), Bytes (key))
          }
          case "CREATE" => {
            val obj = request .get ("obj")
            ops = ops :+ WriteOp.Create (schema.getTableId (table), Bytes (key), obj.toBytes)
          }
          case "HOLD" => {
            ops = ops :+ WriteOp.Hold (schema.getTableId (table), Bytes (key))
          }
          case _ => {
            throw new OperationNotFoundException
        }}}}
    val done = store.write (tx, ct, ops:_*)
    done
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
