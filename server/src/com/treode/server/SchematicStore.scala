package com.treode.server

import com.twitter.util.Future
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.treode.store._
import com.treode.async.Async
import com.treode.async.BatchIterator
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

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

  def batch_write(tx: TxId, ct: TxClock, operations: Seq [Ops]): Async [TxClock] = {
    var ops = Seq [WriteOp] ()
    val it = operations. iterator
    for (request <- it) {
      var table = request.table
      var key = request.key
      var op = request.op
      op match {
        case "UPDATE" => {
          val obj = request.obj
          ops = ops :+ WriteOp.Update (schema.getTableId (table), Bytes (key), obj.toBytes)
        }
        case "DELETE" => {
          ops = ops :+ WriteOp.Delete (schema.getTableId (table), Bytes (key))
        }
        case "CREATE" => {
          val obj = request.obj
          ops = ops :+ WriteOp.Create (schema.getTableId (table), Bytes (key), obj.toBytes)
        }
        case "HOLD" => {
          ops = ops :+ WriteOp.Hold (schema.getTableId (table), Bytes (key))
        }
        case _ => {

        }
      }
    }
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
