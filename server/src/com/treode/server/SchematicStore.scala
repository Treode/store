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

  def batch_write(tx: TxId, ct: TxClock, node: JsonNode): Async [TxClock] = {
    var ops = Seq [WriteOp] ()
    try {
      val it = node.iterator
      while (it.hasNext) {

        val request = it.next

        var table = request.get("table").toString
        table = table.substring(1, table.length-1)

        var key = request.get("key").toString
        key = key.substring(1,key.length-1)

        var op = request.get("op").toString
        op = op.substring(1, op.length-1)

        if (op.toString == "UPDATE") {
          val obj = request.get("obj")
          ops = ops :+ WriteOp.Update (schema.getTableId (table), Bytes (key), obj.toBytes)
          println ("Update operation")
        } else if (op.toString == "DELETE") {
          ops = ops :+ WriteOp.Delete (schema.getTableId (table), Bytes (key))
          println ("Delete operation")
        } else if (op.toString == "CREATE") {
          val obj = request.get("obj")
          ops = ops :+ WriteOp.Create (schema.getTableId (table), Bytes (key), obj.toBytes)
          println ("Create operation")
        } else if (op.toString == "HOLD") {
          ops = ops :+ WriteOp.Hold (schema.getTableId (table), Bytes (key))
          println ("Hold operation")
        } else {
          println ("Invalid operation")
        }

        println(request)
      }
      //Future.value (respond (req, Status.NotFound))
    } catch {
      //Future.value (respond (req, Status.NotFound))
      case _: Throwable => println ("Exception")
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
