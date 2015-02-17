/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package example

import java.io.OutputStream

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonNode
import com.treode.async.{Async, BatchIterator}, Async.supply
import com.treode.cluster.HostId
import com.treode.store._
import com.treode.twitter.finagle.http.BadRequestException
import io.netty.channel.ChannelHandler.Sharable
import io.netty.handler.codec.http.HttpResponse
import unfiltered.netty.ReceivedMessage
import unfiltered.request._
import unfiltered.response._

@Sharable
class Resource (host: HostId, store: Store) extends Plan {

  object ParamKey extends Params.Extract ("key", Params.first ~> Params.nonempty)

  case class ResponseIterator [A] (iter: BatchIterator [A]) extends ResponseStreamer {

    private def iterate (jgen: JsonGenerator): Async [Unit] =
      for (v <- iter)
        textJson.writeValue (jgen, v)

    def stream (os: OutputStream) {
      val jgen = textJson.getFactory.createGenerator (os)
      jgen.writeStartArray()
      iterate (jgen) .await()
      jgen.writeEndArray()
      jgen.flush()
    }}

  def read (request: Request, table: TableId, key: String): Async [Response] = {
    val rt = request.requestTxClock
    val ct = request.conditionTxClock (TxClock.MinValue)
    val ops = Seq (ReadOp (table, Bytes (key)))
    store.read (rt, ops:_*) .map { vs =>
      val v = vs.head
      v.value match {
        case Some (value) if ct < v.time =>
          JsonContent ~>
          Date (httpDate.print(rt.toDateTime)) ~>
          LastModified (httpDate.print (v.time.toDateTime)) ~>
          ReadTxClock (rt.toString) ~>
          ValueTxClock (v.time.toString) ~>
          Vary ("Request-TxClock") ~>
          ResponseString (value.toJsonText)
        case Some (_) =>
          NotModified
        case None =>
          NotFound
      }}}

  def scan (request: Request, table: TableId): Async [Response] = {
    val rt = request.requestTxClock
    val ct = request.conditionTxClock (TxClock.MinValue)
    val window = Window.Latest (rt, true, ct, false)
    val slice = request.getSlice
    val iter = store
        .scan (table, Bound.firstKey, window, slice)
        .filter (_.value.isDefined)
    supply (JsonContent ~> ResponseIterator (iter))
  }

  def history (request: Request, table: TableId): Async [Response] = {
    val start = Bound.Inclusive (Key.MinValue)
    val rt = request.requestTxClock
    val ct = request.conditionTxClock (TxClock.MinValue)
    val window = Window.Between (rt, true, ct, false)
    val slice = request.getSlice
    val iter = store.scan (table, Bound.firstKey, window, slice)
    supply (JsonContent ~> ResponseIterator (iter))
  }

  def put (request: Request, table: TableId, key: String): Async [Response] = {
    val tx = request.getTransactionId (host)
    val ct = request.conditionTxClock (TxClock.now)
    val value = request.readJson [JsonNode]
    val ops = Seq (WriteOp.Update (table, Bytes (key), value.toBytes))
    store.write (tx, ct, ops:_*)
    .map [Response] { vt =>
      ValueTxClock (vt.toString)
    }
    .recover {
      case _: StaleException =>
        PreconditionFailed
    }}

  def delete (request: Request, table: TableId, key: String): Async [Response] = {
    val tx = request.getTransactionId (host)
    val ct = request.conditionTxClock (TxClock.now)
    val ops = Seq (WriteOp.Delete (table, Bytes (key)))
    store.write (tx, ct, ops:_*)
    .map [Response] { vt =>
      ValueTxClock (vt.toString)
    }
    .recover {
      case _: StaleException =>
        PreconditionFailed
    }}

  def intent = {
    case request @ Path (Seg ("table" :: _table :: Nil)) =>
      val table = _table.getTableId
      request match {
        case GET (Params (ParamKey (key))) =>
          read (request, table, key)
        case GET (_) =>
          scan (request, table)
        case PUT (Params (ParamKey (key))) =>
          put (request, table, key)
        case PUT (_) =>
          throw new BadRequestException ("""Query parameter "key" required for PUT.""")
        case DELETE (Params (ParamKey (key))) =>
          delete (request, table, key)
        case DELETE (_) =>
          throw new BadRequestException ("""Query parameter "key" required for DELETE.""")
        case _ =>
          supply (MethodNotAllowed)
      }
    case request @ Path (Seg ("history" :: _table :: Nil)) =>
      val table = _table.getTableId
      request match {
        case GET (_) =>
          history (request, table)
        case _ =>
          supply (MethodNotAllowed)
      }}}
