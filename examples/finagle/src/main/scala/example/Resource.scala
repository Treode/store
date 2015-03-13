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

import com.fasterxml.jackson.databind.JsonNode
import com.treode.async.Async, Async.supply
import com.treode.cluster.HostId
import com.treode.store._
import com.treode.twitter.finagle.http.RichRequest
import com.treode.twitter.util._
import com.twitter.finagle.Service
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.finagle.http.path._
import com.twitter.util.Future

class Resource (host: HostId, store: Store) extends Service [Request, Response] {

  object KeyParam extends ParamMatcher ("key")

  def read (req: Request, tab: TableId, key: String): Async [Response] = {
    val rt = req.requestTxClock
    val ct = req.conditionTxClock (TxClock.MinValue)
    val ops = Seq (ReadOp (tab, Bytes (key)))
    store.read (rt, ops:_*) .map { vs =>
      val v = vs.head
      v.value match {
        case Some (value) if ct < v.time =>
          respond.json (req, v.time, value)
        case Some (_) =>
          respond (req, Status.NotModified)
        case None =>
          respond (req, Status.NotFound)
      }}}

  def scan (req: Request, table: TableId): Async [Response] = {
    val rt = req.requestTxClock
    val ct = req.conditionTxClock (TxClock.MinValue)
    val window = Window.Latest (rt, true, ct, false)
    val slice = req.slice
    val iter = store
        .scan (table, Bound.firstKey, window, slice)
        .filter (_.value.isDefined)
    supply (respond.json (req, iter))
  }

  def history (req: Request, table: TableId): Async [Response] = {
    val start = Bound.Inclusive (Key.MinValue)
    val rt = req.requestTxClock
    val ct = req.conditionTxClock (TxClock.MinValue)
    val window = Window.Between (rt, true, ct, false)
    val slice = req.slice
    val iter = store.scan (table, Bound.firstKey, window, slice)
    supply (respond.json (req, iter))
  }

  def put (req: Request, table: TableId, key: String): Async [Response] = {
    val tx = req.transactionId (host)
    val ct = req.conditionTxClock (TxClock.now)
    val value = req.readJson [JsonNode]
    val ops = Seq (WriteOp.Update (table, Bytes (key), value.toBytes))
    store.write (tx, ct, ops:_*)
    .map [Response] { vt =>
      val rsp = req.response
      rsp.status = Status.Ok
      rsp.headerMap.add ("Value-TxClock", vt.toString)
      rsp
    }
    .recover {
      case _: StaleException =>
        respond (req, Status.PreconditionFailed)
    }}

  def delete (req: Request, table: TableId, key: String): Async [Response] = {
    val tx = req.transactionId (host)
    val ct = req.conditionTxClock (TxClock.now)
    val ops = Seq (WriteOp.Delete (table, Bytes (key)))
    store.write (tx, ct, ops:_*)
    .map [Response] { vt =>
      val rsp = req.response
      rsp.status = Status.Ok
      rsp.headerMap.add ("Value-TxClock", vt.toString)
      rsp
    }
    .recover {
      case _: StaleException =>
        respond (req, Status.PreconditionFailed)
    }}

  def apply (req: Request): Future [Response] = {
    Path (req.path) :? req.params match {

      case Root / "table" / tab :? KeyParam (key) =>
        req.method match {
          case Method.Get =>
            read (req, tab.getTableId, key) .toTwitterFuture
          case Method.Put =>
            put (req, tab.getTableId, key) .toTwitterFuture
          case Method.Delete =>
            delete (req, tab.getTableId, key) .toTwitterFuture
          case _ =>
            Future.value (respond (req, Status.MethodNotAllowed))
        }

      case Root / "table" / tab :? _ =>
        req.method match {
          case Method.Get =>
            scan (req, tab.getTableId) .toTwitterFuture
          case _ =>
            Future.value (respond (req, Status.MethodNotAllowed))
        }

      case Root / "history" / tab :? _ =>
        req.method match {
          case Method.Get =>
            history (req, tab.getTableId) .toTwitterFuture
          case _ =>
            Future.value (respond (req, Status.MethodNotAllowed))
        }

      case _ =>
        Future.value (respond (req, Status.NotFound))
    }}
}
