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
import com.treode.async.misc.parseUnsignedLong
import com.treode.cluster.HostId
import com.treode.store._
import com.treode.twitter.finagle.http.RichRequest
import com.treode.twitter.util._
import com.twitter.finagle.Service
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.finagle.http.path._
import com.twitter.util.Future

import Resource.{Row, Table, Unmatched, route}

class Resource (host: HostId, store: Store) extends Service [Request, Response] {

  def read (req: Request, tab: TableId, key: String): Async [Response] = {
    val rt = req.readTxClock
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
    val rt = req.readTxClock
    val ct = req.conditionTxClock (TxClock.MinValue)
    val window = req.window
    val slice = req.slice
    val iter = store
        .scan (table, Bound.firstKey, window, slice)
        .filter (_.value.isDefined)
    supply (respond.json (req, iter))
  }

  def put (req: Request, table: TableId, key: String): Async [Response] = {
    val tx = req.transactionId (host)
    val ct = req.conditionTxClock (TxClock.now)
    val value = req.readJson [JsonNode]
    val ops = Seq (WriteOp.Update (table, Bytes (key), value.toBytes))
    store.write (tx, ct, ops:_*)
    .map [Response] { vt =>
      respond.ok (req, vt)
    }
    .recover {
      case exn: StaleException =>
        respond.stale (req, exn.time)
    }}

  def delete (req: Request, table: TableId, key: String): Async [Response] = {
    val tx = req.transactionId (host)
    val ct = req.conditionTxClock (TxClock.now)
    val ops = Seq (WriteOp.Delete (table, Bytes (key)))
    store.write (tx, ct, ops:_*)
    .map [Response] { vt =>
      respond.ok (req, vt)
    }
    .recover {
      case exn: StaleException =>
        respond.stale (req, exn.time)
    }}

  def apply (req: Request): Future [Response] =
    route (req.path) match {

      case Row (tab, key) =>
        req.method match {
          case Method.Get =>
            read (req, tab, key) .toTwitterFuture
          case Method.Put =>
            put (req, tab, key) .toTwitterFuture
          case Method.Delete =>
            delete (req, tab, key) .toTwitterFuture
          case _ =>
            Future.value (respond (req, Status.MethodNotAllowed))
        }

      case Table (tab) =>
        req.method match {
          case Method.Get =>
            scan (req, tab) .toTwitterFuture
          case _ =>
            Future.value (respond (req, Status.MethodNotAllowed))
        }

      case Unmatched =>
        Future.value (respond (req, Status.NotFound))
    }}

object Resource {

  private val _route = """/(([1-9][0-9]*)|(0[0-7]*)|((#|0x)([0-9a-fA-F]+)))(/(.+)?)?""".r

  sealed abstract class Route
  case class Table (id: TableId) extends Route
  case class Row (id: TableId, key: String) extends Route
  case object Unmatched extends Route

  def route (path: String): Route =
    path match {
      case _route (_, dec, null, _, _, null, _, null) =>
        parseUnsignedLong (dec, 10) .map (Table (_)) .getOrElse (Unmatched)
      case _route (_, null, oct, _, _, null, _, null) =>
        parseUnsignedLong (oct, 8) .map (Table (_)) .getOrElse (Unmatched)
      case _route (_, null, null, _, _, hex, _, null) =>
        parseUnsignedLong (hex, 16) .map (Table (_)) .getOrElse (Unmatched)
      case _route (_, dec, null, _, _, null, _, key) =>
        parseUnsignedLong (dec, 10) .map (Row (_, key)) .getOrElse (Unmatched)
      case _route (_, null, oct, _, _, null, _, key) =>
        parseUnsignedLong (oct, 8) .map (Row (_, key)) .getOrElse (Unmatched)
      case _route (_, null, null, _, _, hex, _, key) =>
        parseUnsignedLong (hex, 16) .map (Row (_, key)) .getOrElse (Unmatched)
      case _ =>
        Unmatched
    }}
