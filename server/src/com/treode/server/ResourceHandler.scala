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

package com.treode.server

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.treode.async.Async, Async.supply
import com.treode.cluster.HostId
import com.treode.store._, TxStatus._
import com.treode.twitter.finagle.http.RichRequest
import com.treode.twitter.util._
import com.twitter.finagle.Service
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.util.Future

import ResourceHandler.route

class ResourceHandler (store: Store, librarian: Librarian)
extends Service [Request, Response] {

  import librarian.schema

  def read (req: Request, rt: TxClock, tab: TableId, key: String): Async [Response] = {
    val ct = req.conditionTxClock (TxClock.MinValue)
    store.read (rt, ReadOp (tab, Bytes (key))) .map { vs =>
      val v = vs.head
      v.value match {
        case Some (value) if ct < v.time =>
          respond.json (req, rt, v.time, value)
        case Some (_) =>
          respond.unmodified (req, rt)
        case None =>
          respond.notFound (req, rt)
      }}}

  def scan (req: Request, tab: TableId): Async [Response] = {
    val window = req.window
    val slice = req.slice
    var iter = store
        .scan (tab, Bound.firstKey, window, slice)
    if (window.isInstanceOf [Window.Latest])
        iter = iter.filter (_.value.isDefined)
    supply (respond.json (req, iter))
  }

  def create (req: Request, tab: TableId, key: String): Async [Response] = {
    val tx = req.transactionId
    val ct = req.conditionTxClock (TxClock.now)
    val value = req.readJson [JsonNode]
    store.write (tx, ct, WriteOp.Create (tab, Bytes (key), value.toBytes))
    .map [Response] { vt =>
      respond.ok (req, vt)
    }
    .recover {
      case exn: CollisionException =>
        respond.conflict (req)
      case exn: StaleException =>
        respond.stale (req, exn.time)
    }}

  def update (req: Request, tab: TableId, key: String): Async [Response] = {
    val tx = req.transactionId
    val ct = req.conditionTxClock (TxClock.now)
    val value = req.readJson [JsonNode]
    store.write (tx, ct, WriteOp.Update (tab, Bytes (key), value.toBytes))
    .map [Response] { vt =>
      respond.ok (req, vt)
    }
    .recover {
      case exn: StaleException =>
        respond.stale (req, exn.time)
    }}

  def delete (req: Request, tab: TableId, key: String): Async [Response] = {
    val tx = req.transactionId
    val ct = req.conditionTxClock (TxClock.now)
    store.write (tx, ct, WriteOp.Delete (tab, Bytes (key)))
    .map [Response] { vt =>
      respond.ok (req, vt)
    }
    .recover {
      case exn: StaleException =>
        respond.stale (req, exn.time)
    }}

  def batch (req: Request): Async [Response] = {
    val tx = req.transactionId
    val ct = req.conditionTxClock (TxClock.now)
    val json = textJson.readTree (req.getContentString)
    val ops = schema.parseBatchWrite (json)
    store.write (tx, ct, ops:_*)
    .map [Response] { vt =>
      respond.ok (req, vt)
    }
    .recover {
      case exn: CollisionException =>
        respond.conflict (req)
      case exn: StaleException =>
        respond.stale (req, exn.time)
    }}

  def status (req: Request): Async [Response] = {
    val tx = req.transactionId
    store.status (tx)
    .map [Response] {
      case Aborted =>
        respond.aborted (req)
      case Committed (vt) =>
        respond.ok (req, vt)
    }}

  def apply (req: Request): Future [Response] = {
    val rt = req.readTxClock
    route (req.path) match {

      case ResourceHandler.Row (name, key) =>
        schema.getTableId (name) match {
          case Some (id) =>
            req.method match {
              case Method.Post =>
                create (req, id, key) .toTwitterFuture
              case Method.Get =>
                read (req, rt, id, key) .toTwitterFuture
              case Method.Put =>
                update (req, id, key) .toTwitterFuture
              case Method.Delete =>
                delete (req, id, key) .toTwitterFuture
              case _ =>
                Future.value (respond.notAllowed (req, rt))
            }
          case None =>
            Future.value (respond.notFound (req, rt))
        }

      case ResourceHandler.Table (name) =>
        schema.getTableId (name) match {
          case Some (id) =>
            req.method match {
              case Method.Get =>
                scan (req, id) .toTwitterFuture
              case _ =>
                Future.value (respond.notAllowed (req, rt))
            }
          case None =>
            Future.value (respond.notFound (req, rt))
        }

      case ResourceHandler.Status =>
        req.method match {
          case Method.Get =>
            status (req) .toTwitterFuture
          case _ =>
            Future.value (respond.notAllowed (req, rt))
        }

      case ResourceHandler.Batch =>
        req.method match {
          case Method.Post =>
            batch (req) .toTwitterFuture
          case _ =>
            Future.value (respond.notAllowed (req, rt))
        }

      case ResourceHandler.Unmatched =>
        Future.value (respond.notFound (req, rt))
    }}}

object ResourceHandler {

  private val _route = """/([\p{Graph}&&[^/]]+)(/([\p{Graph}&&[^/]]+)?)?""".r

  sealed abstract class Route
  case class Table (table: String) extends Route
  case class Row (table: String, key: String) extends Route
  case object Batch extends Route
  case object Status extends Route
  case object Unmatched extends Route

  def route (path: String): Route =
    path match {
      case _route ("batch-write", _, null) =>
        Batch
      case _route ("tx-status", _, null) =>
        Status
      case _route (tab, _, null) =>
        Table (tab)
      case _route (tab, _, key) =>
        Row (tab, key)
      case _ =>
        Unmatched
    }}
