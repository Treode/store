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
import com.treode.async.Async
import com.treode.cluster.HostId
import com.treode.store._
import com.twitter.finatra.{Request, ResponseBuilder}
import org.joda.time.Instant

import Async.{guard, supply}
import WriteOp._

class Resource (host: HostId, store: Store) extends AsyncFinatraController {

  def read (request: Request, table: TableId, key: String): Async [ResponseBuilder] = {
    val rt = request.getLastModificationBefore
    val ct = request.getIfModifiedSince
    val table = request.getTableId
    val ops = Seq (ReadOp (table, Bytes (key)))
    for {
      vs <- store.read (rt, ops:_*)
    } yield {
      val v = vs.head
      v.value match {
        case Some (value) if ct < v.time =>
          render.header (ETag, v.time.toString) .appjson (value)
        case Some (value) =>
          render.status (NotModified) .nothing
        case None =>
          render.notFound.nothing
      }}}

  def scan (request: Request, table: TableId): Async [ResponseBuilder] = {
    val rt = request.getLastModificationBefore
    val ct = request.getIfModifiedSince
    val window = Window.Recent (rt, true, ct, false)
    val slice = request.getSlice
    val iter = store
        .scan (table, Bound.firstKey, window, slice)
        .filter (_.value.isDefined)
    for {
      vs <- iter.toSeq
    } yield {
      render.appjson (vs)
    }}

  get ("/table/:name") { request =>
    val table = request.getTableId
    val key = request.params.get ("key")
    if (key.isDefined)
      read (request, table, key.get)
    else
      scan (request, table)
  }

  get ("/history/:name") { request =>
    val table = request.getTableId
    val start = Bound.Inclusive (Key.MinValue)
    val rt = request.getLastModificationBefore
    val ct = request.getIfModifiedSince
    val window = Window.Between (rt, true, ct, false)
    val slice = request.getSlice
    val iter = store.scan (table, Bound.firstKey, window, slice)
    for {
      vs <- iter.toSeq
    } yield {
      render.appjson (vs)
    }}

  put ("/table/:name") { request =>
    val tx = request.getTransactionId (host)
    val ct = request.getIfUnmodifiedSince
    val table = request.getTableId
    val key = request.params.getOrThrow ("key", new BadRequestException ("Expected key"))
    val value = request.readJson()
    val ops = Seq (Update (table, Bytes (key), value.toBytes))
    (for {
      vt <- store.write (tx, ct, ops:_*)
    } yield {
      render.ok.header (ETag, vt.toString) .nothing
    }) .recover {
      case _: StaleException =>
        render.status (PreconditionFailed) .nothing
    }}

  delete ("/table/:name") { request =>
    val tx = request.getTransactionId (host)
    val ct = request.getIfUnmodifiedSince
    val table = request.getTableId
    val key = request.params.getOrThrow ("key", new BadRequestException ("Expected key"))
    val ops = Seq (Delete (table, Bytes (key)))
    (for {
      vt <- store.write (tx, ct, ops:_*)
    } yield {
      render.ok.header (ETag, vt.toString) .nothing
    }) .recover {
      case _: StaleException =>
        render.status (PreconditionFailed) .nothing
    }}}
